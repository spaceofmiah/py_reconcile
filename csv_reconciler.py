import pandas as pd
import csv
from tap import Tap

dtype = {
    'ID': str,
    'Name': str,
    'Date': str,
    'Amount': float,
}

class ScriptArguments(Tap):
    s: str  
    t: str 
    o: str  


args = ScriptArguments(underscores_to_dashes=True).parse_args()

source_df = pd.read_csv(args.s, dtype=dtype)
target_df = pd.read_csv(args.t, dtype=dtype)

# Merge the DataFrames
merged = pd.merge(source_df, target_df, how='outer', indicator=True)

# Keep only the rows that exist in one DataFrame but not in the other
in_target_only = merged[merged['_merge'] == 'right_only']
in_target_only = in_target_only.drop(columns=['_merge'])

in_source_only = merged[merged['_merge'] == 'left_only']
in_source_only = in_source_only.drop(columns=['_merge'])


reconciliation = []

for _, row in in_source_only.iterrows():
    reconciliation.append(('Missing in Target', *row))


for _, row in in_target_only.iterrows():
    reconciliation.append(('Missing in Source', *row))


for i, row in source_df.iterrows():
    for column in source_df.columns:
        if row[column] != target_df.loc[i, column] and row["ID"] == target_df.loc[i, "ID"]:
            reconciliation.append(("Field Discrepancy", row[column], target_df.loc[i, column]))


writer = csv.writer(open(args.o, mode='w'))
writer.writerows(reconciliation)