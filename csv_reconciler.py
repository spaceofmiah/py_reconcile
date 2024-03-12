import pandas as pd
from tap import Tap
import csv

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

# Merge the DataFrames on the 'ID' column, keeping only rows from df2 that have no corresponding ID in df1
merged = pd.merge(target_df, source_df['ID'], on='ID', how='left', indicator=True)

# Filter the merged DataFrame to find records missing from source
in_source_only = merged[merged['_merge'] == 'left_only']

# Drop the '_merge' column
in_source_only = in_source_only.drop(columns=['_merge'])



# Merge the DataFrames on the 'ID' column, keeping only rows from df2 that have no corresponding ID in df1
merged = pd.merge(source_df, target_df['ID'], on='ID', how='left', indicator=True)

# Filter the merged DataFrame to find records missing from source
in_target_only = merged[merged['_merge'] == 'left_only']

# Drop the '_merge' column
in_target_only = in_target_only.drop(columns=['_merge'])


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



