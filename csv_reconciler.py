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


def get_missing_records(df, series, key) -> pd.DataFrame:
    merged = pd.merge(df, series, on=key, how='left', indicator=True)
    data = merged[merged['_merge'] == 'left_only']
    data = data.drop(columns=['_merge'])
    return data

def store_missing_records(df, store, message) -> None:
    for _, row in df.iterrows():
        store.append((message, *row))

def store_field_discrepancies(source, target, store, key, message) -> int:
    count = 0
    for i, row in source.iterrows():
        for column in source.columns:
            if row[column] != target.loc[i, column] and row[key] == target_df.loc[i, key]:
                count += 1
                store.append((message, row[column], target_df.loc[i, column]))
    return count



if __name__ == "__main__":
    args = ScriptArguments(underscores_to_dashes=True).parse_args()

    source_df = pd.read_csv(args.s, dtype=dtype)
    target_df = pd.read_csv(args.t, dtype=dtype)

    in_source_only = get_missing_records(target_df, source_df['ID'], 'ID')
    in_target_only = get_missing_records(source_df, target_df['ID'], 'ID')

    store = []
    store_missing_records(in_source_only, store, 'Missing in Target')
    store_missing_records(in_target_only, store, 'Missing in Source')
    total_discrepancies = store_field_discrepancies(source_df, target_df, store, 'ID', 'Field Discrepancy')



    writer = csv.writer(open(args.o, mode='w'))
    writer.writerows(store)

    print("\n\nReconciliation Completed:")
    print("- Records missing in target: ", in_source_only.shape[0])
    print("- Records missing in source: ", in_target_only.shape[0])
    print("- Records with field discrepancies: ", total_discrepancies)

    print("\nReport saved to: ", args.o, "\n\n")


