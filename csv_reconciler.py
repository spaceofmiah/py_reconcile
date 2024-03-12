import pandas as pd
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
    c: str = '' # comma separated columns to compare. column name should be same as that found on both source and target file


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
            try:
                if row[column] != target.loc[i, column] and row[key] == target_df.loc[i, key]:
                    count += 1
                    store.append((message, row[column], target_df.loc[i, column]))
            except KeyError:
                continue

    return count



if __name__ == "__main__":
    import csv
    from pathlib import Path

    args = ScriptArguments(underscores_to_dashes=True).parse_args()

    # check if the passed source file exists
    source_exist = Path(args.s).exists()

    # check if the passed target file exists
    target_exist = Path(args.t).exists()

    # handle for when source file doesn't exist
    if not source_exist:
        print(f"Unable to locate source file: {args.s}")
        exit(0)

    # handle for when target file doesn't exist
    if not target_exist:
        print(f"Unable to locate target file: {args.t}")
        exit(0)

    # preprocess columns to reconcile.
    if not args.c or len(args.c) == 0:
        columns = dtype.keys() # use default columns 
    else:
        columns = args.c.split(',') # use specified reconciliation columns


    # read source and target csv files as given on program run
    # while handling for column mismatch should there be any. 
    try:
        source_df = pd.read_csv(args.s, usecols=columns)
        target_df = pd.read_csv(args.t, usecols=columns)
    except ValueError as e:
        if "do not match columns" in str(e):
            missing_columns = str(e).split(':')[-1]
            print(f"\n\nColumns not found in the provided source & target CSV: {missing_columns}\n")
            exit(0)

    # get missing records in source csv that's present in target csv
    in_target_only = get_missing_records(target_df, source_df['ID'], 'ID')

    # get missing records in target csv that's present in source csv
    in_source_only = get_missing_records(source_df, target_df['ID'], 'ID')


    # Store all missing records and field discrepancies
    store = []
    store_missing_records(in_source_only, store, 'Missing in Target')
    store_missing_records(in_target_only, store, 'Missing in Source')
    total_discrepancies = store_field_discrepancies(
        source_df, target_df, store, 'ID', 'Field Discrepancy'
    )

    # save reconciliation to the provided output csv file
    writer = csv.writer(open(args.o, mode='w'))
    writer.writerows(store)


    # output bash command
    print("\n\nReconciliation Completed:")
    print("- Records missing in target: ", in_source_only.shape[0])
    print("- Records missing in source: ", in_target_only.shape[0])
    print("- Records with field discrepancies: ", total_discrepancies)

    print("\nReport saved to: ", args.o, "\n\n")


