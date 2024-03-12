# py_reconcile
A script to reconcile two different CSV data records 

Program takes in two different CSV file, one as source and the other as target and reconciles data in both.

### Getting Started

To get started using the program, you first have to install the dependencies. Use below command to install dependencies

```bash
pip install -r requirements.txt
```

### Running the Program

The program requires two compulsory csv file as inputs serving as source and target respectively

### Supported Features

- Produce a reconciliation report with the following sections:
    - Records present in source but missing in target.
    - Records present in target but missing in source.
    - Records with field discrepancies, highlighting the specific fields that differ.

- Allow the user to configure which columns to compare, in case some columns should be ignored during reconciliation.


```bash
python csv_reconciler.py --s path/to/sourc_csv --t path/to/target_csv --o file.csv
```

`--s` : source : a csv file with data entries

`--t` : target : a csv file with data entries

`--o` : output

**optional**

`--c` : comma separated list of columns to reconcile e.g 'ID,Name,Date' would only reconcile for ID,   Name,   & Date column


To run the program with specific columns to reconcile use the below command. This would only reconcile column ID  Date &  Name

```bash
python csv_reconciler.py --s path/to/sourc_csv --t path/to/target_csv --o file.csv --c "ID,Date,Name"
```

### Followed Guidelines

- Ensured code quality with a well-organized, commented, and
tested solution

- Provided documentation or instructions on how to run your tool.

- Product is built to scale over millions of records.