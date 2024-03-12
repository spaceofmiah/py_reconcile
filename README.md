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

```bash
python csv_reconciler.py --s path/to/csv --t path/to/csv --o file.csv
```

`--s` : source : a csv file with data entries

`--t` : target : a csv file with data entries

`--o` : output

**optional**

`--c` : comma separated list of columns to reconcile e.g 'ID,Name,Date' would only reconcile for ID,   Name,   & Date column