# pyspark_proj_6_delete_cols

Project will load data from `Data/2015-summary.csv' to Spark data frame.

This project helps us to delete the columns from the data set and store as a seperate file.

## `requirments.txt`
- pyspark
- findspark

## `init_context.py`
It initializes the Spark session and Spark context.

## `read_data.py`
Reads the data from Data Folder and store it into Pyspark Data Frame.

## `data_man.py`
Following data operations could be performed:
- `no_of_records()`: Number of records in the data.
- `col_names()`: Display the Filed names of the data.
- `count_missing_values()`: Count the missing values column wise if any.
- `delete_cols()` : Delete any column of the data frame and write to seperate file.

## `write_data.py`
It helps to write the data into seperte file by converting into the pandas data frame and then to csv.
- `write_data()` : Helps to convert spark data frame to pandas data frame and then save file to csv.

## `main.py`

All main operations will be here.
