# ResolveDateConflict
Resolve date conflicts arising from SQL-type joins on records.

## Description:
One way of tracking changes in a database is to take regular snapshots of the entire database and append the snapshot date. However this approach is space-inefficient. A way around that is to assign validity dates to records for the duration they remain static. It can be done by:

1. Sorting records by primary key and snapshot date
2. Grouping records by primary key and data fields
3. Sampling first and last records from each group
4. Joining two sample groups on primary key and data fields
  - The Snapshot column of the first sample becomes the "From" date
  - The Snapshot column of the last sample becomes the "To" date

## The Problem:
If the records revert back to a previous state in the future after some intermediate changes, the join will malfunction. Instead of similar records being split by validity dates on either side of intermediate records, a single records will be created with validity dates encompassing the first and the last instance of the record.

## The Solution:
A python script that takes the following commandline arguments:

1. Name of primary key field
2. Path to source file (.csv)
3. Path to output file (.csv)
4. "From" column name
5. "To" column name

*Date columns must be in YYYY-MM-DD format*

For example:

>python fix.py "ID" "source.csv" "final.csv" "ValidFrom" "ValidTo"

The script creates a logs folder that contains records of the fixes made and the runtime for the script.

The concept is illustrated in `workflow_testing.xlsx`
