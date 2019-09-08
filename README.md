# basic-rdb-sql
A basic Relational Database which takes SQL Queries in Python

## Error Handling
- Should have metadata.txt
- table_name.csv should exist
- attributes given in SELECT should be in the table
- with multiple tables, if attribute is "table1.A" then "table1" should be in FROM AND attribute should be of that table
- with multiple tables, tables should be same length
- with multiple tables, attribute mentioned should be in one of the tables mentioned
- attribute in WHERE must be in mentioned tables
- Columns mentioned in Select where must be in mentioned tables
- Module won't point out ambiguity

## Note
- Order of attributes in SELECT does not matter
- In Aggregate, no space between function and brackets
- Where clause column need not be in select columns

## Order of Execution (Pipeline)
- Parse Query
- Check if multiple tables
- [IF WHERE]
- Execute Select ALL for all tables mentioned in query
- Execute Where on output of SELECT ALL
- Execute Select Query (Multiple if multiple tables and combine together)
- [IF NOT WHERE]
- Execute Select Query (Multiple if multiple tables and combine together)
- If DISTINCT, Remove all but one copy of same tuples
- Execute Aggregate on Output of Where

## TO-DO
- [x] Parse SQL Queries
- [x] Select All records using *
- [x] Select Any Columns from one Table
- [x] Display tablename.columnname
- [x] Detect type of query - Where, if not where, aggregate or normal
- [x] Deal with Aggregate Functions
- [x] Deal with more than one table (multiple calls to select)
- [x] Incorporate multiple tables in final output
- [ ] Add Distinct
- [x] Detect Where Clause (single)
- [x] Run Where Clause on full table and give pruned table
- [x] Run SelectWhere on Pruned table to give only required columns
- [x] Add functionality for OR and AND in Where
- [x] Detect when Join
- [ ] Join Tables (Remove Common Column)
- [x] Multiple table select gives Cross Join
- [x] Aggregate functions in the pipeline at the end (work with where etc)
 