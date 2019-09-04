# basic-rdb-sql
A basic Relational Database which takes SQL Queries in Python

## Error Handling
- Should have metadata.txt
- table_name.csv should exist
- attributes given in SELECT should be in the table
- with multiple tables, if attribute is "table1.A" then "table1" should be in FROM AND attribute should be of that table
- with multiple tables, tables should be same length
- with multiple tables, attribute mentioned should be in one of the tables mentioned

## Note
- Order of attributes in SELECT does not matter
- In Aggregate, no space between function and brackets

## Order of Execution (Pipeline)
- Parse Query
- Check if multiple tables
- Execute Select Query (Multiple if multiple tables and combine together)
- Execute Where on Output of Select
- If DISTINCT, Remove all but one copy of same tuples
- Execute Aggregate on Output of Where

## TO-DO
- [x] Parse SQL Queries
- [x] Select All records using *
- [x] Select Any Columns from one Table
- [x] Display tablename.columnname
- [x] Detect type of query - Where, if not where, aggregate or normal
- [x] Deal with Aggregate Functions
- [ ] Deal with more than one table (multiple calls to select)
- [ ] Incorporate multiple tables in final output
- [ ] Add Distinct
- [ ] Detect Where Clause (single)
- [ ] Run Where Clause on returned table and give final table
- [ ] Add functionality for OR and AND in Where
- [ ] Detect when Join
- [ ] Join Tables (Remove Common Column)
 