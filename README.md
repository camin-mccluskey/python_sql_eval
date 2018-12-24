# Python SQL Evaluator

> Command line program to evaluate and run SQL queries using Python.

To run program from command line: ```$ python3 sqlEval.py <table-folder> <sql-json-file> <output-file> ``` </br>
To run program against all test cases: ```$ ./check python3 sqlEval.py -- examples examples/*.sql``` </br>
To see an overview of the module: ```$ pydoc sqlEval```

### Explanation of code
* Command line args are parsed by sqlEval. Query object is initialised with the location of the tables folder and the
.sql.json file of the query to be executed.
* The Query object constructs Table objects for the relevant table (Docs: ```$ pydoc Query```). Using the contents of each table and the query JSON
an error check is conducted. A boolean success variable is returned, with an error message if the query is not valid.
* If the query is valid, it is optimised to avoid redundant cross product work by eliminating the following:
    1. Columns which are not needed for any SELECT or WHERE clauses.
    2. WHERE clauses which are evaluating a column to literal.
    3. WHERE clauses which result in row never being needed in output. E.g. clause: a.data > b.data. a.data has rows
    <= all rows in b.data, in this case a.data at that row can be removed from table a.
* Generate the cross product of all remaining rows in tables.
* Evaluate remaining WHERE clauses on cross product table, generate reduced table.
* Evaluate SELECT clauses on reduced table.
* Return query success and result (either error or JSON result of query execution).
* Write result of query execution to output file.

## Todo
- [ ] Refactor Table object - making WHERE and SELECT methods.
- [ ] Refactor to make cross product a Table object to take advantage of WHERE and SELECT methods.
- [ ] Benchmark performance of query optimisation on larger tables.
- [ ] Build a more user friendly CLI using [Click](https://click.palletsprojects.com/en/7.x/) or equivalent.
- [ ] Ensure JSON queries are properly formatted.
