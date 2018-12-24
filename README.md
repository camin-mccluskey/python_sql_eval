# python_sql_eval

> Program to evaluate and run SQL queries.

To run program from command line: ```$ ./check python3 sqlEval.py <table-folder> <sql-json-file> <output-file> ```\- # TODO change
To run program against all test cases ```$ ./check python3 sqlEval.py -- examples examples/*.sql```\
To see an overview of the module: ```$ pydoc sqlEval```\

### Explanation of code

* Command line args are parsed by sqlEval. Query object is initialised with the location of the tables folder and the
.sql.json file of the query to be executed.
* The query object constructs Table objects for the relevant tables. Using the contents of each table and the query JSON
an error check is conducted. A boolean success variable is returned, with an error message if the query is not valid.
* If the query is valid, it is optimised to avoid redundant cross product work by eliminating the following:
    1. Columns which are not needed for any SELECT or WHERE clauses.
    2. WHERE clauses which are evaluating a column to literal.
    3. WHERE clauses which result in row never being needed in output. i.e. clause: a.data > b.data. a.data has row
    < all rows in b.data, in this case a.data at that row can be removed from table a.
* Generate the cross product of all remaining rows in tables
* Evaluate remaining WHERE clauses on cross product table, generate reduced table.
* Evaluate SELECT clauses on reduced table
* Return query success and result (either error or JSON result of query execution
* Write to output file

## Todo
[] Refactor Table object - making WHERE and SELECT methods
[] Refactor to make cross product a Table object to take advantage of WHERE and SELECT methods
[]
