"""

file to construct SQL query object and table object

"""

import json
import pandas as pd
from functools import reduce
import operator


class Query:
    def __init__(self, jsonSQL, tableFolder):
        # JSON object of SQL query
        self.json = jsonSQL
        # location of tables in query
        self.tableFolder = tableFolder
        # instantiate hashmap of {alis: Table objects} in query as self.tables
        self.tables = self.__constructTables()
        # check query for errors
        self.passedErrorCheck, self.errorMessage = self.noErrors()
        # optimise query
        if self.passedErrorCheck:
            self.__optimiseQuery()

    def run(self):
        """
        Run query
        :return: object with query status and result if query is valid
        """
        response = {'success': None, 'JSON': None}
        if self.passedErrorCheck:
            # compute cross product of all tables as self.crossProduct
            self.__constructCrossPoduct()

            # filter by where clause in query
            whereClauses = self.json['where']
            modifiedCrossProduct = self.__where(whereClauses)

            # select and rename selected columns
            selectStatement = self.json['select']
            queryResult = self.__select(modifiedCrossProduct, selectStatement).reset_index(drop=True)

            labels = queryResult.columns
            result = []
            newLabels = []
            data = queryResult.values.tolist()
            for i in range(len(data[0])):
                dType = type(data[0][i]).__name__
                newLabels.append([labels[i], dType])

            # convert to output format
            result.append(newLabels)
            result.extend(data)
            response['success'] = True
            response['JSON'] = result
            return response
        # Error in query
        else:
            response['success'] = False
            response['JSON'] = self.errorMessage
            return response

    def __optimiseQuery(self):
        """
        Function to optimise the query by eliminating redundant cross product work in cases where WHERE clause is
        evaluating column to literal and where SELECT and WHERE clauses are not using a column in table. Redundant WHERE
        clauses are removed from sqljson to avoid them being considered in .run(). Columns not needed to execute the
        query are removed from there respective tables.
        :return: None
        """
        # generate set of columns needed for the WHERE and SELECT queries
        requiredCols = set()
        # add select statements in query
        for statement in self.json['select']:
            requiredCols.add(statement['column']['name'])
        # add where clauses in query
        for clause in self.json['where']:
            left = clause['left']
            right = clause['right']
            if 'column' in left:
                requiredCols.add(clause['left']['column']['name'])
            if 'column' in right:
                requiredCols.add(clause['right']['column']['name'])

        # delete columns that are not necessary for SELECT or WHERE statements
        for table in self.tables.values():
            toDelete = set()
            for colLabel in table.dataTypes.keys():
                    if colLabel not in requiredCols:
                        toDelete.add(colLabel)
            # delete columns
            for colLabel in toDelete:
                table.deleteCol(colLabel)

        # Optimise WHERE
        redundant = set()
        for i in range(len(self.json['where'])):
            clause = self.json['where'][i]
            op = clause['op']
            # left is literal
            if 'literal' in clause['left']:
                tableName = clause['right']['column']['table']
                rightCol = clause['right']['column']['name']
                # right table is known
                if tableName:
                    table = self.tables[tableName]
                    # find col index of column reference in table
                    colIndex = table.labels.index(table.alias+'.'+rightCol)
                    # keep rows where literal evaluated with operator on rightCol is true
                    table.data = table.data[table.data[colIndex].map(lambda x: self.__opMap(op, clause['left']['literal'], x))]
                # right table is not known
                else:
                    for table in self.tables.values():
                        if rightCol in table.dataTypes:
                            table = self.tables[table.alias]
                            # find col index of column reference in table
                            colIndex = table.labels.index(table.alias+'.'+rightCol)
                            # keep rows where literal evaluated with operator on rightCol is true
                            table.data = table.data[table.data[colIndex].map(lambda x: self.__opMap(op, clause['left']['literal'], x))]
                # remove WHERE clause
                redundant.add(i)
            # right is literal
            elif 'literal' in clause['right']:
                tableName = clause['left']['column']['table']
                leftCol = clause['left']['column']['name']
                # left table is known
                if tableName:
                    table = self.tables[tableName]
                    # find col index of column reference in table
                    colIndex = table.labels.index(table.alias+'.'+leftCol)
                    # keep rows where leftCol evaluated with operator on literal is true
                    table.data = table.data[table.data[colIndex].map(lambda x: self.__opMap(op, x, clause['right']['literal']))]
                else:
                    for table in self.tables.values():
                        if leftCol in table.dataTypes:
                            table = self.tables[table.alias]
                            # find col index of column reference in table
                            colIndex = table.labels.index(table.alias+'.'+leftCol)
                            # keep rows where literal evaluated with operator on rightCol is true
                            table.data = table.data[table.data[colIndex].map(lambda x: self.__opMap(op, x, clause['right']['literal']))]
                # remove WHERE clause
                redundant.add(i)
        # remove redundant where clauses
        self.json['where'] = [clause for clause in self.json['where'] if self.json['where'].index(clause) not in redundant]

    def __opMap(self, op, left, right):
        """
        Helper function evaluate left op right, given a string represtation of each
        :param op: string representation of the logical operator
        :param left: value on left of operator
        :param right: value on right of operator
        :return: Boolean value of evaluating val1 w.r.t. val2, given the operator
        """
        opMap = {
            '=': operator.eq,
            '!=': operator.ne,
            '>': operator.gt,
            '>=': operator.ge,
            '<': operator.lt,
            '<=': operator.le
        }

        return opMap[op](left, right)

    def __select(self, data, selectStatement):
        """

        :param data:
        :param selectStatement:
        :return:
        """
        # note that the table reference may be None - infer the table in this case
        selectedColumns = []
        for statement in selectStatement:
                tableName = statement['column']['table']
                columnName = statement['column']['name']
                columnAlias = statement['as']
                if tableName:
                    colLabel = tableName + '.' + columnName
                    data = data.rename(columns={colLabel: columnAlias})
                    # rename this col and add to selectedColumns
                    selectedColumns.append(columnAlias)
                else:
                    # column reference not given -  wildcard filter (note: guaranteed only 1 match)
                    colLabel = data.filter(like=columnName).columns[0]
                    # rename this col and add to selectedColumns
                    data = data.rename(columns={colLabel: columnAlias})
                    # rename this col and add to selectedColumns
                    selectedColumns.append(columnAlias)

        # isolate selected columns
        return data[selectedColumns]

    def __where(self, whereClauses):
        """
        Function to take full cross product matrix and apply where clauses
        :param data:
        :param whereClauses:
        :return:
        """
        data = self.crossProduct
        for clause in whereClauses:
                # clause operator -  op: "=" | "!=" | ">" | ">=" | "<" | "<="
                op = clause['op']
                # left and right will always be column references - as literal expressions have been pre-evaluated
                leftColumnName = self.__genColName(clause['left'])
                rightColumnName = self.__genColName(clause['right'])
                data = data[self.__opMap(op, data[leftColumnName], data[rightColumnName])]

        return data

    def __genColName(self, clause):
        """
        helper function to deal with null table reference in WHERE statement
        :param statement:
        :return:
        """
        if clause['column']['table']:
            return clause['column']['table'] + '.' + clause['column']['name']
        else:
            # use string matching to find column name
            return self.crossProduct.filter(like=clause['column']['name']).columns[0]

    def noErrors(self):
        """
        Function to check query for errors in SELECT and WHERE statements
        :return: Tuple: (Boolean of query validity, error message | None)
        """
        # check SELECT statement clauses are valid
        for selectStatement in self.json['select']:
            selectedTable = selectStatement['column']['table']
            selectedCol = selectStatement['column']['name']
            if selectedTable:
                # check selected table exists
                if selectedTable not in self.tables:
                    return False, 'ERROR: Unknown table name "{}".'.format(selectedTable)
                # check selected name exists in given table
                if selectedTable+'.'+selectedCol not in self.tables[selectedTable].labels:
                    return False, 'ERROR: Unknown column "{}" in table "{}".'.format(selectedCol, selectedTable)
            # no table reference provided
            else:
                # check that selected column exists in just one table
                foundInTable = []
                for table in self.tables.values():
                    if selectedCol in table.dataTypes.keys():
                        # column found in this table
                        foundInTable.append(table.alias)
                if len(foundInTable) < 1:
                    return False, 'ERROR: Unknown column "{}".'.format(selectedCol)
                elif len(foundInTable) > 1:
                    return False, \
                           'ERROR: Column reference "{}" is ambiguous; present in multiple tables: "{}", "{}".'\
                               .format(selectedCol, foundInTable[0], foundInTable[1])

        # check WHERE clause for comparison operator use on incompatible data types
        for whereClause in self.json['where']:
            op = whereClause['op']
            left = whereClause['left']
            right = whereClause['right']
            leftDType = self.__getDType(left)
            rightDType = self.__getDType(right)
            if leftDType != rightDType:
                return False, 'ERROR: Incompatible types to "{}": {} and {}.'.format(op, leftDType, rightDType)

        return True, None

    def __getDType(self, dataRef):
        """
        Helper function to fetch data type for column in table
        :param dataRef: json of data reference
        :return:
        """
        if 'literal' in dataRef:
            return type(dataRef['literal']).__name__

        else:
            # column reference
            dataRef = dataRef['column']
            table, colName = dataRef['table'], dataRef['name']
            if table:
                return self.tables[table].dataTypes[colName]
            else:
                # table reference is None
                for table in self.tables.values():
                    if colName in table.dataTypes:
                        return table.dataTypes[colName]

    def __constructTables(self):
        """
        Function to create hashmap of {table alias : Table object} for all tables in query
        :return: hashmap {table alias : Table object}
        """
        tables = {}
        for tableRef in self.json['from']:
            source = tableRef['source']
            alias = tableRef['as']
            filePath = self.tableFolder + '/' + source + '.table.json'
            # open JSON file
            with open(filePath) as f:
                 tableJSON = json.load(f)

            # construct Table object
            table = Table(tableJSON, alias)
            # add Table to tables hashmap with key -> alias
            tables[alias] = table

        return tables


    def __constructCrossPoduct(self):
        """
        Function to construct cross product of all tables in query and bind to self.crossProduct
        :return: None
        """
        # if there is 1 table, cross product is just itself
        if len(self.tables) == 1:
            table = list(self.tables.values())[0]
            labels = table.labels
            table.data.columns = labels
            self.crossProduct = table.data
        else:
            # calculate cross product of all tables
            tableData  = [table.data for table in self.tables.values()]
            result = reduce(lambda left, right: pd.merge(left.assign(key=1),
                                                         right.assign(key=1), on='key'),
                            tableData)
            # add column labels
            result = result.drop('key', 1)
            labels = []
            for table in self.tables.values():
                tableLabels = table.labels
                labels.extend(tableLabels)
            result.columns = labels

            self.crossProduct = result

class Table:
    def __init__(self, tableJSON, alias):
        dataTypes = {}
        labels = []
        for colLabel in tableJSON[0]:
            label = colLabel[0]
            dataType = colLabel[1]
            dataTypes[label] = dataType
            labels.append(alias + '.' + label)

        self.labels = labels
        self.dataTypes = dataTypes
        self.data = pd.DataFrame(tableJSON[1:])
        self.alias = alias

    def deleteCol(self, column):
        """
        Method to delete column at column label = column
        :param column: str of column label
        :return: None
        """
        if column in self.dataTypes:
            colLabel = self.alias+'.'+column
            index = self.labels.index(colLabel)
            # remove column label and its mapping to data type
            self.labels.pop(index)
            self.dataTypes.pop(column)
            # remove column data
            self.data = self.data.drop(index, axis=1)
