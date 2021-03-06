"""
File to run JSON SQL queries
"""

import sys
import json
from Query import Query


def main():
    """
    Function to parse command line input of form '$ sqlEval.py <table-folder> <sql-json-file> <output-file>' and write
    result of query to output file
    :return: None
    """
    tableFolder = sys.argv[1]
    sqlJsonFile = sys.argv[2]
    outputFile = sys.argv[3]

    # parse sql json file
    with open(sqlJsonFile) as f:
        queryJSON = json.load(f)

    # run query
    q = Query(queryJSON, tableFolder)
    result = q.run()

    # write result to output pile
    writeToFile(result, outputFile)


def writeToFile(result, outputFile):
    """
    Helper function to write result of query run to file
    :param result: result of Query.run() -> {'success': True | False, 'JSON': list of query result or str error message}
    :param outputFile: filepath of output file - created if none exist
    :return: None
    """
    # write result to output pile
    file = open(outputFile, 'w')
    if result['success']:
        file.write('[')
        file.write('\n')
        for entry in result['JSON'][:-1]:
            stringEntry = (str(entry) + ',').replace("'", '"')
            width = len(stringEntry)
            stringEntry = stringEntry.rjust(width + 4)
            file.write(stringEntry)
            file.write('\n')
        entry = result['JSON'][-1]
        stringEntry = (str(entry)).replace("'", '"')
        width = len(stringEntry)
        stringEntry = stringEntry.rjust(width + 4)
        file.write(stringEntry)
        file.write('\n')
        file.write(']')
    else:
        file.write(str(result['JSON']))

    file.write('\n')
    file.close()


if __name__ == '__main__':
    main()
