#  Copyright (c) 2010 Franz Allan Valencia See
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from robot.api import logger


class Assertion(object):
    """
    Assertion handles all the assertions of Database Library.
    """

    def check_if_exists_in_database(self, selectStatement, sansTran=False):
        """
        Check if any row would be returned by given the input `selectStatement`. If there are no results, then this will
        throw an AssertionError. Set optional input `sansTran` to True to run command without an explicit transaction
        commit or rollback.

        For example, given we have a table `person` with the following data:
        | id | first_name  | last_name |
        |  1 | Franz Allan | See       |

        When you have the following assertions in your robot
        | Check If Exists In Database | SELECT id FROM person WHERE first_name = 'Franz Allan' |
        | Check If Exists In Database | SELECT id FROM person WHERE first_name = 'John' |

        Then you will get the following:
        | Check If Exists In Database | SELECT id FROM person WHERE first_name = 'Franz Allan' | # PASS |
        | Check If Exists In Database | SELECT id FROM person WHERE first_name = 'John' | # FAIL |

        Using optional `sansTran` to run command without an explicit transaction commit or rollback:
        | Check If Exists In Database | SELECT id FROM person WHERE first_name = 'John' | True |
        """
        logger.info ('Executing : Check If Exists In Database  |  %s ' % selectStatement)
        if not self.query(selectStatement, sansTran):
            raise AssertionError("Expected to have have at least one row from '%s' "
                                 "but got 0 rows." % selectStatement)

    def check_if_not_exists_in_database(self, selectStatement, sansTran=False):
        """
        This is the negation of `check_if_exists_in_database`.

        Check if no rows would be returned by given the input `selectStatement`. If there are any results, then this
        will throw an AssertionError. Set optional input `sansTran` to True to run command without an explicit
        transaction commit or rollback.

        For example, given we have a table `person` with the following data:
        | id | first_name  | last_name |
        |  1 | Franz Allan | See       |

        When you have the following assertions in your robot
        | Check If Not Exists In Database | SELECT id FROM person WHERE first_name = 'John' |
        | Check If Not Exists In Database | SELECT id FROM person WHERE first_name = 'Franz Allan' |

        Then you will get the following:
        | Check If Not Exists In Database | SELECT id FROM person WHERE first_name = 'John' | # PASS |
        | Check If Not Exists In Database | SELECT id FROM person WHERE first_name = 'Franz Allan' | # FAIL |

        Using optional `sansTran` to run command without an explicit transaction commit or rollback:
        | Check If Not Exists In Database | SELECT id FROM person WHERE first_name = 'John' | True |
        """
        logger.info('Executing : Check If Not Exists In Database  |  %s ' % selectStatement)
        queryResults = self.query(selectStatement, sansTran)
        if queryResults:
            raise AssertionError("Expected to have have no rows from '%s' "
                                 "but got some rows : %s." % (selectStatement, queryResults))

    def row_count_is_0(self, selectStatement, sansTran=False):
        """
        Check if any rows are returned from the submitted `selectStatement`. If there are, then this will throw an
        AssertionError. Set optional input `sansTran` to True to run command without an explicit transaction commit or
        rollback.

        For example, given we have a table `person` with the following data:
        | id | first_name  | last_name |
        |  1 | Franz Allan | See       |

        When you have the following assertions in your robot
        | Row Count is 0 | SELECT id FROM person WHERE first_name = 'Franz Allan' |
        | Row Count is 0 | SELECT id FROM person WHERE first_name = 'John' |

        Then you will get the following:
        | Row Count is 0 | SELECT id FROM person WHERE first_name = 'Franz Allan' | # FAIL |
        | Row Count is 0 | SELECT id FROM person WHERE first_name = 'John' | # PASS |

        Using optional `sansTran` to run command without an explicit transaction commit or rollback:
        | Row Count is 0 | SELECT id FROM person WHERE first_name = 'John' | True |
        """
        logger.info('Executing : Row Count Is 0  |  %s ' % selectStatement)
        num_rows = self.row_count(selectStatement, sansTran)
        if num_rows > 0:
            raise AssertionError("Expected zero rows to be returned from '%s' "
                                 "but got rows back. Number of rows returned was %s" % (selectStatement, num_rows))

    def row_count_is_equal_to_x(self, selectStatement, numRows, sansTran=False):
        """
        Check if the number of rows returned from `selectStatement` is equal to the value submitted. If not, then this
        will throw an AssertionError. Set optional input `sansTran` to True to run command without an explicit
        transaction commit or rollback.

        For example, given we have a table `person` with the following data:
        | id | first_name  | last_name |
        |  1 | Franz Allan | See       |
        |  2 | Jerry       | Schneider |

        When you have the following assertions in your robot
        | Row Count Is Equal To X | SELECT id FROM person | 1 |
        | Row Count Is Equal To X | SELECT id FROM person WHERE first_name = 'John' | 0 |

        Then you will get the following:
        | Row Count Is Equal To X | SELECT id FROM person | 1 | # FAIL |
        | Row Count Is Equal To X | SELECT id FROM person WHERE first_name = 'John' | 0 | # PASS |

        Using optional `sansTran` to run command without an explicit transaction commit or rollback:
        | Row Count Is Equal To X | SELECT id FROM person WHERE first_name = 'John' | 0 | True |
        """
        logger.info('Executing : Row Count Is Equal To X  |  %s  |  %s ' % (selectStatement, numRows))
        num_rows = self.row_count(selectStatement, sansTran)
        if num_rows != int(numRows.encode('ascii')):
            raise AssertionError("Expected same number of rows to be returned from '%s' "
                                 "than the returned rows of %s" % (selectStatement, num_rows))

    def row_count_is_greater_than_x(self, selectStatement, numRows, sansTran=False):
        """
        Check if the number of rows returned from `selectStatement` is greater than the value submitted. If not, then
        this will throw an AssertionError. Set optional input `sansTran` to True to run command without an explicit
        transaction commit or rollback.

        For example, given we have a table `person` with the following data:
        | id | first_name  | last_name |
        |  1 | Franz Allan | See       |
        |  2 | Jerry       | Schneider |

        When you have the following assertions in your robot
        | Row Count Is Greater Than X | SELECT id FROM person | 1 |
        | Row Count Is Greater Than X | SELECT id FROM person WHERE first_name = 'John' | 0 |

        Then you will get the following:
        | Row Count Is Greater Than X | SELECT id FROM person | 1 | # PASS |
        | Row Count Is Greater Than X | SELECT id FROM person WHERE first_name = 'John' | 0 | # FAIL |

        Using optional `sansTran` to run command without an explicit transaction commit or rollback:
        | Row Count Is Greater Than X | SELECT id FROM person | 1 | True |
        """
        logger.info('Executing : Row Count Is Greater Than X  |  %s  |  %s ' % (selectStatement, numRows))
        num_rows = self.row_count(selectStatement, sansTran)
        if num_rows <= int(numRows.encode('ascii')):
            raise AssertionError("Expected more rows to be returned from '%s' "
                                 "than the returned rows of %s" % (selectStatement, num_rows))

    def row_count_is_less_than_x(self, selectStatement, numRows, sansTran=False):
        """
        Check if the number of rows returned from `selectStatement` is less than the value submitted. If not, then this
        will throw an AssertionError. Set optional input `sansTran` to True to run command without an explicit
        transaction commit or rollback.

        For example, given we have a table `person` with the following data:
        | id | first_name  | last_name |
        |  1 | Franz Allan | See       |
        |  2 | Jerry       | Schneider |

        When you have the following assertions in your robot
        | Row Count Is Less Than X | SELECT id FROM person | 3 |
        | Row Count Is Less Than X | SELECT id FROM person WHERE first_name = 'John' | 1 |

        Then you will get the following:
        | Row Count Is Less Than X | SELECT id FROM person | 3 | # PASS |
        | Row Count Is Less Than X | SELECT id FROM person WHERE first_name = 'John' | 1 | # FAIL |

        Using optional `sansTran` to run command without an explicit transaction commit or rollback:
        | Row Count Is Less Than X | SELECT id FROM person | 3 | True |
        """
        logger.info('Executing : Row Count Is Less Than X  |  %s  |  %s ' % (selectStatement, numRows))
        num_rows = self.row_count(selectStatement, sansTran)
        if num_rows >= int(numRows.encode('ascii')):
            raise AssertionError("Expected less rows to be returned from '%s' "
                                 "than the returned rows of %s" % (selectStatement, num_rows))

    def table_must_exist(self, tableName, sansTran=False):
        """
        Check if the table given exists in the database. Set optional input `sansTran` to True to run command without an
        explicit transaction commit or rollback.

        For example, given we have a table `person` in a database

        When you do the following:
        | Table Must Exist | person |

        Then you will get the following:
        | Table Must Exist | person | # PASS |
        | Table Must Exist | first_name | # FAIL |

        Using optional `sansTran` to run command without an explicit transaction commit or rollback:
        | Table Must Exist | person | True |
        """
        logger.info('Executing : Table Must Exist  |  %s ' % tableName)
        if self.db_api_module_name in ["cx_Oracle"]:
            selectStatement = ("SELECT * FROM all_objects WHERE object_type IN ('TABLE','VIEW') AND owner = SYS_CONTEXT('USERENV', 'SESSION_USER') AND object_name = UPPER('%s')" % tableName)
        elif self.db_api_module_name in ["sqlite3"]:
            selectStatement = ("SELECT name FROM sqlite_master WHERE type='table' AND name='%s' COLLATE NOCASE" % tableName)
        elif self.db_api_module_name in ["ibm_db", "ibm_db_dbi"]:
            selectStatement = ("SELECT name FROM SYSIBM.SYSTABLES WHERE type='T' AND name=UPPER('%s')" % tableName)
        else:
            selectStatement = ("SELECT * FROM information_schema.tables WHERE table_name='%s'" % tableName)
        num_rows = self.row_count(selectStatement, sansTran)
        if num_rows == 0:
            raise AssertionError("Table '%s' does not exist in the db" % tableName)

    def compare_table_structure_snowflake(self, script, test_schema_name, test_table_name, result_schema_name, result_table_name):

        logger.info('Executing : Compare Table Structure for table |  %s ' % test_table_name)

        # deploy table/view
        self.execute_sql_script(script)

        query = f'select * from information_schema.columns columns_left full outer join information_schema.columns columns_right on columns_left.column_name = columns_right.column_name where columns_left.table_schema = \'{test_schema_name}\' and columns_left.table_name = \'{test_table_name}\' and columns_right.table_schema = \'{result_schema_name}\' and columns_right.table_name = \'{result_table_name}\' and (columns_left.data_type <> columns_right.data_type or columns_left.ordinal_position <> columns_right.ordinal_position)'

        num_rows = self.row_count(query)

        if num_rows > 0:
            raise AssertionError("Table structure of table '%s' does not match expected result. The number of faulty columns is '%s'" % (test_table_name, num_rows))

# ADDED FUNCTIONALITY BELOW


    def compare_table_structure(self, test_table_name, schema_name, compare_table_name):

        logger.info('Executing : Compare Table Structure for table |  %s ' % test_table_name)

        query = f'WITH DB AS (SELECT TABLE_NAME,COLUMN_NAME,DATA_TYPE FROM ALL_TAB_COLUMNS WHERE OWNER = \'{schema_name}\' AND TABLE_NAME IN (\'{test_table_name}\')), {compare_table_name}_VW_STRUC AS (SELECT * FROM EDW_TALEND.{compare_table_name}_VIEW_STRUCTURE WHERE TABLE_NAME = REPLACE(\'{test_table_name}\', \'VW_AVALOQ_{compare_table_name}_\', \'\')) SELECT * FROM DB FULL OUTER JOIN {compare_table_name}_VW_STRUC ON {compare_table_name}_VW_STRUC.COLUMN_NAME = DB.COLUMN_NAME WHERE DB.COLUMN_NAME IS NULL OR {compare_table_name}_VW_STRUC.COLUMN_NAME IS NULL'

        num_rows = self.row_count(query)

        if num_rows > 0:
            raise AssertionError("Table structure of table '%s' does not match expected result. The number of faulty columns is '%s'" % (test_table_name, num_rows))

    def check_query_result(self, script, test_schema_name, test_table_name, result_schema_name, result_table_name, sansTrans=False):

        logger.info('Executing : Compare Query Result |  %s ' % script)

        # deploy table/view
        self.execute_sql_script(script)

        num_rows = self.compare_tables(test_schema_name, test_table_name, result_schema_name, result_table_name)

        if num_rows > 0:
            raise AssertionError("Result of script '%s' does not match expected result. The number of faulty rows is '%s'" % (script, num_rows))

    def script_row_count_is_greater_than_x(self, sqlScriptFileName, numRows, sansTran=False):
        
        selectStatement = self.open_sql(sqlScriptFileName)
        
        self.row_count_is_greater_than_x(selectStatement, numRows)
        
    def script_row_count_is_equal_to_x(self, sqlScriptFileName, numRows, sansTran=False):
        
        selectStatement = self.open_sql(sqlScriptFileName)
        
        self.row_count_is_equal_to_x(selectStatement, numRows)
        
    def count_specific_table(self, schema_name, table_name, num_rows):

        logger.info('Executing : Count for specific table: |  %s ' % table_name)

        query = f'SELECT COUNT(*) FROM {schema_name}.{table_name}'
        
        self.row_count_is_equal_to_x(query, num_rows)
    
    def count_unique_rows_table2(self, schema_name, table_name, unique_key, num_rows):

        logger.info('Executing : Count (unique rows) for table: |  %s ' % table_name)

        uniqueRows = f'SELECT COUNT(DISTINCT {unique_key}) FROM {schema_name}.{table_name}'
        
        self.row_count_is_equal_to_x(uniqueRows, num_rows)

    def count_empty_column_value_percentage(self, schema_name, table_name, column_name, percentage):

        query_total = f'SELECT * FROM {schema_name}.{table_name}'
        query_column = f'SELECT * FROM {schema_name}.{table_name} WHERE {column_name} IS NULL'
        
        num_rows_total = self.row_count(query_total)
        num_rows_columns = self.row_count(query_column)
        
        percentage_calculated = (num_rows_columns/num_rows_total) * 100
        
        if percentage_calculated > float(percentage):
            raise AssertionError("Percentage of EMPTY columns is '%s' is higher than allow percentage '%s'" % (percentage_calculated, percentage))
 
    def row_unique_check2(self, count_key, count_total):
        
        selectStatement1 = self.open_sql(count_key)
        selectStatement2 = self.open_sql(count_total)
        
        cur = None
        cur = self._dbconnection.cursor()
        result_key = self.query(selectStatement1)
        result_total = self.query(selectStatement2)
        
        rowunique = round(float('.'.join(str(elem) for elem in result_key[0])))
        objectrows = round(float('.'.join(str(elem) for elem in result_total[0])))
      
        if rowunique != objectrows:
            raise AssertionError("Key is not unique, total rows: '%s', key rows: '%s'" % (objectrows, rowunique))

    def value_variance_check(self, schema_name, table_name, unique_key, field):
        logger.info('Executing : Values variance check: |  %s ' % table_name)
        
        result_uniqueRows = self.count_unique_rows_table(schema_name, table_name, unique_key)

        uniqueRows = round(float('.'.join(str(elem) for elem in result_uniqueRows[0])))

        if int(field) == 0:
            if uniqueRows != int(field):
                raise AssertionError("Expected: '%s'   Actual: '%s'" % (field, uniqueRows))
        elif int(field) == 1:
            if uniqueRows != int(field):
                raise AssertionError("Expected: '%s'   Actual: '%s'" % (field, uniqueRows))
        elif int(field) == 2:
            if uniqueRows < int(field):
                raise AssertionError("Expected: '%s'   Actual: '%s'" % (field, uniqueRows))
        else:
            raise AssertionError("Check 4th argument in TC, expected value 0, 1 or 2")
        
    def row_unique_check(self, schema_name, table_name, unique_key):  
        """
        SELECT COUNT(*), COUNT(DISTINCT {unique_key})
        FROM {schema_name}.{table_name}
        """
        logger.info('Executing : Row unique check for table: |  %s ' % table_name)
        
        result_uniqueRows = self.count_unique_rows_table(schema_name, table_name, unique_key)
        result_objectRows = self.count_object_rows_table(schema_name, table_name)

        uniqueRows = round(float('.'.join(str(elem) for elem in result_uniqueRows[0])))
        objectRows = round(float('.'.join(str(elem) for elem in result_objectRows[0])))

        logger.info("Object rows: '%s', unique rows '%s': '%s'" % (objectRows, unique_key, uniqueRows))

        if uniqueRows != objectRows:
            raise AssertionError("Object rows: '%s', unique rows '%s': '%s'" % (objectRows, unique_key, uniqueRows))

    def row_unique_check_argument(self, schema_name, table_name, unique_key, argument):  
        """
        SELECT COUNT(*), COUNT(DISTINCT {unique_key})
        FROM {schema_name}.{table_name}
        WHERE {argument}
        """
        logger.info('Executing : Row unique check for table: |  %s ' % table_name)
        
        result_uniqueRows = self.count_unique_rows_table(schema_name, table_name, unique_key)
        result_objectRows = self.count_object_rows_table_argument(schema_name, table_name, argument)

        uniqueRows = round(float('.'.join(str(elem) for elem in result_uniqueRows[0])))
        objectRows = round(float('.'.join(str(elem) for elem in result_objectRows[0])))

        logger.info("Object rows: '%s', unique rows '%s': '%s'" % (objectRows, unique_key, uniqueRows))

        if uniqueRows != objectRows:
            raise AssertionError("Object rows: '%s', unique rows '%s': '%s'" % (objectRows, unique_key, uniqueRows))

    def dimensional_integrity_check_found(self, schema_name, table_name, unique_key, foreign_table_name, foreign_key):  
        """
        SELECT COUNT(DISTINCT {unique_key}), COUNT(DISTINCT {foreign_key})
        FROM {schema_name}.{table_name}
        LEFT OUTER JOIN {foreign_table_name}
        ON {foreign_key} = {unique_key}
        """
        logger.info('Executing : Dimensional integrity check: | Between tables %s and %s' % (table_name, foreign_table_name))
        
        result_foundRows = self.count_found_rows_table(schema_name, table_name, unique_key, foreign_table_name, foreign_key)
        result_uniqueRows = self.count_unique_rows_table(schema_name, table_name, unique_key)

        foundRows = round(float('.'.join(str(elem) for elem in result_foundRows[0])))
        uniqueRows = round(float('.'.join(str(elem) for elem in result_uniqueRows[0])))

        logger.info("Unique rows '%s': '%s', found rows '%s': '%s'" % (unique_key, uniqueRows, foreign_key, foundRows))

        if foundRows != uniqueRows:
            raise AssertionError("Unique rows '%s': '%s', found rows '%s': '%s'" % (unique_key, uniqueRows, foreign_key, foundRows))
    
    def dimensional_integrity_check_found_joinArgument(self, schema_name, table_name, unique_key, foreign_table_name, foreign_key, argument):  
        """
        SELECT COUNT(DISTINCT {unique_key}), COUNT(DISTINCT {foreign_key})
        FROM {schema_name}.{table_name}
        LEFT OUTER JOIN {foreign_table_name}
        ON {foreign_key} = {unique_key}
        AND {argument}
        """
        logger.info('Executing : Dimensional integrity check: | Between tables %s and %s' % (table_name, foreign_table_name))
        
        result_foundRows = self.count_found_rows_table_argument(schema_name, table_name, unique_key, foreign_table_name, foreign_key, argument)
        result_uniqueRows = self.count_unique_rows_table(schema_name, table_name, unique_key)

        foundRows = round(float('.'.join(str(elem) for elem in result_foundRows[0])))
        uniqueRows = round(float('.'.join(str(elem) for elem in result_uniqueRows[0])))

        logger.info("Unique rows '%s': '%s', found rows '%s': '%s'" % (unique_key, uniqueRows, foreign_key, foundRows))

        if foundRows != uniqueRows:
            raise AssertionError("Unique rows '%s': '%s', found rows '%s': '%s'" % (unique_key, uniqueRows, foreign_key, foundRows))


    def dimensional_integrity_check_found_argument(self, schema_name, table_name, unique_key, argument, foreign_table_name, foreign_key):  
        """
        SELECT COUNT(DISTINCT {unique_key}), COUNT(DISTINCT {foreign_key})
        FROM {schema_name}.{table_name}
        LEFT OUTER JOIN {foreign_table_name}
        ON {foreign_key} = {unique_key}
        WHERE {argument}
        """
        logger.info('Executing : Dimensional integrity check: | Between tables %s and %s' % (table_name, foreign_table_name))
        
        result_foundRows = self.count_found_rows_table(schema_name, table_name, unique_key, foreign_table_name, foreign_key)
        result_uniqueRows = self.count_unique_rows_table_argument(schema_name, table_name, unique_key, argument)

        foundRows = round(float('.'.join(str(elem) for elem in result_foundRows[0])))
        uniqueRows = round(float('.'.join(str(elem) for elem in result_uniqueRows[0])))

        logger.info("Unique rows '%s': '%s', found rows '%s': '%s'" % (unique_key, uniqueRows, foreign_key, foundRows))

        if foundRows != uniqueRows:
            raise AssertionError("Unique rows '%s': '%s', found rows '%s': '%s'" % (unique_key, uniqueRows, foreign_key, foundRows))
    
    def dimensional_integrity_check_found_withClause_argument(self, schema_name, table_name, unique_key, argument, foreign_table_name, foreign_key):  
        """
        WITH rel AS (SELECT {unique_key} FROM {schema_name}.{table_name})
        SELECT COUNT(DISTINCT {unique_key}), COUNT(DISTINCT {foreign_key})
        FROM rel
        LEFT OUTER JOIN {foreign_table_name}
        ON {foreign_key} = {unique_key}
        WHERE {argument}
        """
        logger.info('Executing : Dimensional integrity check: | Between tables %s and %s' % (table_name, foreign_table_name))
        
        result_foundRows = self.count_found_rows_table_withClause(schema_name, table_name, unique_key, foreign_table_name, foreign_key)
        result_uniqueRows = self.count_unique_rows_table_argument(schema_name, table_name, unique_key, argument)

        foundRows = round(float('.'.join(str(elem) for elem in result_foundRows[0])))
        uniqueRows = round(float('.'.join(str(elem) for elem in result_uniqueRows[0])))

        logger.info("Unique rows '%s': '%s', found rows '%s': '%s'" % (unique_key, uniqueRows, foreign_key, foundRows))

        if foundRows != uniqueRows:
            raise AssertionError("Unique rows '%s': '%s', found rows '%s': '%s'" % (unique_key, uniqueRows, foreign_key, foundRows))

    def dimensional_integrity_check_potential(self, schema_name, table_name, unique_key, foreign_key):  
        """
        SELECT COUNT(DISTINCT {unique_key}), COUNT(DISTINCT {foreign_key})
        FROM {schema_name}.{table_name}
        """
        logger.info('Executing : Dimensional integrity check: | %s' % table_name)
        
        result_potentialRows = self.count_unique_rows_table(schema_name, table_name, foreign_key)
        result_uniqueRows = self.count_unique_rows_table(schema_name, table_name, unique_key)

        potentialRows = round(float('.'.join(str(elem) for elem in result_potentialRows[0])))
        uniqueRows = round(float('.'.join(str(elem) for elem in result_uniqueRows[0])))

        logger.info("Unique rows '%s': '%s', found rows '%s': '%s'" % (unique_key, uniqueRows, foreign_key, potentialRows))

        if potentialRows != uniqueRows:
            raise AssertionError("Unique rows '%s': '%s', potential rows '%s': '%s'" % (unique_key, uniqueRows, foreign_key, potentialRows))