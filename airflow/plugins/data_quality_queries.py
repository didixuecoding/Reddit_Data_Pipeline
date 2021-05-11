class DataQualityQueries:
    @staticmethod
    def table_not_empty(table):
        """
        Tests if a given table has at least one row.
        :param table: Name of the table.
        :return: SQL statement that returns a single column 'result' with
            either the value 1 if the table has at least one entry, otherwise
            it returns 0.
        """
        return """
        SELECT (CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END) AS result
        FROM {table:}
        """.format(**dict(table=table))

    @staticmethod
    def check_table_not_empty(result):
        return result['result'][0] == 1

    @staticmethod
    def col_does_not_contain_null(table, col):
        """
        Tests if a given column in a table contain any null value
        :param table: Name of the table.
        :param col: Name of the column.
        :return: SQL statement that returns a single column 'result' indicate how many rows are null.
        """
        return """
        SELECT COUNT({col:}) AS result
        FROM {table:}
        WHERE "{col:}" IS NULL 
        """.format(**dict(table=table, col=col))

    @staticmethod
    def check_col_does_not_contain_null(result):
        return result['result'][0] == 0

    @staticmethod
    def col_does_not_contain_str(table, col, val):
        """
        Tests if a given column in a table contain any given string
        :param table: Name of the table.
        :param col: Name of the column.
        :param val: String that should be checked
        :return: SQL statement that returns a single column 'result' indicate how many rows are contain given string.
        """
        return """
        SELECT COUNT({col:}) AS result
        FROM {table:}
        WHERE "{col:}" LIKE "%{val:}%"
        """.format(**dict(table=table, col=col, val=val))

    @staticmethod
    def check_col_does_not_contain_str(result):
        return result["result"][0] == 0
