import cx_Oracle
import contextlib
import sys

sys.path.insert(0, '/path/to/gwas_data_sources')
import gwas_data_sources


class DBConnection:
    '''
    General Oracle database connection methods.
    '''

    def __init__(self, database_name):
        self.database_name = database_name

        ip, port, sid, username, password = gwas_data_sources.get_db_properties(self.database_name)
        dsn_tns = cx_Oracle.makedsn(ip, port, sid)

        self._db_connection = cx_Oracle.connect(username, password, dsn_tns)
        self._db_cursor = self._db_connection.cursor()


    def query_database(self, query):
        return self._db_cur.execute(query)


    def close_db_connection(self):
        self._db_connection.close()