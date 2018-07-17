import cx_Oracle
import gwas_data_sources

class gwasCatalogDbConnector(object):
    '''
    General Oracle database connection methods.
    '''
    
    def __init__(self, database_name):

        self.database_name = database_name

        # Trying to connect:
        try:
            ip, port, sid, username, password = gwas_data_sources.get_db_properties(self.database_name)
            dsn_tns = cx_Oracle.makedsn(ip, port, sid)
            self.connection = cx_Oracle.connect(username, password, dsn_tns)
            self.cursor = self.connection.cursor()
            print "[Info] Database successfully loaded."

        except cx_Oracle.DatabaseError, exception:
            print exception

    def close(self):
        self.connection.close()
