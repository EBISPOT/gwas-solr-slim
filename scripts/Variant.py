import cx_Oracle
import sys

sys.path.insert(0, '/path/to/gwas_data_sources')
import gwas_data_sources


class Variant:
    '''
    Methods to generate Variant data. 
    '''

    def __init__(self, database_name):
        self.database_name = database_name


    def get_database_connection(self):
        DATABASE_NAME = self.database_name

        try:
            ip, port, sid, username, password = gwas_data_sources.get_db_properties(DATABASE_NAME)
            dsn_tns = cx_Oracle.makedsn(ip, port, sid)
            connection = cx_Oracle.connect(username, password, dsn_tns)
            return connection

        except cx_Oracle.DatabaseError, exception:
            print exception


    def get_snps(self, cursor):
        snp_sql = """
            SELECT SNP.ID, SNP.RS_ID, SNP.FUNCTIONAL_CLASS, 'variant' as resourcename
            FROM SINGLE_NUCLEOTIDE_POLYMORPHISM SNP
        """

        cursor.execute(snp_sql)
        variant_data = cursor.fetchall()

        return variant_data


    def get_variant_location(self, cursor, variant_id):
        snp_location_sql = """
            SELECT L.CHROMOSOME_NAME, L.CHROMOSOME_POSITION, R.NAME
            FROM LOCATION L, SNP_LOCATION SL, SINGLE_NUCLEOTIDE_POLYMORPHISM SNP, REGION R
            WHERE L.ID=SL.LOCATION_ID and SL.SNP_ID=SNP.ID and L.REGION_ID=R.ID 
                and SNP.ID= :snp_id
        """

        cursor.prepare(snp_location_sql)
        r = cursor.execute(None, {'snp_id': variant_id})
        all_snp_locations = cursor.fetchall()

        return all_snp_locations

