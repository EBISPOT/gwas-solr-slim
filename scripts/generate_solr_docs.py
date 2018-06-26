# Activate Python venv for the script - uncomment to run script on commandline
activate_this_file = "/path/to/bin/activate_this.py"
execfile(activate_this_file, dict(__file__ = activate_this_file))

import cx_Oracle
import contextlib
import argparse
import sys
from tqdm import tqdm
import json

sys.path.insert(0, '/path/to/gwas_data_sources')
import gwas_data_sources


def get_publicaton_data():
    '''
    Get Publication data for Solr document.
    '''

    # List of queries
    publication_sql = """
        SELECT DISTINCT(P.PUBMED_ID), P.PUBLICATION, A.FULLNAME_STANDARD, A.ORCID, 
            P.TITLE, TO_CHAR(P.PUBLICATION_DATE, 'yyyy-mm-dd')
        FROM PUBLICATION P, AUTHOR A, STUDY S, HOUSEKEEPING H
        WHERE P.FIRST_AUTHOR_ID=A.ID
            and S.PUBLICATION_ID=P.ID and S.HOUSEKEEPING_ID=H.ID 
            and H.IS_PUBLISHED=1
    """


    publication_author_list_sql = """
    SELECT A.FULLNAME_STANDARD
    FROM PUBLICATION P, AUTHOR A, PUBLICATION_AUTHORS PA, STUDY S, HOUSEKEEPING H
    WHERE P.ID=PA.PUBLICATION_ID and PA.AUTHOR_ID=A.ID
          and S.PUBLICATION_ID=P.ID and S.HOUSEKEEPING_ID=H.ID
          and H.IS_PUBLISHED=1
          and P.PUBMED_ID= :pubmed_id
    ORDER BY PA.SORT ASC
    """


    all_publication_data = []

    try:
        ip, port, sid, username, password = gwas_data_sources.get_db_properties('DEV3')
        dsn_tns = cx_Oracle.makedsn(ip, port, sid)
        connection = cx_Oracle.connect(username, password, dsn_tns)

        # cursor = connection.cursor()
        with contextlib.closing(connection.cursor()) as cursor:

            cursor.execute(publication_sql)

            publication_data = cursor.fetchall()

            for publication in tqdm(publication_data):
                publication = list(publication)

                # get author list
                cursor.prepare(publication_author_list_sql)                
                r = cursor.execute(None, {'pubmed_id': publication[0]})
                all_authors = cursor.fetchall()

                authors = []
                for pmid_author in all_authors:
                    # convert to string and remove trailing space and comma
                    author = str(pmid_author[0])
                    authors.append(author)
                    
                # add authors to publication data
                publication.append(authors)

                # add publication data to list of all publication data
                all_publication_data.append(publication)

        # cursor.close()
        connection.close()

        # print "** Num Rows: ", len(all_publication_data), all_publication_data[0]
        return all_publication_data

    except cx_Oracle.DatabaseError, exception:
        print exception


def format_data(data):
    '''
    Convert list of data to JSON.
    '''
    publication_dict = {}
    publication_solr_doc = []
    count = 0

    for row in data:
        print "** Row: ", row
        for item in row:
            count += 1
        #     print "** DataItem: ", item
            publication_dict[count] = item

        publication_solr_doc.append(publication_dict)
        publication_dict = {}
        count = 0

    print json.dumps(publication_solr_doc)


if __name__ == '__main__':
    '''
    Create Solr documents for categories of interest.
    '''


    data = get_publicaton_data()


    format_data(data)
