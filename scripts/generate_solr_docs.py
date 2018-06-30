# Activate Python venv for the script - uncomment to run script on commandline
activate_this_file = "/path/to/bin/activate_this.py"
execfile(activate_this_file, dict(__file__ = activate_this_file))

import cx_Oracle
import contextlib
import argparse
import sys
from tqdm import tqdm
import json
# import csv
# from pprint import pprint
import os.path

sys.path.insert(0, '/path/to/gwas_data_sources')
import gwas_data_sources

import OLSData


def get_publicaton_data():
    '''
    Get Publication data for Solr document.
    '''

    # List of queries
    publication_sql = """
        SELECT P.ID, P.PUBMED_ID, P.PUBLICATION, P.TITLE,
            TO_CHAR(P.PUBLICATION_DATE, 'yyyy-mm-dd'), 'publication' as resourcename
        FROM PUBLICATION P
    """

    publication_author_list_sql = """
        SELECT A.FULLNAME, A.FULLNAME_STANDARD, PA.SORT, A.ORCID
        FROM PUBLICATION P, AUTHOR A, PUBLICATION_AUTHORS PA
        WHERE P.ID=PA.PUBLICATION_ID and PA.AUTHOR_ID=A.ID
              and P.PUBMED_ID= :pubmed_id
        ORDER BY PA.SORT ASC
    """


    all_publication_data = []

    try:
        ip, port, sid, username, password = gwas_data_sources.get_db_properties(DATABASE_NAME)
        dsn_tns = cx_Oracle.makedsn(ip, port, sid)
        connection = cx_Oracle.connect(username, password, dsn_tns)

        with contextlib.closing(connection.cursor()) as cursor:

            cursor.execute(publication_sql)

            publication_data = cursor.fetchall()
            # print "** PUBLICATION: ", publication_data


            for publication in tqdm(publication_data):
                publication = list(publication)

                # get author list
                cursor.prepare(publication_author_list_sql)                
                r = cursor.execute(None, {'pubmed_id': publication[1]})
                author_data = cursor.fetchall()

                # print "** PMID: ", publication[1]
                # print "** All-Authors: ", author_data


                # TODO: Generate author, author_s, authorAscii, authorList fields
                # AND Update JSON formatting to account for these new fields

                first_author = [author_data[0][0]]
                publication.append(first_author)

                author_s = author_data[0][0]
                publication.append(author_s)

                author_ascii = [author_data[0][1]]
                publication.append(author_ascii)

                author_ascii_s = author_data[0][1]
                publication.append(author_ascii_s)

                if author_data[0][3] is None:
                    author_orcid = 'NA'
                else:   
                    author_orcid = author_data[0][3] 
                
                authorList = []
                for author in author_data:
                    # create author list, e.g. "Grallert H | Grallert H | 5 | ORCID"
                    author_formatted = str(author[0])+" | "+str(author[1])+\
                        " | "+str(author[2])+" | "+author_orcid
                    authorList.append(author_formatted)
                
                # add authorList to publication data
                publication.append(authorList)
                # print "** Publication: ", publication, "\n"


                # add publication data to list of all publication data
                all_publication_data.append(publication)
        
        connection.close()
        return all_publication_data

    except cx_Oracle.DatabaseError, exception:
        print exception


def format_data(data, data_type):
    '''
    Convert list of data to JSON and write to file.
    '''
    data_dict = {}
    data_solr_doc = []
    
    
    for data_row in data:
        data_row = list(data_row)
        
        publication_attr_list = ['id', 'pmid', 'journal', 'title', \
            'publicationDate', 'resourcename', 'author', 'author_s', \
            'authorAscii', 'authorAscii_s', 'authorsList']
       
        if data_type == 'publication':

            data_dict = dict(zip(publication_attr_list, data_row))

            data_dict['id'] = data_row[5]+":"+str(data_row[0])

            data_solr_doc.append(data_dict)
            data_dict = {}

    jsonData = json.dumps(data_solr_doc)
    # print type(jsonData)

    my_path = os.path.abspath(os.path.dirname(__file__))
    path = os.path.join(my_path, "data/publication_data.json")

    with open(path, 'w') as outfile:
        outfile.write(jsonData)



def get_study_data():
    '''
    Get Study data for Solr document.
    '''

    # List of queries
    study_sql = """
        SELECT S.ID, S.ACCESSION_ID, S.TITLE, 'study' as resourcename
        FROM STUDY S
        WHERE ROWNUM <= 3
    """

    study_platform_types_sql = """
        SELECT DISTINCT S.ID, listagg(P.MANUFACTURER, ', ') WITHIN GROUP (ORDER BY P.MANUFACTURER) AS PLATFORM_TYPES
        FROM STUDY S, PLATFORM P, STUDY_PLATFORM SP
        WHERE S.ID=SP.STUDY_ID and SP.PLATFORM_ID=P.ID
            and S.ID= :study_id
        GROUP BY S.ID
    """

    study_ancestral_groups_sql = """
        SELECT DISTINCT S.ID , listagg(AG.ANCESTRAL_GROUP, ', ') WITHIN GROUP (ORDER BY AG.ANCESTRAL_GROUP) AS ANCESTRAL_GROUP
        FROM STUDY S, ANCESTRY A, ANCESTRY_ANCESTRAL_GROUP AAG, ANCESTRAL_GROUP AG
        WHERE S.ID=A.STUDY_ID and A.ID=AAG.ANCESTRY_ID and AAG.ANCESTRAL_GROUP_ID=AG.ID
            and S.ID= :study_id
        GROUP BY S.ID
    """


    study_reported_trait_sql = """
        SELECT S.ID, DT.TRAIT
        FROM STUDY S, STUDY_DISEASE_TRAIT SDT, DISEASE_TRAIT DT
        WHERE S.ID=SDT.STUDY_ID and SDT.DISEASE_TRAIT_ID=DT.ID
            and S.ID= :study_id
        """

    study_num_associations_sql = """
        SELECT S.ID, COUNT(ASSOC.ID)
        FROM STUDY S, ASSOCIATION ASSOC
        WHERE S.ID=ASSOC.STUDY_ID
            and S.ID= :study_id
        GROUP BY S.ID
        """


    all_study_data = []


    try:
        ip, port, sid, username, password = gwas_data_sources.get_db_properties(DATABASE_NAME)
        dsn_tns = cx_Oracle.makedsn(ip, port, sid)
        connection = cx_Oracle.connect(username, password, dsn_tns)

        # cursor = connection.cursor()
        with contextlib.closing(connection.cursor()) as cursor:

            cursor.execute(study_sql)

            study_data = cursor.fetchall()

            for study in tqdm(study_data):
                study = list(study)

                # get plaform list
                cursor.prepare(study_platform_types_sql)                
                r = cursor.execute(None, {'study_id': study[0]})
                platforms = cursor.fetchall()
                print "** Platforms: ", platforms, type(platforms)
                # Do null values occur for platform?

                # add platforms to study data
                study.append(platforms[0][1])
                

                # TODO: Make queries to other datatypes, e.g. ancestral_group, reported_trait, number of associations


                # add study data to list of all study data
                all_study_data.append(study)

        # cursor.close()
        connection.close()

        # print "** Num Rows: ", len(all_study_data), all_study_data[0]
        return all_study_data

    except cx_Oracle.DatabaseError, exception:
        print exception



def get_efo_data():
    '''
    '''

    # Get a list of all EFOs
    # For each EFO, find out how many studies and associations are mapped to the trait OR should the 
    # number of studies and associations per EFO be determined as part of the Study doc gneration?
    # Additional fields, like the EFO definition, synonyms, parent terms can be pulled from OLS
    ols_data = OLSData.OLSData("heart")
    ols_data.get_ols_results()


def get_disease_trait():
    '''
    Given each unique reported disease trait, get all mapped/EFO trait information.
    '''
    
    efo_sql = """

    """

    disease_sql = """
        SELECT DT.ID, DT.TRAIT, 'diseaseTrait' as resourcename
        FROM DISEASE_TRAIT DT
    """

    mapped_trait_sql = """
        SELECT ET.TRAIT, DT.TRAIT
        FROM STUDY S, EFO_TRAIT ET, DISEASE_TRAIT DT, STUDY_EFO_TRAIT SETR, STUDY_DISEASE_TRAIT SDT
        WHERE S.ID=SETR.STUDY_ID and SETR.EFO_TRAIT_ID=ET.ID 
            and S.ID=SDT.STUDY_ID and SDT.DISEASE_TRAIT_ID=DT.ID
            and DT.TRAIT = :disease_trait
    """

    all_disease_data = []

    try:
        ip, port, sid, username, password = gwas_data_sources.get_db_properties(DATABASE_NAME)
        dsn_tns = cx_Oracle.makedsn(ip, port, sid)
        connection = cx_Oracle.connect(username, password, dsn_tns)


        with contextlib.closing(connection.cursor()) as cursor:

            cursor.execute(disease_sql)

            disease_trait_data = cursor.fetchall()

            for reported_trait in tqdm(disease_trait_data):
                reported_trait = list(reported_trait)
                print "** Reported disease trait: ", reported_trait

                # get mapped/efo trait
                cursor.prepare(mapped_trait_sql)                
                r = cursor.execute(None, {'disease_trait': reported_trait[1]})
                all_mapped_traits = cursor.fetchall()
                # print "** Mapped trait(s): ", all_mapped_traits

                # add EFOs to disease trait data, this can return >1 result
                for mapped_trait in all_mapped_traits:
                    print "** MT: ", mapped_trait[0]
                    reported_trait.append(mapped_trait[0])
                
                print "\n"

                # TODO: Add data for EFOs from OLS


                # add study data to list of all study data
                all_disease_data.append(reported_trait)

        # cursor.close()
        connection.close()

        # print "** Num Rows: ", len(all_study_data), all_study_data[0]
        return all_disease_data

    except cx_Oracle.DatabaseError, exception:
        print exception




def test_format():
    with open('publication_data.json') as f:
        data = json.load(f)

    # pprint(data)
    print "** First JSON doc: ", data[1]
    for doc in data:
        print "** Item-5: ", doc['1'], doc['5']



if __name__ == '__main__':
    '''
    Create Solr documents for categories of interest.
    '''

    # Commandline arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', default='debug', choices=['debug', 'production'], 
                        help='Run as (default: debug).')
    parser.add_argument('--database', default='SPOTREL', choices=['DEV3', 'SPOTREL'], 
                        help='Run as (default: SPOTREL).')
    args = parser.parse_args()

    global DATABASE_NAME
    DATABASE_NAME = args.database 


    # Create Publication documents
    publication_data = get_publicaton_data()
    publication_data_type  = 'publication'

    format_data(publication_data, publication_data_type)


    # Create Study documents
    # study_data = get_study_data()
    # print "** SD: ", study_data


    # Create EFO documents
    # efo_data = get_efo_data()

    # Create Disease Trait documents
    # disease_trait_data = get_disease_trait()


    

    # test_format()


