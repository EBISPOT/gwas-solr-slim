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
import DBConnection


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


    publication_association_cnt_sql = """
        SELECT COUNT(A.ID)
        FROM STUDY S, PUBLICATION P, ASSOCIATION A
        WHERE S.PUBLICATION_ID=P.ID and A.STUDY_ID=S.ID
            and P.PUBMED_ID= :pubmed_id
    """


    publication_study_cnt_sql = """
        SELECT COUNT(S.ID)
        FROM STUDY S, PUBLICATION P
        WHERE S.PUBLICATION_ID=P.ID
            and P.PUBMED_ID= :pubmed_id
    """


    all_publication_data = []

    try:
        ip, port, sid, username, password = gwas_data_sources.get_db_properties(DATABASE_NAME)
        dsn_tns = cx_Oracle.makedsn(ip, port, sid)
        connection = cx_Oracle.connect(username, password, dsn_tns)

        with contextlib.closing(connection.cursor()) as cursor:

            cursor.execute(publication_sql)

            publication_data = cursor.fetchall()


            for publication in tqdm(publication_data, desc='Get Publication data'):
                publication = list(publication)

                ############################
                # Get Author data
                ############################
                cursor.prepare(publication_author_list_sql)                
                r = cursor.execute(None, {'pubmed_id': publication[1]})
                author_data = cursor.fetchall()

                # create author field data
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


                ##########################
                # Get Association count 
                ##########################
                cursor.prepare(publication_association_cnt_sql)                
                r = cursor.execute(None, {'pubmed_id': publication[1]})
                association_cnt = cursor.fetchone()

                publication.append(association_cnt)


                #########################################
                # Get number of Studies per Publication
                #########################################
                cursor.prepare(publication_study_cnt_sql)                
                r = cursor.execute(None, {'pubmed_id': publication[1]})
                study_cnt = cursor.fetchone()

                publication.append(study_cnt)


                ######################################
                # Add publication data document to
                # list of all publication data docs
                ######################################
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

    publication_attr_list = ['id', 'pmid', 'journal', 'title', \
        'publicationDate', 'resourcename', 'author', 'author_s', \
        'authorAscii', 'authorAscii_s', 'authorsList', \
        'associationCount', 'studyCount']


    study_attr_list = ['id', 'accessionId', 'title', 'resourcename', \
        'platform', 'ancestralGroups', 'traitName_s', 'traitName', \
        'associationCount']

    trait_attr_list = ['id', 'mappedLabel', 'mappedUri', 'studyCount', \
        'resourcename', 'traitName_s', 'traitName', 'associationCount', \
        'shortForm', 'synonyms', 'parent']

    variant_attr_list = ['id', 'rsId', 'snpType', 'resourcename', \
        'chromosomeName', 'chromosomePosition', 'region']


    for data_row in tqdm(data, desc='Format data'):
        data_row = list(data_row)

        # Create Publication documents
        if data_type in ['publication', 'all']:

            data_dict = dict(zip(publication_attr_list, data_row))

            data_dict['id'] = data_row[5]+":"+str(data_row[0])

            data_solr_doc.append(data_dict)
            data_dict = {}

            jsonData = json.dumps(data_solr_doc)

            my_path = os.path.abspath(os.path.dirname(__file__))
            path = os.path.join(my_path, "data/publication_data.json")

            with open(path, 'w') as outfile:
                outfile.write(jsonData)


        # Create Study documents
        if data_type in ['study', 'all']:
            data_dict = dict(zip(study_attr_list, data_row))

            data_dict['id'] = data_row[3]+":"+str(data_row[0])

            data_solr_doc.append(data_dict)
            data_dict = {}

            jsonData = json.dumps(data_solr_doc)

            my_path = os.path.abspath(os.path.dirname(__file__))
            path = os.path.join(my_path, "data/study_data.json")

            with open(path, 'w') as outfile:
                outfile.write(jsonData)


        # Create Trait documents
        if data_type in ['trait', 'all']:
            data_dict = dict(zip(trait_attr_list, data_row))

            data_dict['id'] = data_row[4]+":"+str(data_row[0])

            data_solr_doc.append(data_dict)
            data_dict = {}

            jsonData = json.dumps(data_solr_doc)

            my_path = os.path.abspath(os.path.dirname(__file__))
            path = os.path.join(my_path, "data/trait_data.json")

            with open(path, 'w') as outfile:
                outfile.write(jsonData)


        # Create association documents
        if data_type in ['variant', 'all']:
            data_dict = dict(zip(variant_attr_list, data_row))

            data_dict['id'] = data_row[3]+":"+str(data_row[0])

            data_solr_doc.append(data_dict)
            data_dict = {}

            jsonData = json.dumps(data_solr_doc)

            my_path = os.path.abspath(os.path.dirname(__file__))
            path = os.path.join(my_path, "data/variant_data.json")

            with open(path, 'w') as outfile:
                outfile.write(jsonData)




def get_study_data():
    '''
    Get Study data for Solr document.
    '''

    # List of queries
    study_sql = """
        SELECT S.ID, S.ACCESSION_ID, 'TODO-Title-Generation' as title, 'study' as resourcename
        FROM STUDY S
    """

    study_platform_types_sql = """
        SELECT DISTINCT S.ID, listagg(P.MANUFACTURER, ', ') WITHIN GROUP (ORDER BY P.MANUFACTURER) AS PLATFORM_TYPES
        FROM STUDY S, PLATFORM P, STUDY_PLATFORM SP
        WHERE S.ID=SP.STUDY_ID and SP.PLATFORM_ID=P.ID
            and S.ID= :study_id
        GROUP BY S.ID
    """

    study_ancestral_groups_sql = """
        SELECT  x.ID, listagg(x.ANCESTRAL_GROUP, ', ') WITHIN GROUP (ORDER BY x.ANCESTRAL_GROUP)
        FROM (
                SELECT DISTINCT S.ID, AG.ANCESTRAL_GROUP
                FROM STUDY S, ANCESTRY A, ANCESTRY_ANCESTRAL_GROUP AAG, ANCESTRAL_GROUP AG
                WHERE S.ID=A.STUDY_ID and A.ID=AAG.ANCESTRY_ID and AAG.ANCESTRAL_GROUP_ID=AG.ID
                    and S.ID= :study_id
            ) x
        GROUP BY x.ID
    """


    study_reported_trait_sql = """
        SELECT DISTINCT S.ID, listagg(DT.TRAIT, ', ') WITHIN GROUP (ORDER BY DT.TRAIT) AS TRAITS
        FROM STUDY S, STUDY_DISEASE_TRAIT SDT, DISEASE_TRAIT DT
        WHERE S.ID=SDT.STUDY_ID and SDT.DISEASE_TRAIT_ID=DT.ID
              and S.ID= :study_id
        GROUP BY S.ID
    """


    study_associations_cnt_sql = """
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

            studies_missing_ancestral_groups = []
            studies_missing_associations = []

            for study in tqdm(study_data, desc='Get Study data'):
                study = list(study)


                ######################
                # Get platform list
                ######################
                cursor.prepare(study_platform_types_sql)
                r = cursor.execute(None, {'study_id': study[0]})
                platforms = cursor.fetchall()


                # Handle cases where there are no values for Platform
                if not platforms:
                    platform = 'NR'
                else:
                    platform = platforms[0][1]

                # add platforms (as string) to study data
                study.append(platform)
                

                #######################
                # Get Ancestral groups
                #######################
                study_ancestral_groups = []

                cursor.prepare(study_ancestral_groups_sql)
                r = cursor.execute(None, {'study_id': study[0]})
                ancestral_groups = cursor.fetchall()

                if not ancestral_groups:
                    study_ancestral_groups = 'NR'
                    studies_missing_ancestral_groups.append(study[1])
                else:
                    study_ancestral_groups = [ancestral_groups[0][1]]

                # add ancestry information to study data
                study.append(study_ancestral_groups)


                #######################
                # Get Reported Trait
                #######################
                study_reported_traits = []

                cursor.prepare(study_reported_trait_sql)
                r = cursor.execute(None, {'study_id': study[0]})
                reported_traits = cursor.fetchall()

                # add trait as string
                study.append(reported_traits[0][1])

                # add trait as list
                study.append([reported_traits[0][1]])


                ###############################
                # Get Study-Association count
                ###############################
                cursor.prepare(study_associations_cnt_sql)
                r = cursor.execute(None, {'study_id': study[0]})
                association_cnt = cursor.fetchall()

                if not association_cnt:
                    association_cnt = 0
                    study.append(association_cnt)

                    if study[1] not in studies_missing_associations:
                        studies_missing_associations.append(study[1])
                else:
                    study.append(association_cnt[0][1])



                #############################################
                # Add study data to list of all study data
                #############################################
                all_study_data.append(study)


        connection.close()

        # QA Checks of Association and Ancestral Group information
        # print "** All Studies missing Ancestral groups: ", \
        #     len(studies_missing_ancestral_groups), studies_missing_ancestral_groups

        # print "** All Studies missing Associations: ", \
        #     len(studies_missing_associations), studies_missing_associations


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
    Given each EFO trait, get all Reported trait information.
    '''
    
    # Only select EFOs that are already assigned to Studies
    efo_sql = """
        SELECT DISTINCT(ET.ID), ET.TRAIT, ET.URI, COUNT(S.ID), 'trait' as resourcename
        FROM STUDY S, EFO_TRAIT ET, STUDY_EFO_TRAIT SETR
        WHERE S.ID=SETR.STUDY_ID and SETR.EFO_TRAIT_ID=ET.ID
        GROUP BY ET.ID, ET.TRAIT, 'trait', ET.URI
    """


    reported_trait_sql = """
        SELECT DISTINCT(ET.ID), listagg(DT.TRAIT, ', ') WITHIN GROUP (ORDER BY DT.TRAIT)
        FROM STUDY S, EFO_TRAIT ET, DISEASE_TRAIT DT, STUDY_EFO_TRAIT SETR, STUDY_DISEASE_TRAIT SDT
        WHERE S.ID=SETR.STUDY_ID and SETR.EFO_TRAIT_ID=ET.ID 
            and S.ID=SDT.STUDY_ID and SDT.DISEASE_TRAIT_ID=DT.ID 
            and ET.ID = :trait_id
        GROUP BY ET.ID
    """

    trait_association_cnt_sql = """
        SELECT COUNT(ET.ID)
        FROM EFO_TRAIT ET, ASSOCIATION_EFO_TRAIT AET, ASSOCIATION A
        WHERE ET.ID=AET.EFO_TRAIT_ID and AET.ASSOCIATION_ID=A.ID
            and ET.ID = :trait_id
        GROUP BY ET.TRAIT
    """


    all_trait_data = []

    try:
        ip, port, sid, username, password = gwas_data_sources.get_db_properties(DATABASE_NAME)
        dsn_tns = cx_Oracle.makedsn(ip, port, sid)
        connection = cx_Oracle.connect(username, password, dsn_tns)


        with contextlib.closing(connection.cursor()) as cursor:

            cursor.execute(efo_sql)

            mapped_trait_data = cursor.fetchall()

            for mapped_trait in tqdm(mapped_trait_data, desc='Get EFO/Mapped trait data'):
                mapped_trait = list(mapped_trait)
                # print "** Mapped EFO trait: ", mapped_trait

                #########################
                # Get reported trait(s)
                #########################
                cursor.prepare(reported_trait_sql)
                r = cursor.execute(None, {'trait_id': mapped_trait[0]})
                all_reported_traits = cursor.fetchall()
                # print "** Reported trait(s): ", all_reported_traits, "\n", [all_reported_traits[0][1]]

                # add reported trait as string
                mapped_trait.append(all_reported_traits[0][1])

                # add reported trait as list
                mapped_trait.append([all_reported_traits[0][1]])

                
                #######################################
                # Get count of associations per trait
                #######################################
                cursor.prepare(trait_association_cnt_sql)
                r = cursor.execute(None, {'trait_id': mapped_trait[0]})
                trait_assoc_cnt = cursor.fetchone()
                # print "** Assoc CNT: ", trait_assoc_cnt

                if not trait_assoc_cnt:
                    trait_assoc_cnt = 0
                    mapped_trait.append(trait_assoc_cnt)
                else:
                    mapped_trait.append(trait_assoc_cnt[0])


                #####################################
                # Get EFO term information from OLS
                #####################################
                ols_data = OLSData.OLSData(mapped_trait[2])
                ols_term_data = ols_data.get_ols_term()


                if not ols_term_data['iri'] == None:
                    mapped_uri = [ols_term_data['iri'].encode('utf-8')]
                    # mapped_trait.append(mapped_uri) - this also comes from the db

                    # use this or from db, note is entered manually into db?
                    short_form = [ols_term_data['short_form'].encode('utf-8')]
                    mapped_trait.append(short_form)

                    # use this or from db?
                    label = [ols_term_data['label'].encode('utf-8')]
                    # mapped_trait.append(label)


                    if not ols_term_data['synonyms'] == None:
                        synonyms = [synonym.encode('utf-8') for synonym in ols_term_data['synonyms']]
                        mapped_trait.append(synonyms)
                    else: 
                        synonyms = []
                        mapped_trait.append(synonyms)


                    if not ols_term_data['ancestors'] == None:
                        ancestor_data = OLSData.OLSData(ols_term_data['ancestors'])
                        ancestor_terms = [ancestor.encode('utf-8') for ancestor in ancestor_data.get_ancestors()]
                        mapped_trait.append(ancestor_terms)

                else:
                    # add placeholder data
                    for item in range(4):
                        mapped_trait.append(None)



                ############################################
                # Add trait data to list of all trait data
                ############################################
                all_trait_data.append(mapped_trait)


        connection.close()

        return all_trait_data

    except cx_Oracle.DatabaseError, exception:
        print exception



def get_variant_data():
    '''
    Get Variant data for Solr document.
    '''

    # List of queries
    snp_sql = """
        SELECT SNP.ID, SNP.RS_ID, SNP.FUNCTIONAL_CLASS, 'variant' as resourcename
        FROM SINGLE_NUCLEOTIDE_POLYMORPHISM SNP
    """

    snp_location_sql = """
        SELECT L.CHROMOSOME_NAME, L.CHROMOSOME_POSITION, R.NAME
        FROM LOCATION L, SNP_LOCATION SL, SINGLE_NUCLEOTIDE_POLYMORPHISM SNP, REGION R
        WHERE L.ID=SL.LOCATION_ID and SL.SNP_ID=SNP.ID and L.REGION_ID=R.ID 
            and SNP.ID= :snp_id
    """

    # TODO: Abstract out Database connection information
    # DBConnection.DBConnection(DATABASE_NAME)

    all_variant_data = []

    try:
        ip, port, sid, username, password = gwas_data_sources.get_db_properties(DATABASE_NAME)
        dsn_tns = cx_Oracle.makedsn(ip, port, sid)
        connection = cx_Oracle.connect(username, password, dsn_tns)


        with contextlib.closing(connection.cursor()) as cursor:

            cursor.execute(snp_sql)

            variant_data = cursor.fetchall()

            for variant in tqdm(variant_data, desc='Get Variant data'):
                variant = list(variant)
                # print "** SNP: ", variant

                ############################
                # Get Location information
                ############################
                cursor.prepare(snp_location_sql)
                r = cursor.execute(None, {'snp_id': variant[0]})
                all_snp_locations = cursor.fetchall()
                # print "** Assoc Location: ", all_snp_locations

                # add snp location to variant_data data, 
                # chromosome, position, region
                if not all_snp_locations:
                    # add placeholder values
                    # NOTE: Current Solr data does not include field when value is null
                    for item in range(3):
                        variant.append(None)
                else:
                    variant.append(all_snp_locations[0][0])
                    variant.append(all_snp_locations[0][1])
                    variant.append(all_snp_locations[0][2])


                all_variant_data.append(variant)


        # print "** All Variant: ", all_variant_data

        connection.close()

        return all_variant_data

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
    # parser.add_argument('--mode', default='debug', choices=['debug', 'production'],
    #                     help='Run as (default: debug).')
    parser.add_argument('--database', default='SPOTREL', choices=['DEV3', 'SPOTREL'], 
                        help='Run as (default: SPOTREL).')
    parser.add_argument('--data_type', default='publication', \
                        choices=['publication', 'study', 'trait', 'variant', 'all'],
                        help='Run as (default: publication).')
    args = parser.parse_args()

    global DATABASE_NAME
    DATABASE_NAME = args.database


    # Create Publication documents
    if args.data_type in ['publication', 'all']:
        publication_datatype  = 'publication'
        publication_data = get_publicaton_data()
        format_data(publication_data, publication_datatype)


    # Create Study documents
    if args.data_type in ['study', 'all']:
        study_data_type  = 'study'
        study_data = get_study_data()
        format_data(study_data, study_data_type)


    # Create EFO documents, are these needed for labs pages?
    # efo_data = get_efo_data()


    # Create Disease Trait documents
    if args.data_type in ['trait', 'all']:
        trait_data_type  = 'trait'
        trait_data = get_disease_trait()
        format_data(trait_data, trait_data_type)


    # Create Variant documents
    if args.data_type in ['variant', 'all']:
        variant_data_type  = 'variant'
        variant_data = get_variant_data()
        format_data(variant_data, variant_data_type)


    

    # test_format()


