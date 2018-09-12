import cx_Oracle
import contextlib
import argparse
import sys
from tqdm import tqdm
import json
import os.path

# Custom modules
# import DBConnection
# import gwas_data_sources


def get_publication_data(connection, limit=0):
    '''
    Get Publication data for Solr document.
    '''

    # List of queries
    publication_sql = """
        SELECT P.ID, P.PUBMED_ID, P.PUBLICATION, P.TITLE,
            TO_CHAR(P.PUBLICATION_DATE, 'yyyy-mm-dd'),
            'publication' as resourcename
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

    publication_study_sql = """
        SELECT S.ID, S.ACCESSION_ID, S.FULL_PVALUE_SET 
        FROM STUDY S, PUBLICATION P 
        WHERE S.PUBLICATION_ID = P.ID 
            and P.PUBMED_ID= :pubmed_id
    """

    # Study related queries
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

    study_genotyping_technology_sql = """
        SELECT  x.ID, listagg(x.GENOTYPING_TECHNOLOGY, ', ') WITHIN GROUP (ORDER BY x.GENOTYPING_TECHNOLOGY) 
        FROM (
            SELECT DISTINCT S.ID, GT.GENOTYPING_TECHNOLOGY 
            FROM STUDY S, STUDY_GENOTYPING_TECHNOLOGY SGT, GENOTYPING_TECHNOLOGY GT 
            WHERE S.ID=SGT.STUDY_ID and SGT.GENOTYPING_TECHNOLOGY_ID=GT.ID 
                and S.ID= :study_id
            ) x 
        GROUP BY x.ID
    """

    country_of_recruitment_sql = """
        SELECT DISTINCT (C.COUNTRY_NAME)
        FROM STUDY S, ANCESTRY A, ANCESTRY_COUNTRY_RECRUITMENT ACR, COUNTRY C
        WHERE S.ID=A.STUDY_ID and A.ID=ACR.ANCESTRY_ID and ACR.COUNTRY_ID=C.ID
            and S.PUBLICATION_ID= :publication_id
    """


    all_publication_data = []

    publication_attr_list = [
        'id', 'pmid', 'journal', 'title',
        'publicationDate', 'resourcename', 'author', 'author_s',
        'authorAscii', 'authorAscii_s', 'authorsList',
        'associationCount', 'studyCount', 'description', 'countryOfRecruitment'
    ]

    try:
        # ip, port, sid, username, password = gwas_data_sources.get_db_properties(DATABASE_NAME)  # noqa
        # dsn_tns = cx_Oracle.makedsn(ip, port, sid)
        # connection = cx_Oracle.connect(username, password, dsn_tns)
        with contextlib.closing(connection.cursor()) as cursor:

            cursor.execute(publication_sql)
            publication_data = cursor.fetchall()

            for publication in tqdm(publication_data, desc='Get Publication data'):  # noqa
                publication = list(publication)

                publication_document = {}

                # Add data from gene to dictionary
                publication_document[publication_attr_list[0]] = publication[5]+":"+str(publication[0])  # noqa
                publication_document[publication_attr_list[1]] = publication[1]
                publication_document[publication_attr_list[2]] = publication[2]
                publication_document[publication_attr_list[3]] = publication[3]
                publication_document[publication_attr_list[4]] = publication[4]
                publication_document[publication_attr_list[5]] = publication[5]

                ############################
                # Get Author data
                ############################
                cursor.prepare(publication_author_list_sql)
                r = cursor.execute(None, {'pubmed_id': publication[1]})
                author_data = cursor.fetchall()

                # Create first author
                # first_author = [author_data[0][0]]
                publication_document[publication_attr_list[6]] = [author_data[0][0]]  # noqa

                # Create first author as string
                # author_s = author_data[0][0]
                publication_document[publication_attr_list[7]] = author_data[0][0]  # noqa

                # Create ascii author list
                # author_ascii = [author_data[0][1]]
                publication_document[publication_attr_list[8]] = [author_data[0][1]]  # noqa

                # Create ascii author string 
                # author_ascii_s = author_data[0][1]
                # publication.append(author_ascii_s)
                publication_document[publication_attr_list[9]] = author_data[0][1]  # noqa

                if author_data[0][3] is None:
                    author_orcid = 'NA'
                else:   
                    author_orcid = author_data[0][3] 
                
                authorList = []
                for author in author_data:
                    # author list, e.g. "Grallert H | Grallert H | 5 | ORCID"
                    author_formatted = str(author[0])+" | "+str(author[1])+\
                        " | "+str(author[2])+" | "+author_orcid
                    authorList.append(author_formatted)
                
                # add authorList to publication data
                publication_document[publication_attr_list[10]] = authorList


                ##########################
                # Get Association count 
                ##########################
                cursor.prepare(publication_association_cnt_sql)                
                r = cursor.execute(None, {'pubmed_id': publication[1]})
                association_cnt = cursor.fetchone()

                publication_document[publication_attr_list[11]] = association_cnt[0]


                #########################################
                # Get number of Studies per Publication
                #########################################
                cursor.prepare(publication_study_cnt_sql)                
                r = cursor.execute(None, {'pubmed_id': publication[1]})
                study_cnt = cursor.fetchone()

                publication_document[publication_attr_list[12]] = study_cnt[0]


                ##########################################
                # Get a list of countries of recruitment
                ##########################################
                cursor.prepare(country_of_recruitment_sql)
                r = cursor.execute(None, {'publication_id' : publication[0]})
                country_of_recruitment = cursor.fetchall()
                publication_document['countryOfRecruitment'] = [ x[0] for x in country_of_recruitment ]  # noqa


                #########################################
                # Get List of Studies per Publication
                #########################################
                cursor.prepare(publication_study_sql)
                r = cursor.execute(None, {'pubmed_id': publication[1]})
                studies = cursor.fetchall()

                # TEMP FIX - Add Study and FullPValue information to Publication document
                study_list = []
                full_pvalue = False
                for study in studies:
                    study_list.append(study[1])
                    publication_document['parentDocument_accessionId'] = study_list

                    # If any Study for the Publication includes Summary Stats, 
                    # mark the Publication as having Summary Stats for Search results. 
                    # Users will identify individual studies with summary stats on dedicated pages.
                    if study[2]:
                        full_pvalue = True
                    publication_document['fullPvalueSet'] = full_pvalue

                
                # TEMP FIX - Add Study information to Publication document
                all_genotyping_technologies = []
                all_ancestral_groups = []
                # For each study, get the Ancestral Groups
                # child_docs = []
                for study in studies:
                    # study_doc = {}
                    # study_doc['content_type'] = 'childDocument'
                    # study_doc['id'] = "study"+":"+str(study[0])
                    # study_doc['accessionId'] = study[1]


                    #############################
                    # Get Genotyping Technology 
                    #############################
                    # study_genotyping_technologies = []
                    cursor.prepare(study_genotyping_technology_sql)
                    r = cursor.execute(None, {'study_id': study[0]})
                    genotyping_technologies = cursor.fetchall()

                    if not genotyping_technologies:
                        gt_technologies = 'NA'
                    else:
                        gt_technologies = genotyping_technologies[0][1]
                    
                    # Add only distinct values to the all_genotyping_technologies list
                    if gt_technologies not in all_genotyping_technologies:
                        all_genotyping_technologies.append(gt_technologies)
                    

                    #######################
                    # Get Ancestral groups
                    #######################
                    study_ancestral_groups = []

                    cursor.prepare(study_ancestral_groups_sql)
                    r = cursor.execute(None, {'study_id': study[0]})
                    ancestral_groups = cursor.fetchall()

                    if not ancestral_groups:
                        study_ancestral_groups = 'NR'
                        all_ancestral_groups.append('NR')
                    else:
                        study_ancestral_groups = [ancestral_groups[0][1]]
                        all_ancestral_groups.append(ancestral_groups[0][1])

                # Finally, add child_docs to publication document
                # publication_document['_childDocuments_'] = child_docs
                # publication_document['content_type'] = 'parentDocument'
                publication_document['parentDocument_ancestralGroups'] = all_ancestral_groups
                publication_document['genotypingTechnologies'] = all_genotyping_technologies



                #############################
                # Create description field
                #############################
                # The description field is formatted as:
                # First author, year, journal, pmid.
                year, month, day = publication[4].split("-")
                
                description = author_data[0][0]+" et al. "+year+" "+publication[2]+" "\
                +"PMID:"+publication[1]

                publication_document['description'] = description


                ######################################
                # Add publication data document to
                # list of all publication data docs
                ######################################
                # all_publication_data.append(publication)
                all_publication_data.append(publication_document)
        

        # connection.close()

        return all_publication_data

    except cx_Oracle.DatabaseError, exception:
        print exception

