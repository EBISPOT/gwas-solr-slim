import cx_Oracle
import contextlib
import argparse
import sys
from tqdm import tqdm
import json
import os.path


def get_study_data(connection, limit=0):
    '''
    Get Study data for Solr document.
    '''

    # List of queries
    study_sql = """
        SELECT S.ID, S.ACCESSION_ID, 'TODO-Title-Generation' as title, 'study' as resourcename,
        A.FULLNAME, TO_CHAR(P.PUBLICATION_DATE, 'yyyy'), P.PUBLICATION, P.PUBMED_ID, S.INITIAL_SAMPLE_SIZE,
        S.FULL_PVALUE_SET
        FROM STUDY S, PUBLICATION P, AUTHOR A
        WHERE S.PUBLICATION_ID=P.ID and P.FIRST_AUTHOR_ID=A.ID
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
        SELECT DISTINCT S.ID, listagg(DT.TRAIT, ', ') WITHIN GROUP (ORDER BY DT.TRAIT)
        FROM STUDY S, STUDY_DISEASE_TRAIT SDT, DISEASE_TRAIT DT
        WHERE S.ID=SDT.STUDY_ID and SDT.DISEASE_TRAIT_ID=DT.ID
              and S.ID= :study_id
        GROUP BY S.ID
    """


    study_mapped_trait_sql = """
        SELECT DISTINCT S.ID, listagg(ET.TRAIT, ', ') WITHIN GROUP (ORDER BY ET.TRAIT)
        FROM STUDY S, STUDY_EFO_TRAIT SETR, EFO_TRAIT ET
        WHERE S.ID=SETR.STUDY_ID and SETR.EFO_TRAIT_ID=ET.ID
              and S.ID= :study_id
        GROUP BY S.ID
    """


    study_associations_cnt_sql = """
        SELECT COUNT(ASSOC.ID)
        FROM STUDY S, ASSOCIATION ASSOC
        WHERE S.ID=ASSOC.STUDY_ID
            and S.ID= :study_id
        """


    all_study_data = []

    study_attr_list = ['id', 'accessionId', 'title', 'resourcename', \
        'platform', 'ancestralGroups', 'reportedTrait_s', 'reportedTrait', \
        'associationCount', 'description', 'fullPvalueSet']


    try:
        # ip, port, sid, username, password = gwas_data_sources.get_db_properties(DATABASE_NAME)
        # dsn_tns = cx_Oracle.makedsn(ip, port, sid)
        # connection = cx_Oracle.connect(username, password, dsn_tns)

        with contextlib.closing(connection.cursor()) as cursor:
            cursor.execute(study_sql)
            study_data = cursor.fetchall()


            for study in tqdm(study_data, desc='Get Study data'):
                # Data object for each Study
                study_document = {}

                # Add items to Study document
                study_document['id'] = study[3]+":"+str(study[0])
                study_document['accessionId'] = study[1]
                study_document['title'] = study[2]
                study_document['resourcename'] = study[3]
                study_document['fullPvalueSet'] = study[9]


                ######################
                # Get Platform list
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
                study_document['platform'] = platform


                #######################
                # Get Ancestral groups
                #######################
                study_ancestral_groups = []

                cursor.prepare(study_ancestral_groups_sql)
                r = cursor.execute(None, {'study_id': study[0]})
                ancestral_groups = cursor.fetchall()

                if not ancestral_groups:
                    study_ancestral_groups = 'NR'
                else:
                    study_ancestral_groups = [ancestral_groups[0][1]]

                # add ancestry information to study data
                study_document['ancestralGroups'] = study_ancestral_groups


                #######################
                # Get Reported Trait
                #######################
                study_reported_traits = []

                cursor.prepare(study_reported_trait_sql)
                r = cursor.execute(None, {'study_id': study[0]})
                reported_traits = cursor.fetchall()

                # add trait as string
                # study_document['reportedTrait_s'] = reported_traits[0][1]

                # add trait as list
                # study_document['reportedTrait'] = [reported_traits[0][1]]


                #######################
                # Get Mapped Trait
                #######################
                study_mapped_traits = []

                cursor.prepare(study_mapped_trait_sql)
                r = cursor.execute(None, {'study_id': study[0]})
                mapped_traits = cursor.fetchall()

                # add trait as string
                # study_document['mappedTrait'] = mapped_traits[0][1]


                ######################
                # Create Title field
                ######################
                # AccessionID + mapped trait + initial sample
                # study_document['title'] = study[1]+": "+mapped_traits[0][1]+", "+study[8]
                study_document['title'] = study[1]+", "+study[8]


                ###############################
                # Get Study-Association count
                ###############################
                cursor.prepare(study_associations_cnt_sql)
                r = cursor.execute(None, {'study_id': study[0]})
                association_cnt = cursor.fetchone()

                if not association_cnt:
                    association_cnt = 0
                    study_document['associationCount'] = association_cnt

                else:
                    study_document['associationCount'] = association_cnt[0]


                #############################
                # Create Description field
                #############################
                # The description field is formatted as:
                # First author, year, journal, pmid, # associations.
                # Update: Add Mapped and Reported traits to the description
                description = study[4]+" et al. "+study[5]+" "+study[6]+" "\
                +"PMID:"+study[7]+", associations: "+str(association_cnt[0])+" "\
                +"Mapped trait: "+mapped_traits[0][1]+" "\
                +"Reported trait: "+reported_traits[0][1]

                study_document['description'] = description


                #############################################
                # Add study data to list of all study data
                #############################################
                all_study_data.append(study_document)


        # connection.close()


        return all_study_data

    except cx_Oracle.DatabaseError, exception:
        print exception
