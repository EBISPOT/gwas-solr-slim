import cx_Oracle
import contextlib
import argparse
import sys
from tqdm import tqdm
import json
import os.path

from ols import OLSData


def get_trait_data(connection, limit=0):
    '''
    Given each EFO trait, get all Reported trait information.
    '''
    
    # Only select EFOs that are already assigned to Studies
    efo_sql = """
        SELECT DISTINCT(ET.ID), ET.TRAIT, ET.URI, COUNT(S.ID), 
            'trait' as resourcename, ET.SHORT_FORM
        FROM STUDY S, EFO_TRAIT ET, STUDY_EFO_TRAIT SETR
        WHERE S.ID=SETR.STUDY_ID and SETR.EFO_TRAIT_ID=ET.ID
        GROUP BY ET.ID, ET.TRAIT, 'trait', ET.URI, ET.SHORT_FORM
    """

    reported_trait_sql = """
        SELECT listagg(REPORTED_DISEASE_TRAIT, ', ') WITHIN GROUP (ORDER BY REPORTED_DISEASE_TRAIT)
        FROM ( 
            SELECT DISTINCT DT.TRAIT AS REPORTED_DISEASE_TRAIT 
            FROM STUDY S, EFO_TRAIT ET, DISEASE_TRAIT DT, STUDY_EFO_TRAIT SETR, STUDY_DISEASE_TRAIT SDT 
            WHERE S.ID=SETR.STUDY_ID and SETR.EFO_TRAIT_ID=ET.ID 
            and S.ID=SDT.STUDY_ID and SDT.DISEASE_TRAIT_ID=DT.ID 
            and ET.ID= :trait_id)
    """

    trait_association_cnt_sql = """
        SELECT COUNT(A.ID)
        FROM EFO_TRAIT ET, ASSOCIATION_EFO_TRAIT AET, ASSOCIATION A
        WHERE ET.ID=AET.EFO_TRAIT_ID AND AET.ASSOCIATION_ID=A.ID
              AND ET.ID= :trait_id
    """


    all_trait_data = []

    trait_attr_list = ['id', 'mappedTrait', 'mappedUri', 'studyCount', \
        'resourcename', 'reportedTrait_s', 'reportedTrait', 'associationCount', \
        'shortForm', 'synonyms', 'parent']

        # 'shortform_autosuggest', 'label_autosuggest', 'label_autosuggest_ws', \
        # 'label_autosuggest_e', 'synonym_autosuggest', 'synonym_autosuggest_ws', \
        # 'synonym_autosuggest_e'

    try:
        # ip, port, sid, username, password = gwas_data_sources.get_db_properties(DATABASE_NAME)
        # dsn_tns = cx_Oracle.makedsn(ip, port, sid)
        # connection = cx_Oracle.connect(username, password, dsn_tns)


        with contextlib.closing(connection.cursor()) as cursor:
            cursor.execute(efo_sql)
            mapped_trait_data = cursor.fetchall()

            for mapped_trait in tqdm(mapped_trait_data, desc='Get EFO/Mapped trait data'):
                # Data object for each mapped trait
                mapped_trait_document = {}

                mapped_trait_document['id'] = mapped_trait[4]+":"+str(mapped_trait[0])
                mapped_trait_document['mappedTrait'] = mapped_trait[1]
                mapped_trait_document['mappedUri'] = mapped_trait[2]
                mapped_trait_document['studyCount'] = mapped_trait[3]
                mapped_trait_document['resourcename'] = mapped_trait[4]
                mapped_trait_document['efoLink'] = [mapped_trait[1]+"|"+\
                    mapped_trait[5]+"|"+mapped_trait[2]]

                # Add Autosuggest fields
                mapped_trait_document['shortform_autosuggest'] = [mapped_trait[5]]
                mapped_trait_document['label_autosuggest'] = [mapped_trait[1]]
                mapped_trait_document['label_autosuggest_ws'] = [mapped_trait[1]]
                mapped_trait_document['label_autosuggest_e'] = [mapped_trait[1]]


                #########################
                # Get reported trait(s)
                #########################
                cursor.prepare(reported_trait_sql)
                r = cursor.execute(None, {'trait_id': mapped_trait[0]})
                all_reported_traits = cursor.fetchall()

                # add reported trait as string
                mapped_trait_document['reportedTrait_s'] = all_reported_traits[0][0]

                # add reported trait as list
                mapped_trait_document['reportedTrait'] = [all_reported_traits[0][0]]

                
                #######################################
                # Get count of associations per trait
                #######################################
                cursor.prepare(trait_association_cnt_sql)
                r = cursor.execute(None, {'trait_id': mapped_trait[0]})
                trait_assoc_cnt = cursor.fetchone()

                if not trait_assoc_cnt:
                    trait_assoc_cnt = 0
                    mapped_trait_document['associationCount'] = trait_assoc_cnt
                else:
                    mapped_trait_document['associationCount'] = trait_assoc_cnt[0]


                #####################################
                # Get EFO term information from OLS
                #####################################
                ols_data = OLSData.OLSData(mapped_trait[2])
                ols_term_data = ols_data.get_ols_term()


                if not ols_term_data['iri'] == None:
                    mapped_uri = [ols_term_data['iri'].encode('utf-8')]

                    # use this or from db? note is entered manually into db
                    short_form = [ols_term_data['short_form'].encode('utf-8')]
                    mapped_trait_document['shortForm'] = short_form

                    # use this or from db? note is entered manually into db
                    label = [ols_term_data['label'].encode('utf-8')]

                    # Create title field, use this or from db? note is entered manually into db
                    mapped_trait_document['title'] = ols_term_data['label']+" ("+ols_term_data['short_form']+")"

                    #######################
                    # Create description
                    ######################
                    # Trait description, number of studies, number of associations.
                    term_description = ""
                    if not ols_term_data['description'] == None:
                        term_description = ''.join(ols_term_data['description'])
                    else:
                        term_description = "NA"

                    mapped_trait_document['description'] = term_description+", associations: "+\
                    str(mapped_trait_document['associationCount'])+", studies: "+str(mapped_trait_document['studyCount'])


                    # Add synonyms
                    if not ols_term_data['synonyms'] == None:
                        synonyms = [synonym.encode('utf-8') for synonym in ols_term_data['synonyms']]
                        mapped_trait_document['synonyms'] = synonyms
                        # Add autosuggest fields
                        mapped_trait_document['synonym_autosuggest'] = synonyms
                        mapped_trait_document['synonym_autosuggest_ws'] = synonyms
                        mapped_trait_document['synonym_autosuggest_e'] = synonyms
                    else: 
                        synonyms = []
                        mapped_trait_document['synonyms'] = synonyms

                    # Add ancestors
                    if not ols_term_data['ancestors'] == None:
                        ancestor_data = OLSData.OLSData(ols_term_data['ancestors'])
                        ancestor_terms = [ancestor.encode('utf-8') for ancestor in ancestor_data.get_ancestors()]
                        mapped_trait_document['parent'] = ancestor_terms

                else:
                    # # add placeholder data
                    # for item in range(4):
                    #     mapped_trait.append(None)
                    pass


                ############################################
                # Add trait data to list of all trait data
                ############################################
                all_trait_data.append(mapped_trait_document)


        # connection.close()

        return all_trait_data

    except cx_Oracle.DatabaseError, exception:
        print exception