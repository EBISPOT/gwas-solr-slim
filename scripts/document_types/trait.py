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
    Given each Mapped EFO trait, get all Reported trait information.
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
        SELECT DISTINCT DT.TRAIT AS REPORTED_DISEASE_TRAIT 
        FROM STUDY S, EFO_TRAIT ET, DISEASE_TRAIT DT, STUDY_EFO_TRAIT SETR, STUDY_DISEASE_TRAIT SDT 
        WHERE S.ID=SETR.STUDY_ID and SETR.EFO_TRAIT_ID=ET.ID 
        and S.ID=SDT.STUDY_ID and SDT.DISEASE_TRAIT_ID=DT.ID 
        and ET.ID= :trait_id
    """

    trait_association_cnt_sql = """
        SELECT COUNT(A.ID)
        FROM EFO_TRAIT ET, ASSOCIATION_EFO_TRAIT AET, ASSOCIATION A
        WHERE ET.ID=AET.EFO_TRAIT_ID AND AET.ASSOCIATION_ID=A.ID
              AND ET.ID= :trait_id
    """

    all_trait_data = []

    try:
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

                mapped_trait_document['shortForm'] = [mapped_trait[5]]
                mapped_trait_document['title'] = mapped_trait[1]

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

                # add reported trait as list
                reported_trait_list = []
                for trait in all_reported_traits:
                    reported_trait_list.append(trait[0])
                mapped_trait_document['reportedTrait'] = reported_trait_list

                # add reported trait as a string
                reported_trait_s = ', '.join(reported_trait_list)
                mapped_trait_document['reportedTrait_s'] = reported_trait_s

                
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
                    # Not used in Solr document yet
                    # mapped_uri = [ols_term_data['iri'].encode('utf-8')]

                    # Not all EFO terms will be in OLS when the Solr data 
                    # is generated so use the shortForm from the database


                    #######################
                    # Create description
                    ######################
                    term_description = ""
                    if not ols_term_data['description'] == None:
                        term_description = ''.join(ols_term_data['description'])
                    else:
                        term_description = "NA"

                    ## GOCI2359 - Removing association and study count from the description.
                    # mapped_trait_document['description'] = term_description+", associations: "+\
                    # str(mapped_trait_document['associationCount'])+", studies: "+str(mapped_trait_document['studyCount'])
                    mapped_trait_document['description'] = term_description

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
                    # TODO: Handle cases when term is not
                    # in EFO submitted to OLS
                   pass


                ############################################
                # Add trait data to list of all trait data
                ############################################
                all_trait_data.append(mapped_trait_document)

        return all_trait_data


    except cx_Oracle.DatabaseError, exception:
        print exception
