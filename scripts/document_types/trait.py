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

    # trait_association_cnt_sql = """
    #     SELECT COUNT(A.ID)
    #     FROM EFO_TRAIT ET, ASSOCIATION_EFO_TRAIT AET, ASSOCIATION A
    #     WHERE ET.ID=AET.EFO_TRAIT_ID AND AET.ASSOCIATION_ID=A.ID
    #           AND ET.ID= :trait_id
    # """

    all_trait_data = []

    try:
        with contextlib.closing(connection.cursor()) as cursor:
            cursor.execute(efo_sql)
            mapped_trait_data = cursor.fetchall()

            # Build Lookup table of EFO_ID to Study count
            efo_study_count_map = __build_efo_studyCnt_map(mapped_trait_data)

            # Build Lookup table of EFO_ID to Association count
            efo_association_count_map = __build_efo_associationCnt_map(mapped_trait_data, cursor)

            # Build Lookup table of EFO_ID to list of children
            __get_descendants(mapped_trait_data)


            for mapped_trait in tqdm(mapped_trait_data, desc='Get EFO/Mapped trait data'):
                # Data object for each mapped trait
                mapped_trait_document = {}

                mapped_trait_document['id'] = mapped_trait[4]+":"+str(mapped_trait[0])
                mapped_trait_document['mappedTrait'] = mapped_trait[1]
                mapped_trait_document['mappedUri'] = mapped_trait[2]
                
                # Study Count needs to be for current EFO and All of it's children
                # mapped_trait_document['studyCount'] = mapped_trait[3]
                mapped_trait_document['studyCount'] = efo_study_count_map[mapped_trait[5]]


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
                # cursor.prepare(trait_association_cnt_sql)
                # r = cursor.execute(None, {'trait_id': mapped_trait[0]})
                # trait_assoc_cnt = cursor.fetchone()

                # if not trait_assoc_cnt:
                #     trait_assoc_cnt = 0
                #     mapped_trait_document['associationCount'] = trait_assoc_cnt
                # else:
                #     mapped_trait_document['associationCount'] = trait_assoc_cnt[0]
                
                mapped_trait_document['associationCount'] = efo_association_count_map[mapped_trait[5]]


                #####################################
                # Get EFO term information from OLS
                #####################################
                ols_data = OLSData.OLSData(mapped_trait[2])
                type = 'ancestors'
                ols_term_data = ols_data.get_ols_term(type)


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
                        # handle terms that do not have a term definition
                        term_description = "NA"

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
                    # yet in published EFO submitted to OLS
                    
                    # add description since this is a required field for the Solr documents
                    term_description = "NA"

                    mapped_trait_document['description'] = term_description+", associations: "+\
                    str(mapped_trait_document['associationCount'])+", studies: "+str(mapped_trait_document['studyCount'])
                    


                ############################################
                # Add trait data to list of all trait data
                ############################################
                all_trait_data.append(mapped_trait_document)

        return all_trait_data


    except cx_Oracle.DatabaseError, exception:
        print exception


def __build_efo_studyCnt_map(efo_data):
    ''' 
    Given a list of data from the "efo_sql" query, build lookup table
    keyed on EFO Id with the value as the Study count.
    '''
    efo_study = {}

    for row in efo_data:
        efo_id = row[5]
        study_count = row[3]

        efo_study[efo_id] = study_count

    return efo_study


def __build_efo_associationCnt_map(efo_data, cursor):
    '''
    Given a list of data from the "efo_sql" query, build a lookup table
    keyed on the EFO Id with the value as Association count.
    '''
    efo_association = {}

    trait_association_cnt_sql = """
        SELECT COUNT(A.ID)
        FROM EFO_TRAIT ET, ASSOCIATION_EFO_TRAIT AET, ASSOCIATION A
        WHERE ET.ID=AET.EFO_TRAIT_ID AND AET.ASSOCIATION_ID=A.ID
              AND ET.SHORT_FORM= :trait_id
    """

    # Get count of associations per trait
    for row in efo_data:
        efo_id = row[5]

        cursor.prepare(trait_association_cnt_sql)
        r = cursor.execute(None, {'trait_id': efo_id})
        trait_assoc_cnt = cursor.fetchone()

        if not trait_assoc_cnt:
            trait_assoc_cnt = 0
            efo_association[efo_id] = trait_assoc_cnt
        else:
            efo_association[efo_id] = trait_assoc_cnt[0]

    return efo_association


def __get_descendants(efo_data):
    '''
    For each EFO Id, get a list of all descendants and 
    store in a lookup table keyed on the EFO Id with the 
    value as the list of descendant EFO Ids (short_form).
    '''

    type = 'descendants'
    
    # TEST - Get descendants
    for row in efo_data:
        print "** EFO ID: ", row[2]
        ols_data = OLSData.OLSData(row[2])
        ols_term_data = ols_data.get_ols_term(type)
        # print "** TD: ", ols_term_data

        if not ols_term_data['iri'] == None:
            # if not ols_term_data['descendants'] == None:
            if 'descendants' in ols_term_data.keys():
                        descendant_data = OLSData.OLSData(ols_term_data['descendants'])
                        descendant_terms = [descendant.encode('utf-8') for descendant in descendant_data.get_descendants()]
                        # mapped_trait_document['parent'] = ancestor_terms
                        print "** Descendants: ", descendant_terms
            else:
                print "** Term has no descendants!\n"













