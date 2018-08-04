import cx_Oracle
import contextlib
import argparse
import sys
from tqdm import tqdm
import json
import os.path
import datetime
import pandas

# Custom modules
import DBConnection
import gwas_data_sources
import OLSData
import Variant


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
        SELECT S.ID, S.ACCESSION_ID 
        FROM STUDY S, PUBLICATION P 
        WHERE S.PUBLICATION_ID = P.ID 
            and P.PUBMED_ID= :pubmed_id
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

    all_publication_data = []

    publication_attr_list = [
        'id', 'pmid', 'journal', 'title',
        'publicationDate', 'resourcename', 'author', 'author_s',
        'authorAscii', 'authorAscii_s', 'authorsList',
        'associationCount', 'studyCount', 'description'
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


                #########################################
                # Get List of Studies per Publication
                #########################################
                cursor.prepare(publication_study_sql)
                r = cursor.execute(None, {'pubmed_id': publication[1]})
                studies = cursor.fetchall()

                # TEMP FIX
                study_list = []
                for study in studies:
                    study_list.append(study[1])
                    publication_document['parentDocument_accessionId'] = study_list
               
               
                # TEMP FIX
                all_ancestral_groups = []
                # For each study, get the Ancestral Groups
                # child_docs = []
                for study in studies:
                    # study_doc = {}
                    # study_doc['content_type'] = 'childDocument'
                    # study_doc['id'] = "study"+":"+str(study[0])
                    # study_doc['accessionId'] = study[1]
                    

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

                    # Add ancestry information to study data
                    # study_doc['ancestralGroups'] = study_ancestral_groups
                    
                    # Add to child document list
                    # child_docs.append(study_doc)

                # Finally, add child_docs to publication document
                # publication_document['_childDocuments_'] = child_docs
                # publication_document['content_type'] = 'parentDocument'
                publication_document['parentDocument_ancestralGroups'] = all_ancestral_groups



                #############################
                # Create description field
                #############################
                # The description field is formatted as:
                # First author, year, journal, pmid, # studies, # associations.
                year, month, day = publication[4].split("-")
                description = author_data[0][0]+" et al. "+year+" "+publication[2]+" "\
                +"PMID:"+publication[1]+", studies: "+str(study_cnt[0])+", associations: "\
                +str(association_cnt[0])

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


# def save_data(data, docfileSuffix, data_type=None):
def save_data(data, data_type=None):
    '''
    data: list of solr ducments as dictionaries
        dictionaries have to contain the resourcename key.
    '''

    ##
    ## In the future, we can include a test here to make sure that the documents 
    ## contain all the type specific fields.
    ## Now we are only checking the common ones
    ##

    # TODO: Move to a different data QA script
    # requireFields = {
    #     'required' : ['id', 'title', 'description', 'resourcename'],
    #     'variant' : ['associationCount','chromosomeName', 'chromosomePosition', 'mappedGenes', 'region', 'rsID', 'consequence']
    # }

    # # Testing if documents have the required fields:
    # for field in requireFields['required']:
    #     if not field in data[0].keys():
    #         sys.exit("[ERROR] The provided data does has not %s field. Exiting." % field)


    resourcename = data[0]['resourcename']

    # Save data to file
    jsonData = json.dumps(data)
    my_path = os.path.abspath(os.path.dirname(__file__))
    # path = os.path.join(my_path, "data/%s_data_%s.json" % (resourcename, docfileSuffix))
    path = os.path.join(my_path, "data/%s_data.json" % (resourcename))
    with open(path, 'w') as outfile:
        outfile.write(jsonData)


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
        ip, port, sid, username, password = gwas_data_sources.get_db_properties(DATABASE_NAME)
        dsn_tns = cx_Oracle.makedsn(ip, port, sid)
        connection = cx_Oracle.connect(username, password, dsn_tns)

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


        connection.close()


        return all_study_data

    except cx_Oracle.DatabaseError, exception:
        print exception


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
        ip, port, sid, username, password = gwas_data_sources.get_db_properties(DATABASE_NAME)
        dsn_tns = cx_Oracle.makedsn(ip, port, sid)
        connection = cx_Oracle.connect(username, password, dsn_tns)


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


        connection.close()

        return all_trait_data

    except cx_Oracle.DatabaseError, exception:
        print exception


def get_variant_data(DATABASE_NAME, limit=0):
    '''
    Get Variant data for Solr document.
    '''

    # function to retrieve further data from the database:
    def get_more_variant_data(row):
        # Extracting basic variant information:
        resourcename = 'variant'
        ID = row['ID']
        rsID = row['RS_ID']
        consequence = row['FUNCTIONAL_CLASS']

        # Extracting genomic location:
        location = variant_cls.get_variant_location(ID)

        # Extracting mapped genes:
        mapped_genes_list = variant_cls.get_mapped_genes(ID)
        
        mapped_genes_names = [x.split("|")[0] for x in mapped_genes_list]

        # Extracting association count:
        associations = variant_cls.get_association_count(ID)
        
        # Combining data into a dictionary:
        varDoc = {
            'resourcename' : resourcename,
            'id' : "%s-%s" % (resourcename,ID),
            'title' : rsID,
            'rsID' : rsID,
            'associationCount' : associations,
            'mappedGenes' : mapped_genes_list,
            'chromosomeName' : location['chromosome'],
            'chromosomePosition' : location['position'],
            'region' : location['region'],
            'consequence' : consequence,
            'link' : 'variants/rs7329174'

        }
        varDoc['description'] =  '%s:%s, %s, %s, mapped to: %s, associaitons: %s' %(
                    varDoc['chromosomeName'], varDoc['chromosomePosition'], varDoc['region'], varDoc['consequence'],
                    ",".join(mapped_genes_names),varDoc['associationCount']
                )

        # Adding to document list:
        all_variant_data.append(varDoc)

    # Initialize empty list for the documents:
    all_variant_data = []

    # Step 1: initialize variant object:
    variant_cls = Variant.Variant(DATABASE_NAME)

    # Step 2: retrieve all the variants in the database:
    variants_df = variant_cls.get_snps()

    # Inintialize progress bar:
    tqdm.pandas(desc="Returning variant data")

    # Step 3: Calling apply to retrieve all variant data:
    if limit != 0:
        variants_df[1:limit].progress_apply(get_more_variant_data, axis = 1)
    else:
        variants_df.progress_apply(get_more_variant_data, axis = 1)

    return all_variant_data


def get_gene_data():
    '''
    Get Gene data for genes with associations for Solr document.
    '''

    # gene_sql = """
    #     SELECT G.ID, G.GENE_NAME, 'gene' as resourcename 
    #     FROM GENE G
    # """

    # Get only genes with associations, assume if a 
    # Gene has an association it is in the Genomic Context table
    gene_sql = """
        SELECT DISTINCT(G.GENE_NAME), G.ID, 'gene' as resourcename
          FROM GENE G, GENOMIC_CONTEXT GC
        WHERE G.ID=GC.GENE_ID
    """

    # ensembl_gene_sql = """
    #     SELECT EG.ENSEMBL_GENE_ID 
    #     FROM GENE G, GENE_ENSEMBL_GENE GEG, ENSEMBL_GENE EG 
    #     WHERE G.ID=GEG.GENE_ID and GEG.ENSEMBL_GENE_ID=EG.ID 
    #         and G.ID= :gene_id
    # """

    # Get csv list of Ensembl Ids for each Gene name
    ensembl_gene_sql = """
        SELECT listagg(EG.ENSEMBL_GENE_ID, ', ') WITHIN GROUP (ORDER BY EG.ENSEMBL_GENE_ID)
        FROM GENE G, GENE_ENSEMBL_GENE GEG, ENSEMBL_GENE EG
        WHERE G.ID=GEG.GENE_ID and GEG.ENSEMBL_GENE_ID=EG.ID
        and G.ID= :gene_id
    """


    # entrez_gene_sql = """
    #     SELECT ENTRZG.ENTREZ_GENE_ID 
    #     FROM GENE G, GENE_ENTREZ_GENE GENTRZG, ENTREZ_GENE ENTRZG 
    #     WHERE G.ID=GENTRZG.GENE_ID and GENTRZG.ENTREZ_GENE_ID=ENTRZG.ENTREZ_GENE_ID 
    #         and G.ID= :gene_id
    # """

    # Get csv list of Entrez Ids for each Gene name
    entrez_gene_sql = """
        SELECT listagg(EG.ENTREZ_GENE_ID, ', ') WITHIN GROUP(ORDER BY EG.ENTREZ_GENE_ID)
        FROM GENE G, GENE_ENTREZ_GENE GEG, ENTREZ_GENE EG
        WHERE G.ID=GEG.GENE_ID and GEG.ENTREZ_GENE_ID=EG.ID
        and G.ID= :gene_id
    """


    gene_region_sql = """
        SELECT DISTINCT(R.NAME) 
        FROM GENE G, GENOMIC_CONTEXT GC, LOCATION L, REGION R 
        WHERE G.ID=GC.GENE_ID and GC.LOCATION_ID=L.ID and L.REGION_ID=R.ID 
            and G.ID= :gene_id
    """

    # TODO: Consider using gene_sql query to get associationCount
    gene_association_cnt_sql = """
        SELECT COUNT(A.ID) 
        FROM GENE G, GENOMIC_CONTEXT GC, SINGLE_NUCLEOTIDE_POLYMORPHISM SNP, 
            RISK_ALLELE_SNP RAS, RISK_ALLELE RA, LOCUS_RISK_ALLELE LRA, LOCUS L, 
            ASSOCIATION_LOCUS AL, ASSOCIATION A 
        WHERE G.ID=GC.GENE_ID and GC.SNP_ID=SNP.ID and SNP.ID=RAS.SNP_ID 
            and RAS.RISK_ALLELE_ID=RA.ID and RA.ID=LRA.RISK_ALLELE_ID 
            and LRA.LOCUS_ID=L.ID and L.ID=AL.LOCUS_ID and AL.ASSOCIATION_ID=A.ID 
            and G.GENE_NAME= :gene_id
    """


    all_gene_data = []

    gene_attr_list = ['geneName', 'id', 'resourcename', 'ensemblGeneId', \
        'entrezGeneId']


    try:
        ip, port, sid, username, password = gwas_data_sources.get_db_properties(DATABASE_NAME)
        dsn_tns = cx_Oracle.makedsn(ip, port, sid)
        connection = cx_Oracle.connect(username, password, dsn_tns)


        with contextlib.closing(connection.cursor()) as cursor:

            cursor.execute(gene_sql)

            gene_data = cursor.fetchall()

            for gene in tqdm(gene_data, desc='Get Gene data'):
                gene = list(gene)
                gene_document = {}

                # Add data from gene to dictionary
                gene_document[gene_attr_list[0]] = gene[0]
                gene_document[gene_attr_list[1]] = gene[2]+":"+str(gene[1])
                gene_document[gene_attr_list[2]] = gene[2]


                ############################
                # Get Ensembl information
                ############################
                cursor.prepare(ensembl_gene_sql)
                r = cursor.execute(None, {'gene_id': gene[1]})
                all_ensembl_gene_ids = cursor.fetchall()

                # Format Ensembl Ids returned as list
                if not all_ensembl_gene_ids:
                    gene_document[gene_attr_list[0]] = None
                else:
                    gene_document[gene_attr_list[3]] = all_ensembl_gene_ids[0][0]


                # Format Ensembl Ids returned as 1-to-many
                # if not all_ensembl_gene_ids:
                #     # add placeholder values
                #     # gene.append(None)
                #      gene_document[gene_attr_list[0]] = None
                # else:
                #     all_ens_ids = []
                #     for ensembl_gene_id in all_ensembl_gene_ids:
                #         # gene.append(ensembl_gene_id[0])
                #         all_ens_ids.append(ensembl_gene_id[0])
                #     gene_document[gene_attr_list[3]] = all_ens_ids


                ###########################
                # Get Entrez information
                ###########################
                cursor.prepare(entrez_gene_sql)
                r = cursor.execute(None, {'gene_id': gene[1]})
                all_entrez_gene_ids = cursor.fetchall()
                
                if not all_entrez_gene_ids:
                    gene_document[gene_attr_list[0]] = None
                else:
                    gene_document[gene_attr_list[4]] = all_entrez_gene_ids[0][0]

                # Format Entrez Ids returned as 1-to-many
                # if not all_entrez_gene_ids:
                #     # add placeholder values
                #     # gene.append(None)
                #     gene_document[gene_attr_list[4]] = None
                # else:
                #     all_ent_ids = []
                #     for entrez_gene_id in all_entrez_gene_ids:
                #         # gene.append(entrez_gene_id[0])
                #         all_ent_ids.append(entrez_gene_id[0])
                #     gene_document[gene_attr_list[4]] = all_ent_ids



                # add snp location to variant_data data, 
                # chromosome, position, region
                # if not all_snp_locations:
                #     # add placeholder values
                #     # NOTE: Current Solr data does not include field when value is null
                #     for item in range(3):
                #         variant.append(None)
                # else:
                #     variant.append(all_snp_locations[0][0])
                #     variant.append(all_snp_locations[0][1])
                #     variant.append(all_snp_locations[0][2])


                # Add gene document dictionary to list of all gene docs
                all_gene_data.append(gene_document)


        # print "** All Gene: ", all_gene_data

        connection.close()

        return all_gene_data

    except cx_Oracle.DatabaseError, exception:
        print exception


if __name__ == '__main__':
    '''
    Create Solr documents for categories of interest.
    '''

    # Commandline arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--database', default='SPOTREL', choices=['DEV3', 'SPOTREL'],
                        help='Run as (default: SPOTREL).')
    parser.add_argument('--limit', type=int, help='Limit the nuber of document to this number for testing purposes.')
    parser.add_argument('--data_type', default='publication',
                        choices=['publication', 'trait', 'variant', 'all'],
                        help='Run as (default: publication).')
    args = parser.parse_args()

    global DATABASE_NAME
    DATABASE_NAME = args.database
    limit = args.limit

    # Docfile suffix
    # now = datetime.datetime.now()
    # docfileSuffix = now.strftime("%Y.%m.%d-%H.%M")

    # Initialize database connection
    db_object = DBConnection.gwasCatalogDbConnector(DATABASE_NAME)

    # select function
    dispatcher = {
        'publication' : get_publication_data, 
        # 'study' : get_study_data,
        'trait' : get_trait_data, 
        'variant' : get_variant_data, 
        # 'gene' : get_gene_data
    }

    # Get the list of document types to create
    document_types = [args.data_type]
    if args.data_type == 'all': document_types = ['publication', 'trait', 'variant']

    # Loop through all the document types and generate document
    for doc_type in document_types:
        document_data = dispatcher[doc_type](db_object.connection, limit)
        # save_data(document_data, docfileSuffix)
        save_data(document_data)

    # Close database connection
    db_object.close()
