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
from database import DBConnection
import gwas_data_sources
from ols import OLSData
from document_types import publication
from document_types import trait
from document_types import study
from document_types import variant


def publication_data(connection, limit=0):
    return publication.get_publication_data(connection)


def trait_data(connection, limit=0):
    return trait.get_trait_data(connection)


def study_data(connection, limit=0):
    return study.get_study_data(connection)


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

    current_dir = os.getcwd()
    path = os.path.join(current_dir, "data/%s_data.json" % (resourcename))


    # path = os.path.join(my_path, "data/%s_data_%s.json" % (resourcename, docfileSuffix))
    # path = os.path.join(my_path, "data/%s_data.json" % (resourcename))
    with open(path, 'w') as outfile:
        outfile.write(jsonData)

def variant_data(connection, limit=0):
    return variant.get_variant_data(connection, limit)


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
    parser.add_argument('--limit', type=int, help='Limit the number of created documents to this number for testing purposes.')
    parser.add_argument('--document', default='publication',
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
        'publication' : publication_data, 
        # 'study' : study_data,
        'trait' : trait_data, 
        'variant' : variant_data, 
        # 'gene' : get_gene_data
    }

    # Get the list of document types to create
    documents = [args.document]
    if args.document == 'all': documents = ['publication', 'trait', 'variant']

    # Loop through all the document types and generate document
    for doc in documents:
        document_data = dispatcher[doc](db_object.connection, limit)
        # save_data(document_data, docfileSuffix)
        save_data(document_data)

    # Close database connection
    db_object.close()
