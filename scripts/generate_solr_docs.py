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
from document_types import gene
from document_types import gene_annotator

def publication_data(connection, limit=0, test=False):
    return publication.get_publication_data(connection, testRun = test)

def trait_data(connection, limit=0, test=False):
    return trait.get_trait_data(connection)

def study_data(connection, limit=0, test=False):
    return study.get_study_data(connection)

def check_data(data, doctype):
    '''
    This function checks if all the required fields of the documents are present.
    Input is the list with all the documents.
    If any of the required field is missing from the document, it will be deleted.
    '''

    # Check if the submitted data is a dictionary:
    if not isinstance(data, list):
        
        # Report to standard output:
        print("[Error] An error occured while generating the %s documents: the submitted data is not a list, but a %s!" % (doctype, type(data)))
        print("[Error] The data looks like this:")
        print(data)

        # Exiting with reporting error:
        sys.exit('[Error] %s data could not be saved. Exiting.!' % doctype)

    # A minimal list of fields that need to be found in every documents:
    requireFields = ['id', 'title', 'description', 'resourcename']

    for i, doc in reversed(list(enumerate(data))):
        for field in requireFields:
            if not field in doc:
                print("[Warning] %s is missing from document. Removing." % (field))
                print('[Warning] The problematic document looks like this: ')
                print(doc)

                data.pop(i)
                break

    # Exit if there's no document left to save:
    if len(data) == 0:
        sys.exit('[Error] %s data could not be saved as no documents left. Exiting.' % doctype)

    return(data)

# def save_data(data, docfileSuffix, data_type=None):
def save_data(data, data_type=None):
    '''
    data: list of solr ducments as dictionaries
        dictionaries have to contain the resourcename key.
    '''

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

def variant_data(connection, limit=0, test=False):
    return variant.get_variant_data(connection, limit, testRun = test)

def gene_data(connection, limit=0, test=False):
    return gene.get_gene_data(connection, RESTURL, limit)

if __name__ == '__main__':
    '''
    Create Solr documents for categories of interest.
    '''

    # Commandline arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--database', default='SPOTREL', choices=['DEV3', 'SPOTREL'],
                        help='Run as (default: SPOTREL).')
    parser.add_argument('--limit', type=int, help='Limit the number of created documents to this number for testing purposes.', default = 0)
    parser.add_argument('--document', default='publication',
                        choices=['publication', 'trait', 'variant', 'gene', 'all'],
                        help='Run as (default: publication).')
    parser.add_argument('--restURL', default = 'https://rest.ensembl.org', help = 'URL of Ensembl REST API. Determines which Ensembl release will be used.')
    parser.add_argument('--test', help = 'Generate ducments on a test set. (needs to be implemented to each document types1!)', action = 'store_true', default=False)
    args = parser.parse_args()

    global DATABASE_NAME
    DATABASE_NAME = args.database

    limit = args.limit
    test = args.test

    global RESTURL  
    RESTURL = args.restURL

    # Docfile suffix
    # now = datetime.datetime.now()
    # docfileSuffix = now.strftime("%Y.%m.%d-%H.%M")

    # Initialize database connection
    db_object = DBConnection.gwasCatalogDbConnector(DATABASE_NAME)

    # select function
    dispatcher = {
        'publication' : publication_data, 
        'trait' : trait_data, 
        'variant' : variant_data, 
        'gene' : gene_data
    }

    # Get the list of document types to create
    documents = [args.document]
    if args.document == 'all': documents = ['publication', 'trait', 'variant', 'gene']

    # Loop through all the document types and generate document
    for doc in documents:
        document_data = dispatcher[doc](db_object.connection, limit, test)
        document_data = check_data(document_data, doc)
        # save_data(document_data, docfileSuffix)
        save_data(document_data)

    # Close database connection
    db_object.close()

