import argparse
import sys
import json
import numpy as np
from gwas_db_connect import DBConnection

# Custom modules
from scripts.document_types import publication
from scripts.document_types import trait
from scripts.document_types import study
from scripts.document_types import variant
from scripts.document_types import gene
from scripts.document_types import unpub_study


def publication_data(connection, limit=0, test=False):
    return publication.get_publication_data(connection, testRun = test)

def trait_data(connection, limit=0, test=False):
    return trait.get_trait_data(connection)

def study_data(connection, limit=0, test=False):
    return study.get_study_data(connection)

def unpub_study_data(connection, limit=0, test=False):
    return unpub_study.get_unpub_study_data(connection)

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
def save_data(data, targetDir, data_type=None):
    '''
    data: list of solr ducments as dictionaries
        dictionaries have to contain the resourcename key.
    '''

    resourcename = data[0]['resourcename']

    fileNameWithPath = '{}/{}_data.json'.format(targetDir, resourcename)

    with open(fileNameWithPath, 'w') as outfile:
        json.dump(data, outfile, cls=NumpyEncoder)

def variant_data(connection, limit=0, test=False):
    return variant.get_variant_data(connection, limit, testRun = test)

def gene_data(connection, limit=0, test=False):
    return gene.get_gene_data(connection, RESTURL, limit, testRun = test)


class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NpEncoder, self).default(obj)


def main():
    '''
     Create Solr documents for categories of interest.
     '''

    # Commandline arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--database', default='SPOTREL', choices=['DEV3', 'SPOTREL'],
                        help='Run as (default: SPOTREL).')
    parser.add_argument('--limit', type=int,
                        help='Limit the number of created documents to this number for testing purposes.', default=0)
    parser.add_argument('--document', default='publication',
                        choices=['publication', 'trait', 'variant', 'gene', 'unpub', 'study', 'all'],
                        help='Run as (default: publication).')
    parser.add_argument('--restURL', default='https://rest.ensembl.org',
                        help='URL of Ensembl REST API. Determines which Ensembl release will be used.')
    parser.add_argument('--test',
                        help='Generate docments on a test set. (needs to be implemented to each document type!)',
                        action='store_true', default=False)
    parser.add_argument('--targetDir', help='Folder in which the output files will be saved.', type=str,
                        default='./data')

    args = parser.parse_args()

    targetDir = args.targetDir

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
        'publication': publication_data,
        'trait': trait_data,
        'variant': variant_data,
        'gene': gene_data,
        'unpub': unpub_study_data,
        'study': study_data
    }

    # Get the list of document types to create
    documents = [args.document]
    if args.document == 'all': documents = ['publication', 'trait', 'variant', 'gene', 'study', 'unpub']

    # Loop through all the document types and generate document
    for doc in documents:
        document_data = dispatcher[doc](db_object.connection, limit, test)
        document_data = check_data(document_data, doc)
        # save_data(document_data, docfileSuffix)
        save_data(document_data, targetDir)

    # Close database connection
    db_object.close()


if __name__ == '__main__':
    main()

