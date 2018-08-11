import requests
import argparse
from tqdm import tqdm
from requests.utils import quote
import re


class slimSolrWalker(object):
    '''
    This class is responsible to return fields from all document types from a solr server.

    Initializiation: 
        <solrHost URL>
        <resource name>
        <field to extract>
        <limit if not all document wanted>
        <stepsize in which the documents are walked along>

    Once the object is created, the fields can be extracted and returned in an array
    '''

    def __init__(self, slimHost, resource_name, field, limit, stepSize = 500):
        
        # Adding values to object:
        self.resourcename = resource_name
        self.fieldToExtract = field
        self.stepSize = stepSize
        self.baseURL = slimHost
        self.limit = limit
        self.__get_document_count()


    def __get_document_count(self):
        
        URL = "%s/select?q=resourcename:%s&rows=1&wt=json&indent=true" % (self.baseURL, self.resourcename)
        r = requests.get(URL)
        if not r.status_code == 200:
            print(r.text)
            exit("[Error] Could not connect to the slim solr server. Exiting.")

        data = r.json()
        
        if data['response']['numFound'] > self.limit and self.limit != 0:
            self.rowNum = self.limit
            self.stepSize = self.limit
        else:
            self.rowNum = data['response']['numFound']

        self.pageNum = int(self.rowNum / self.stepSize) + 1 

    def __get_page(self, page):
        startRow = page * self.stepSize
        URL = "%s/select?q=resourcename:%s&start=%s&rows=%s&wt=json&indent=true" % (self.baseURL, self.resourcename, startRow, self.stepSize)
        r = requests.get(URL)
        if not r.status_code == 200:
            print(r.text)
            exit("[Error] Could not connect to the slim solr server. Exiting.")

        data = r.json()
        values = []
        for document in data['response']['docs']:
            values.append(document[self.fieldToExtract])

        return(values)

    def get_all_values(self):
        all_values = []
        for page in range(self.pageNum):
            all_values += self.__get_page(page)

        return(all_values)


def get_identifiers(slimhost, resource_name, field, limit):
    solrWalk_obj = slimSolrWalker(slimHost, resource_name, field, limit)
    return(solrWalk_obj.get_all_values())

# Checking publications:
def publication_test(fatHost, identifiers):
    failed_identifiers = {}

    for pmid in tqdm(identifiers, desc='Testing publications... '):
        URL = ('%s/select?wt=json&rows=99999&start=0&q=%s&rows=10000&group=true&group.limit=99999&group.field=resourcename&facet=true&facet.field=resourcename&hl=true&hl.simple.pre=%%3Cb%%3E&hl.simple.post=%%3C%%2Fb%%3E&hl.snippets=100&hl.fl=shortForm,efoLink&fl=pubmedId%%2Ctitle%%2Cauthor_s%%2Corcid_s%%2Cpublication%%2CpublicationDate%%2CcatalogPublishDate%%2CauthorsList%%2CinitialSampleDescription%%2CreplicateSampleDescription%%2CancestralGroups%%2CcountriesOfRecruitment%%2CancestryLinks%%2Cassociation_rsId%%2CtraitName%%2CmappedLabel%%2CmappedUri%%2CtraitUri%%2CshortForm%%2Clabel%%2CefoLink%%2Cparent%%2Cid%%2Cresourcename%%2CriskFrequency%%2Cqualifier%%2CpValueMantissa%%2CpValueExponent%%2CsnpInteraction%%2CmultiSnpHaplotype%%2CrsId%%2CstrongestAllele%%2Ccontext%%2Cregion%%2CentrezMappedGenes%%2CreportedGene%%2Cmerged%%2CcurrentSnp%%2CstudyId%%2CchromosomeName%%2CchromosomePosition%%2CchromLocation%%2CpositionLinks%%2Cauthor_s%%2Cpublication%%2CpublicationDate%%2CcatalogPublishDate%%2CpublicationLink%%2CaccessionId%%2CinitialSampleDescription%%2CreplicateSampleDescription%%2CancestralGroups%%2CcountriesOfRecruitment%%2CnumberOfIndividuals%%2CtraitName_s%%2CmappedLabel%%2CmappedUri%%2CtraitUri%%2CshortForm%%2Clabelda%%2Csynonym%%2CefoLink%%2Cid%%2Cresourcename&fq%%3Aresourcename%%3Aassociation+or+resourcename%%3Astudy' %(fatHost,pmid))
        r = requests.get(URL)

        if not r.status_code == 200:
            print(r.text)
            exit("[Error] Could not connect to the slim solr server. Exiting.")
        
        data = r.json()
        if data['grouped']['resourcename']['matches'] == 0: 
            failed_identifiers[pmid] = "Missing Pubmed ID!"

    return(failed_identifiers)

# https://www.ebi.ac.uk/ols/api/ontologies/efo/terms/http%253A%252F%252Fwww.ebi.ac.uk%252Fefo%252FEFO_1000649/graph
def get_EFO_from_OLS(EFO_URL):
    encoded_EFO_URL = quote(quote(EFO_URL, safe=''), safe='')
    URL = 'https://www.ebi.ac.uk/ols/api/ontologies/efo/terms/%s/graph' % encoded_EFO_URL
    r = requests.get(URL)
    if not r.status_code == 200:
        print(r.text)
        exit("[Error] Could not connect to the slim solr server. Exiting.")

    data = r.json()
    node_list = []
    for node in data['nodes']:
        p = re.compile('[^\/]+$')
        for node in p.findall(node['iri']):
            if "#" not in node: node_list.append(node) 
                
    return(node_list)

# Checkig trait documents:
def trait_test(fatHost, identifiers):
    failed_identifiers = {}
    for identifier in tqdm(identifiers, desc='Testing EFO terms... '):
        
        # Get child terms:
        efoNodes = get_EFO_from_OLS(identifier)
        efoString = ",".join(efoNodes)

        # Query solr:
        URL = ('%s/select?wt=json&rows=99999&start=0&q=%s&rows=10000&group=true&group.limit=99999&group.field=resourcename&facet=true&facet.field=resourcename&hl=true&hl.simple.pre=%%3Cb%%3E&hl.simple.post=%%3C%%2Fb%%3E&hl.snippets=100&hl.fl=shortForm,efoLink&fl=pubmedId%%2Ctitle%%2Cauthor_s%%2Cpublication%%2CpublicationDate%%2CcatalogPublishDate%%2CinitialSampleDescription%%2CreplicateSampleDescription%%2CancestralGroups%%2CcountriesOfRecruitment%%2CancestryLinks%%2CtraitName%%2CmappedLabel%%2CmappedUri%%2CtraitUri%%2CshortForm%%2Clabel%%2CefoLink%%2Cparent%%2Cid%%2Cresourcename%%2CriskFrequency%%2Cqualifier%%2CpValueMantissa%%2CpValueExponent%%2CsnpInteraction%%2CmultiSnpHaplotype%%2CrsId%%2CstrongestAllele%%2Ccontext%%2Cregion%%2CentrezMappedGenes%%2CreportedGene%%2Cmerged%%2CcurrentSnp%%2CstudyId%%2CchromosomeName%%2CchromosomePosition%%2CchromLocation%%2CpositionLinks%%2Cauthor_s%%2Cpublication%%2CpublicationDate%%2CcatalogPublishDate%%2CpublicationLink%%2CaccessionId%%2CinitialSampleDescription%%2CreplicateSampleDescription%%2CancestralGroups%%2CcountriesOfRecruitment%%2CnumberOfIndividuals%%2CtraitName_s%%2CmappedLabel%%2CmappedUri%%2CtraitUri%%2CshortForm%%2Clabelda%%2Csynonym%%2CefoLink%%2Cid%%2Cresourcename&fq%%3Aresourcename%%3Aassociation+or+resourcename%%3Astudy' %(fatHost,efoString))
        r = requests.get(URL)
        if not r.status_code == 200:
            print(r.text)
            exit("[Error] Could not connect to the slim solr server. Exiting.")
        
        data = r.json()
        try:
            if data['grouped']['resourcename']['matches'] == 0: 
                failed_identifiers[identifier] = "Missing EFO term!"
        except:
            failed_identifiers[identifier] = "Bad query: %s" % URL

    return(failed_identifiers)

# Checking variant documents:
def variant_test(fatHost, identifiers):
    failed_identifiers = {}

    for rsID in tqdm(identifiers, desc='Testing variants... '):
        # We have to skip the special rsID which is a dash... what a shitty database...
        if rsID == '-': continue 

        rsID = quote(quote(rsID, safe=''), safe = '')
        URL = ('%s/select?wt=json&rows=1000&start=0&fq=resourcename%%3Aassociation&sort=pValueExponent+asc%%2C+pValueMantissa+asc&q=rsId%%3A%s' %(fatHost,rsID))
        r = requests.get(URL)

        if not r.status_code == 200:
            print(r.text)
            exit("[Error] Could not connect to the slim solr server. Exiting.")
        
        data = r.json()
        try:
            if data['response']['numFound'] == 0: 
                failed_identifiers[rsID] = "Missing rsID"
        except:
            print("[Info] %s has failed. No proper respnse." % rsID)

    return(failed_identifiers)

def get_doc_count(slimhost,resourcename):
    URL = ('%s/select?q=resourcename%%3A%s&wt=json&indent=true' %(slimhost, resourcename))
    r = requests.get(URL)
    if not r.status_code == 200:
        print(r.text)
        exit("[Error] Could not connect to the slim solr server. Exiting.")
    
    data = r.json()
    count = data['response']['numFound']
    return(count)

def delete_documents(slimhost,resourcename,field,failed_identifiers):
    
    if len(failed_identifiers) > 0:
        print("[Info] Removing %s %s documents from the solr index" % (len(failed_identifiers), resourcename))
    else:
        print("[Info] %s documents are fine, no need to change solr index." % resourcename)
        return(0)
    documentCount = get_doc_count(slimhost, resourcename)
    print("[Info] Number of %s documents: %s" %(resourcename, documentCount))
    URL = "%s/update?commit=true" % slimhost

    for identifier in failed_identifiers:
        queryString = ('%s:"%s" AND resourcename:%s' %(field, identifier, resourcename))
        print("\t[Info] Removing %s" % identifier)
        data = {"delete": {"query":queryString}}
        r = requests.post(URL, json=data)
        if not r.status_code == 200:
            print(r.text)
            print("[Warning] Failed to delete document.")
    documentCount = get_doc_count(slimhost, resourcename)
    print("[Info] Number of %s documents: %s" %(resourcename, documentCount))

if __name__ == '__main__':
    '''
    Testing the concordance of the slim and fat solr index
    '''

    # Commandline arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--slim', help='Address of the slim solr server (default: http://garfield.ebi.ac.uk:8983/solr/gwas_slim).', default = 'http://garfield.ebi.ac.uk:8983/solr/gwas_slim')
    parser.add_argument('--fat', help='Address of the fat solr server (default: http://garfield.ebi.ac.uk:8983/solr/gwas )', default = 'http://garfield.ebi.ac.uk:8983/solr/gwas')
    parser.add_argument('--document', default='publication', choices=['publication', 'trait', 'variant', 'all'],
                        help='The document type to be checked (default: publication).')
    parser.add_argument('--limit', help='Limit the number of items to test.', type = int, default = 0)
    args = parser.parse_args()

    # Get the list of document types to create
    documents = [args.document]
    if args.document == 'all': documents = ['publication', 'trait', 'variant']

    # Extracting solr hosts:
    slimHost = args.slim
    fatHost = args.fat
    limit = args.limit

    # At this point we don't check if the hosts are accessible or not.

    # select function
    dispatcher = {
        'publication' : [publication_test, 'pmid'],
        'trait'       : [trait_test, 'mappedUri'],
        'variant'     : [variant_test, 'rsID'],
    }

    for doc in documents:

        # Extract values from the slim solr to test in the fat solr:
        print("[Info] slimhost: %s, doc: %s, field: %s, limit: %s" %(slimHost, doc, dispatcher[doc][1], limit))
        identifiers = get_identifiers(slimHost, doc, dispatcher[doc][1], limit)

        # passing the identifier to the documnet specific functions:
        failed_identifiers = dispatcher[doc][0](fatHost, identifiers)
        if len(failed_identifiers)> 0:
            print("[Info] Number of failed identifiers: %s" %(len(failed_identifiers)))
            print("[Info] Failed identifiers:")
            for ID in failed_identifiers.keys():
                print("\t%s - %s" %(ID, failed_identifiers[ID]))

            # Removing documents:
            delete_documents(slimHost,doc,dispatcher[doc][1],failed_identifiers.keys())
        else:
            print ("[Info] %s documents look good." % doc)

     

## Extracting all the data from the fat solr:
# http://garfield.ebi.ac.uk:8983/solr/gwas/select?wt=json&q=*&resourcename=study&fl=pubmedId%2CrsId%2CshortForm%2Cassociation_rsId&rows=20
# We need to create lists from all the document types. pmids, efo, variant.
# Compare in both direction
