import requests
import sys
import os
import argparse
import pandas as pd
import glob


class solr(object):
    def __init__(self, server, port, core):
        self.base_url = 'http://{}:{}/solr'.format(server, port)
        self.core = core
        self.isRunning()

    def reloadCore(self):
        URL = '{}/admin/cores?action=RELOAD&core={}'.format(self.base_url, self.core)
        content = self._submit(URL)
        return (1)

    def wipeCore(self):
        print('[Info] Wiping all documents from {}...'.format(self.core))
        URL = '{}/{}/update?commit=true'.format(self.base_url, self.core)
        content = self._submit(URL, jsonData={"delete": {"query": "*:*"}})
        self.getDocCount()
        return (0)

    def addDocument(self, documentFile):
        print("[Info] Adding {} to the solr core.".format(documentFile))
        URL = '{}/{}/update?commit=true'.format(self.base_url, self.core)
        content = self._submit(URL, data=open(documentFile, 'rb'))
        return (0)

    def getSchema(self):
        URL = '{}/{}/schema'.format(self.base_url, self.core)
        content = self._submit(URL)
        fieldsDf = pd.DataFrame(content['schema']['fields'])
        print('[Info] Schema retrieved. Number of fields: {}'.format(len(fieldsDf)))
        return (fieldsDf)

    def isRunning(self):
        URL = '{}/{}/admin/ping?wt=json'.format(self.base_url, self.core)
        content = self._submit(URL)
        if content['status'] == 'OK':
            print('[Info] Solr server ({}) is up and running.'.format(self.base_url))
        else:
            print('[Error] Solr server is not up. Exiting.')
            sys.exit(1)

    def getDocCount(self):
        URL = '{}/{}/query?q=*:*&rows=1&wt=json&indent=true'.format(self.base_url, self.core)
        content = self._submit(URL)
        print('[Info] Number of document in the {} core: {}'.format(self.core, content['response']['numFound']))

    def _submit(self, URL, headers={"Content-Type": "application/json", "Accept": "application/json"}, jsonData={},
                data=''):
        if not jsonData and not data:
            r = requests.get(URL, headers=headers, )
        elif data:
            r = requests.post(URL, headers=headers, data=data)
        else:
            r = requests.post(URL, headers=headers, json=jsonData)

        if not r.ok:
            r.raise_for_status()
            sys.exit(1)

        try:
            return (r.json())
        except:
            return (r.content)


def validateDocument(schema, documentFile):
    # Reading file into a dataframe
    docDf = pd.read_json(documentFile)

    # Validate column headers
    schemaFieldNames = set(schema.name)
    for colName in docDf.columns:
        if not colName in schemaFieldNames:
            print(
                "[Warning] {} is a field name in {}, which is not defined in the schema. File will be skipped.".format(
                    colName, documentFile))
            return (0)

    print("[Info] Field names successfully validated for {}".format(documentFile))
    return (1)

    # TODO: implement validation of each document:
    # Validate every rows for each columns
    # Is the type good?
    # Is the field multivalued?


def main():
    # Parsing command line arguments:
    parser = argparse.ArgumentParser()
    parser.add_argument('--server', type=str, help='Name of the solr server eg. localhost.')
    parser.add_argument('--port', type=int, help='Port number of the solr instance.')
    parser.add_argument('--core', type=str, help='Name of the solr core. eg. gwas or gwas_slim')
    parser.add_argument('--documentFolder', type=str, help='Folder with the json documents.')
    args = parser.parse_args()

    server = args.server
    port = args.port
    core = args.core
    documentFolder = args.documentFolder

    # Is the server provided:
    if not server:
        print('[Error] Server name is not provided (eg. localhost). Exiting.')
        sys.exit(1)

    # Is the port provided:
    if not port:
        print('[Error] Number is not provided (eg. 8983). Exiting.')
        sys.exit(1)

    # Is the core provided:
    if not core:
        print('[Error] Solr core is not provided (eg. gwas). Exiting.')
        sys.exit(1)

    # Is the documentfolder provided and exists:
    if not os.path.isdir(documentFolder):
        print('[Error] No valid folder containing documents provided. Exiting.')
        sys.exit(1)

    # Initializing solr object:
    solrObj = solr(server, port, core)

    # # Cleaning solr:
    solrObj.getDocCount()
    solrObj.wipeCore()

    # Reload core:
    solrObj.reloadCore()

    # Get schema from the running instance:
    solrSchema = solrObj.getSchema()
    #   indexed multiValued              name required stored          type
    # 0    True         NaN            _root_      NaN  False        string
    # 1    True         NaN         _version_      NaN   True          long
    # 2   False       False  associationCount      NaN   True           int
    # 3    True        True            author      NaN   True  text_general
    # 4    True        True       authorAscii      NaN   True  text_general

    # Reading all files from a directory and validate fields:
    for documentFile in glob.glob('{}/*.json'.format(documentFolder)):
        valid = validateDocument(solrSchema, documentFile)
        if valid:
            solrObj.addDocument(documentFile)

    solrObj.getDocCount()


if __name__ == '__main__':
    main()