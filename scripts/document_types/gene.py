import pandas as pd
import pybedtools
import requests
import numpy as np
import gzip
import ftplib
import re
import pickle
from tqdm import tqdm

import sys, os

# Importing Ensembl REST class:
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'EnsemblREST'))
from REST import REST

# Looping through all variant in the data 
def process_variant_document(variant_data, mapping):
    gene_data = {}
    for variant in tqdm(variant_data, desc = 'Reading variant data:'):
        if len(variant['mappedGenes']) != 0:
            '''
                "mappedGenes": [
                    "LHX5-AS1|ENSG00000257935|104355219",
                    "LOC105369990|105369990"
                ]
            '''
            for mappedGene in variant['mappedGenes']:
                if mappedGene == 'intergenic':
                    continue

                IDs =  mappedGene.split("|")
                gName = IDs[0]

                if 'ENSG' in IDs[1]: 
                    gEnsID = IDs[1]
                elif not 'ENSG' in IDs[1] and IDs[1] in mapping:
                    gEnsID = mapping.loc[[IDs[1]]].tolist()[0]
                else:
                    continue

                try:
                    gene_data[gEnsID]['studyCount'] += variant['studyCount']
                    gene_data[gEnsID]['associationCount'] += variant['associationCount']
                    gene_data[gEnsID]['rsIDs'].append(variant['current_rsID'])
                except:
                    try:
                        gene_data[gEnsID] = {
                            'studyCount' : variant['studyCount'],
                            'associationCount' : variant['associationCount'],
                            'rsIDs' : [variant['current_rsID']],
                        }
                    except:
                        print(variant)
    return(gene_data)

class GeneAnnotationParser(object):
    '''
    This class will get the mapped genes for variants submitted in a proper bed formatted lines.
    '''

    # Ensembl REST server:
    __Ensembl_REST_server = 'http://jul2018.rest.ensembl.org/'
    
    # Ensembl gene coordinates (release specific!!! will be parsed later!):
    __Ensembl_gtf = {
        'HOST' : 'ftp.ensembl.org',
    }
    
    # HGNC gene names and synonyms:
    __HGNC = {
        'HOST' : 'ftp.ebi.ac.uk',
        'DIRN' : 'pub/databases/genenames/new/tsv',
        'FILE' : 'non_alt_loci_set.txt'
    }
    
    # Cytoband names and coordinates from UCSC:
    __cytobandURL = 'http://hgdownload.cse.ucsc.edu/goldenPath/hg38/database/cytoBand.txt.gz'
    
    def __init__(self, verbose = False):
        '''
        To initialize the object, a properly formatted bedfile needed containing the genes.
        '''

        # Import handler for the REST API of Ensembl:
        self.EnsemblREST = REST(self.__Ensembl_REST_server)

        self.__detect_ensembl_release() # Finding out what Ensembl release we are using
        self.__get_Ensembl_genes() # Extracting Ensembl data:
        self.__get_cytoband() # Extracting cytoband
        self.__get_HGNC() # Extracting HGNC names and synonyms

        # Pooling together all the data:
        self.__merge_data()

    def __download_ftp(self, HOST, DIRN, FILE):
        '''
        This function download a given file to the download location under data/.
        '''

        print (u"[Info] Downloading %s from %s.. " %(FILE, HOST))

        f = ftplib.FTP(HOST, timeout=300) # Opening connection
        f.login(user='anonymous', passwd='') # Assuming anonymous connection, there's no need to authenticate
        f.cwd(DIRN) # Change to working director
        
        # Saving file:
        filedata = open('data/%s' % FILE, 'w+b')
        f.retrbinary("RETR %s" % FILE, filedata.write)
        
        f.quit()
        return('data/%s' % FILE)

    def __detect_ensembl_release(self):
        '''
        Using the REST URL, we found out which Ensembl version we are on, and according to that 
        version, we set the ftp location of the gtf file.
        '''

        # Based on the REST server, check Ensembl version:
        self.__Ensembl_release = self.EnsemblREST.getEnsemblRelease()

        # Based on the Ensembl version, set the file to be downloaded:
        self.__Ensembl_gtf['DIRN'] = 'pub/release-%s/gtf/homo_sapiens' % self.__Ensembl_release
        self.__Ensembl_gtf['FILE'] = 'Homo_sapiens.GRCh38.%s.chr.gtf.gz' % self.__Ensembl_release

        print("[Info] We are using e%s Ensembl version." % self.__Ensembl_release)

    def __get_Ensembl_genes(self):
        '''
        This function downloads the Ensembl gene set in gtf format. It uses the build in variable of the class
        Input: URL pointing to the gtf file on the public ftp site

        Steps:
        1. Download gtf file from ftp
        2. Filter for only genes.
        3. Parse gene annotation.
        4. Select relevant columns, sort dataframe

        Output: dataframe object with the genes + saves dataframe in pkl format
        '''

        # A shortcut to speed up stuffs:
        pickle_filename = 'data/geneGTFTable.pkl'
        if os.path.isfile(pickle_filename):
            print ("[Info] Reading Ensembl gene table from pickle: %s" % pickle_filename)
            self.__ensembl_data = pd.read_pickle(pickle_filename)
            return(0)

        # Extracting location information 
        HOST = self.__Ensembl_gtf['HOST']
        DIRN = self.__Ensembl_gtf['DIRN']
        FILE = self.__Ensembl_gtf['FILE']

        # Downloading file:
        self.__download_ftp(HOST, DIRN, FILE)
        
        # Reading data into a dataframe
        print ("[Info] Reading file into a pandas dataframe.")
        geneGtfTable = pd.read_table("data/%s" % FILE, header='infer', comment='#', index_col=6,
                 names = ['chr', 'source', 'type', 'start', 'end', 'dot1', 'strand', 'annot'], sep = "\t",
                 dtype = {'chr' : str})
        
        print("[Info] Extracting genes from gtf file.")
        geneGtfTable = geneGtfTable[geneGtfTable.type == 'gene']
        geneGtfTable = geneGtfTable.set_index(np.arange(0,len(geneGtfTable)))
        
        print('[Info] Number of genes in the current Ensembl release: %s' % len(geneGtfTable))
        print('[Info] Parsing gene annotation.')
        def split_annot(annot):
            return pd.Series(( x.replace('"','') for x in re.findall(r"\".+?\"", annot)))

        geneGtfTable[['ID', 'version', 'name', 'source', 'biotype']] = geneGtfTable.annot.apply(split_annot)
        geneGtfTable = geneGtfTable[['chr', 'start', 'end', 'ID', 'biotype', 'name']]
        geneGtfTable['chr'] = geneGtfTable['chr'].astype(dtype = str)
        geneGtfTable.sort_values(by=['chr','start'], inplace = True, )
        print (geneGtfTable.head())
        print ('[Info] Creating bedtools object')
        self.__ensembl_data = geneGtfTable

        # Saving pickle:
        geneGtfTable.to_pickle(pickle_filename)
        
    def __get_HGNC(self):
        '''
        Extract HGNC data from ftp. Extracting relevant columns and formats

        1. Downloading file from HGNC.
        2. Select certain fields: alternative names/synonyms + cros ref IDs
        3. Pool these names together + ensembl ID and entrez ID are separate columns.
        4. Saving file in pkl format.
        5. Adding dataframe to object.
        '''

        # A shortcut to speed up stuffs:
        pickle_filename = 'data/HGNCTable.pkl'
        if os.path.isfile(pickle_filename):
            print ("[Info] Reading HGNC table from pickle: %s" % pickle_filename)
            self.__HGNC_data = pd.read_pickle(pickle_filename)
            return(0)
                
        # Extracting location information 
        HOST = self.__HGNC['HOST']
        DIRN = self.__HGNC['DIRN']
        FILE = self.__HGNC['FILE']

        # Downloading file:
        self.__download_ftp(HOST, DIRN, FILE) 

        # Once we have the file downloaded, we can read it:
        df = pd.read_table("data/%s" % FILE, dtype = str)

        # Extract important columns:
        print ("[Info] Filtering HGNC file...")
        print(df.head())

        def concatenate(x):
            row = x.loc[~ x.isna()].tolist()
            return("|".join([str(x) for x in row]))

        # Adding other IDs:
        df['alternativeIDs'] = df[['hgnc_id', 'vega_id', 'ucsc_id', 
                                   'ena', 'refseq_accession', 'ccds_id', 
                                   'mgd_id', 'uniprot_ids']].apply(concatenate, axis=1)

        # Adding alternative names and synonyms:
        df['synonyms'] = df[['symbol', 'name','alias_symbol', 
                             'alias_name','prev_symbol', 'prev_name']].apply(concatenate, axis=1)

        # Filtering columns:
        df = df[['entrez_id', 'ensembl_gene_id', 'alternativeIDs', 'synonyms']]

        # Reporing:
        print(df.head())

        # Saving data:
        df.to_pickle(pickle_filename)
        self.__HGNC_data = df

    def __merge_data(self):
        '''
        This method merges three dataframes: ensembl, cytobands and HGNC
        The resulting dataframe will be used to generate gene document
        '''

        # What we are using:
        # self.__cytobands 
        # self.__ensembl_data
        # self.__HGNC_data

        # Extracting ensembl dataframe, setting index:
        ensembl_df = self.__ensembl_data.set_index('ID')

        # Merging ensembl with cytobands:
        print("[Info] Merging Ensembl data with cytobands...")
        ensembl_df = ensembl_df.join(self.__cytobands.to_frame(name='cytoband'))

        # Merging ensembl with HGNC:
        print("[Info] Merging Ensembl data with HGNC data...")
        ensembl_df = ensembl_df.join(self.__HGNC_data.set_index('ensembl_gene_id'))

        # Replacing pandas nan values to None:
        ensembl_df = ensembl_df.where((pd.notnull(ensembl_df)), None)

        # Adding to object:
        self.__gene_table = ensembl_df

        # Get entrez mapping table:
        self.__mapping_table = pd.Series(ensembl_df.index, index = ensembl_df.entrez_id.tolist())

        # Report:
        print(ensembl_df.head())

    def __get_cytoband(self):
        '''
        1. Downloading the cytoband file from UCSC - readig as dataframe
        2. Creating bedtools object.
        3. Calculate overlap between cytobands and Ensembl genes - extract only Ensembl ID and cytoband columns.
        4. Consolidate df: concatenate cytobands for genes where a gene overlaps with multiple cytobands - Series where ensembl IDs are indices.
        5. Save series object as pkl + adding to gene object
        '''

        pickle_filename = 'data/cytobandTable.pkl'
        if os.path.isfile(pickle_filename):
            print ("[Info] Reading cytoband table from pickle: %s" % pickle_filename)
            self.__cytobands =  pd.read_pickle(pickle_filename)
            print(self.__cytobands.head())
            return(0)
        
        print (u"[Info] Downloading cytoband data file from UCSC.. ")

        cytobandTable = pd.read_table(self.__cytobandURL, header=None, compression='infer',names = ['chr', 'start', 'end', 'region', 'stain'])
        cytobandTable['chr'] = cytobandTable['chr'].apply(lambda x: x.replace('chr', '')) # adjusting chromosome column

        # Testing if the reading was successful
        print ('[Info] Number of cytobands: %s' % len(cytobandTable))
        print (cytobandTable.head())
        
        # Create bedtools from the cytoband table:
        cytobandTable_BT = pybedtools.BedTool.from_dataframe(cytobandTable)

        # Get cytobands overlapping with the genes:
        ensemblGeneBed = pybedtools.BedTool.from_dataframe(self.__ensembl_data)
        cbGeneIntersectBT = ensemblGeneBed.intersect(cytobandTable_BT, wb = True)
        print(cbGeneIntersectBT.head())

        # Extracting information:
        cbGeneIntersectDF = cbGeneIntersectBT.to_dataframe(engine='python', names = ['chr', 'start', 'end', 'geneID', 'biotype','symbol', 'chr2', 'start2', 'end2', 'cb', 'staining'])
        cbGeneIntersectDF['cytoband'] = cbGeneIntersectDF.apply(lambda row: str(row.chr) + row.cb, axis = 1)
        cbGeneIntersectDF = cbGeneIntersectDF[['geneID','cytoband']]
        print(cbGeneIntersectDF.head())

        # Collapsing rows where one gene overlaps with multiple cytobands:
        cbGeneIntersectDF = cbGeneIntersectDF.set_index('geneID')
        duplicated_indices = [ '/'.join(cbGeneIntersectDF.loc[x,'cytoband'].tolist()) for x in cbGeneIntersectDF.index[cbGeneIntersectDF.index.duplicated()]]
        consolidated_cytoband = pd.Series(duplicated_indices, index = cbGeneIntersectDF.index[cbGeneIntersectDF.index.duplicated()])
        print(len(consolidated_cytoband))

        # Adding genes with single cytobands:
        consolidated_cytoband = consolidated_cytoband.append(cbGeneIntersectDF.loc[ ~cbGeneIntersectDF.index.duplicated(keep=False),'cytoband'])
        print(len(consolidated_cytoband))

        # Saving table to pickle:
        consolidated_cytoband.to_pickle(pickle_filename)

        self.__cytobands = consolidated_cytoband
        print(self.__cytobands.head())
           
    def create_document(self, genes_extracted):
        print ("[Info] Creating document for genes.")
        
        def generate_description(gene):
            try:
                description = [
                    str(gene['ensemblDescription']),
                    "%s:%s-%s" % (gene['chromosome'],
                    gene['start'],
                    gene['end']),
                    str(gene['cytoband']),
                    str(gene['biotype'])
                ]
                return("|".join(description))
            except: 
                print(gene)


        def add_HGNC_annot(gene, annot = {'cytoband' : '', 'entrez_id' : '', 'alternativeIDs': '', 'synonyms' : ''}):
            gene['cytoband'] = annot['cytoband']
            gene['entrez_id'] = annot['entrez_id']
            gene['crossRefs'] = annot['alternativeIDs']
            gene['synonymsGene'] = annot['synonyms']

        def add_Ensembl_annot(gene, annot = {'start' : '', 'end' : '', 'seq_region_name': '',
                                            'biotype' : '', 'display_name' : '', 'description': ''}):
            gene['start'] = annot['start']
            gene['end'] = annot['end']
            gene['chromosome'] = annot['seq_region_name']
            gene['biotype'] = annot['biotype']
            gene['title']= annot['display_name']
            if 'description' in annot:
                gene['ensemblDescription'] = annot['description'].split(' [Source')[0]
            else:
                gene['ensemblDescription'] = ''

        IDsToDelete = []
        for gene_ID, gene in tqdm(genes_extracted.items(), desc = "Gene document generation"):
            # Adding general fields:
            gene['resourcename'] = 'gene'
            gene['id'] = 'gene:'+gene_ID
            gene['ensemblID'] = gene_ID

            try:
                if gene_ID in self.__gene_table.index:
                    add_HGNC_annot(gene, self.__gene_table.loc[[gene_ID]].iloc[0])
                else:
                    add_HGNC_annot(gene)
            except:
                print("[Info] %s HGNC failed" % gene_ID) 
                IDsToDelete.append(gene_ID)
            try:
                if gene_ID in self.__Ensembl_annot:
                    add_Ensembl_annot(gene, self.__Ensembl_annot[gene_ID])
                else:
                    add_Ensembl_annot(gene)
            except:
                if not self.__Ensembl_annot[gene_ID]:
                    IDsToDelete.append(gene_ID)

            gene['description'] = generate_description(gene)

        # There might be IDs in the database that no longer in Ensembl. Remove them!            
        if len(IDsToDelete) > 0:            
            print('[Info] There are %s compromised gene IDs. Removing them.' % len(IDsToDelete))
            for ID in IDsToDelete:
                del genes_extracted[ID]
        print("[Info] Gene documents are done.")
        return(list(genes_extracted.values()))

    def get_ID_lookup_table(self):
        '''
        Just returning a dictionary that maps entrez IDs to Ensembl IDs.
        '''
        return(self.__mapping_table)

    def fetch_Ensembl_annotations(self, IDs):
        '''
        Returning gene annotation for a set of stable Ensembl IDs, using the REST API.
        Input: list of IDs from the mapped genes.
        Output: object attribute with the annotation: a dictionary where the keys are the stable IDs.
        '''

        # For diagnostic purposes, we might want to save and reload the downloaded annotation:
        pickle_filename = 'data/ensembl.annotation.pkl'
        if os.path.isfile(pickle_filename):
            with open('filename.pickle', 'rb') as f:
                print("[Info] Reading Ensembl annotation from pickle...")
                self.__Ensembl_annot = pickle.load(f)
                return()

        # If we did not found the pickle file, then we have to fetch annotation.
        print("[Info] Returning annotation from Ensembl for %s genes..." % len(IDs))
        self.__Ensembl_annot = self.EnsemblREST.postID(IDs)

        # Once annotation is extracted, save it:
        with open(pickle_filename, 'wb') as f:
            pickle.dump(self.__Ensembl_annot, f, protocol=pickle.HIGHEST_PROTOCOL)




