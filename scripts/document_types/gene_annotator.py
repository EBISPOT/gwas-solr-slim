import subprocess
import numpy as np
import pandas as pd
import os
import sys
import re
import os.path
import pickle
import urllib

import io
from tqdm import tqdm
import signal

sys.path.append('scripts/EnsemblREST')
from REST import REST

class GeneAnnotator(object):
    '''
    This class collects gene annotations from various sources then generates gene solr objects.
    '''
    
    def __init__(self, HGNCFile, EnsemblFtpPath, RESTServer, verbose = False):
        # Setting verbosity flag:
        self.__verbose = verbose

        # Initialize handler for the REST API of Ensembl:
        try:
            self.EnsemblREST = REST(RESTServer)
        except:
            raise("[Error] The initialization of the REST handler for Ensembl failed.")

        # Detecting the actual ensembl release:
        self.__Ensembl_release = self.EnsemblREST.getEnsemblRelease()

        # Validating input files:
        self.__input_file_validator(HGNCFile,EnsemblFtpPath)
        
        # Report:
        if self.__verbose:
            print("[Info] Initializing gene annotator.")
            print("[Info] REST API URL: %s" % RESTServer )
            print("[Info] Ensembl version: e%s" % self.__Ensembl_release)
            print('[Info] Input file validation passed:')
            print("[Info]     HGNC file: %s" % self.__HGNCFile)
            print("[Info]     Entrez mapping file: %s" % self.__EntrezFile)
            print("[Info]     Ensembl gene annotation file: %s\n" % self.__EnsemblFile)
            print("[Info] Fetching annotation...")

        # Gatering annotation data:
        self.__get_Ensembl_gene_annotation() # Extracting Ensembl data
        self.__get_cytoband() # Extracting cytoband data from REST
        self.__get_Entrez_lookup_table() # Extracting cytoband data from REST
        self.__get_HGNC() # Extracting HGNC names and synonyms

        # Reporting completion:
        if self.__verbose:
            print("[Info] Gene annotation information compiled. Ready to create documents.\n")

    def __input_file_validator(self, HGNCFile, EnsemblFtpPath):

        # Testing if HGNC file exists:
        if not os.path.isfile(HGNCFile):
            raise(ValueError("[Error] HGNC file (%s) does not exist." % HGNCFile))
        else:
            self.__HGNCFile = HGNCFile

        # Testing if Entrez directory:
        if not os.path.exists(EnsemblFtpPath):
            raise(ValueError("[Error] The provided Ensembl ftp path (%s) does not exist." % EnsemblFtpPath))
        else :
            EntrezFile = ("%s/release-%s/tsv/homo_sapiens/Homo_sapiens.GRCh38.%s.entrez.tsv.gz" % (EnsemblFtpPath, self.__Ensembl_release, self.__Ensembl_release))
            EnsemblFile = ("%s/release-%s/gff3/homo_sapiens/Homo_sapiens.GRCh38.%s.chr.gff3.gz" % (EnsemblFtpPath, self.__Ensembl_release, self.__Ensembl_release))

        # Testing if Entrez file exists:
        if not os.path.isfile(EntrezFile):
            raise(ValueError("[Error] Entrez file (%s) does not exist." % EntrezFile))
        else:
            self.__EntrezFile = EntrezFile
        
        # Testing if Ensembl gene file exists:
        if not os.path.isfile(EnsemblFile):
            raise(ValueError("[Error] Ensembl file (%s) does not exist." % EnsemblFile))
        else:
            self.__EnsemblFile = EnsemblFile

        if self.__verbose : print('[Info] Input file validation passed.')

    def __input_data_validator(self, inputData):

        # Testing if input a dictionary:
        if not isinstance(inputData,dict):
            raise(ValueError("[Error] Input is not a dictionary."))

        # Test if the keys are Ensembl stable IDs:
        test_key = np.random.choice(list(inputData.keys()))
        if re.match('ENSG\d+', test_key) is None: 
            raise(ValueError("[Error] A selected key (%s) is not a gene stable ID." % test_key))

        # Test if values are dictionary:
        if not isinstance(inputData[test_key],dict):
            raise(ValueError("[Error] The values of the input is not a dictionary."))

        # Test if the values contain the required fields:
        requiredFields = ['associationCount', 'rsIDs', 'studyCount']
        for field in requiredFields:
            if not field in inputData[test_key]:
                raise(ValueError("[Error] %s field is missing from the dictionary." % field))

        if self.__verbose : print('[Info] Input data validation passed.')

    def __get_Ensembl_gene_annotation(self):

        # list with all the genes and the available annotation:
        EnsemblAnnotationContainer = []

        def parse_annotation(row):
            fields = row.split("\t")
            
            if len(fields) < 8: 
                print(row)
                return()

            # Extracting coordinates:
            geneAnnot = {
                'seq_region_name' : fields[0],
                'start' : fields[3],
                'end' : fields[4],
            }
            
            # Parse description column of the gff file:
            for annot in fields[8].split(";"):
                key, value = annot.split("=")
                if key == 'ID':
                    geneAnnot['id'] = value.split(":")[1]
                if key == 'Name':
                    geneAnnot['display_name'] = value
                if key == 'biotype':
                    geneAnnot['biotype'] = value
                if key == 'description':
                    geneAnnot['description'] = urllib.unquote(value.split(" [")[0])
                else:
                    continue
            
            # Adding annotation to the container
            return(geneAnnot)

        # Using bash to filter the gff3 file with 2.8M lines
        filteredLines = subprocess.Popen(['zgrep', 'ID=gene', self.__EnsemblFile],stdout=subprocess.PIPE)

        # Processing output:
        for line in iter(filteredLines.stdout.readline,''):
            EnsemblAnnotationContainer.append(parse_annotation(line.rstrip()))
   
        # Pooling annotations into a pandas dataframe and format table:
        EnsAnnotDf = pd.DataFrame.from_records(EnsemblAnnotationContainer)

        # Setting start and end columns as integers:
        EnsAnnotDf.start = EnsAnnotDf.start.astype(int)
        EnsAnnotDf.end = EnsAnnotDf.end.astype(int)

        # Filtering df to only a selected list of columns:
        EnsAnnotDf = EnsAnnotDf[['seq_region_name','start', 'end', 'id', 'display_name', 'biotype', 'description']].sort_values(['seq_region_name', 'start'])
        
        # Reindex:
        EnsAnnotDf.index = EnsAnnotDf.id.tolist()
        
        if self.__verbose: print("[Info] Ensembl annotations are extracted for %s genes" % len(EnsAnnotDf))
        
        self.__ensembl_data = EnsAnnotDf

    def __get_HGNC(self):
        '''
        Extract HGNC data from /nfs. Extracting relevant columns and formats

        1. Select certain fields: alternative names/synonyms + cros ref IDs
        2. Pool these names together + ensembl ID and entrez ID are separate columns.
        3. Adding dataframe to object.
        '''

        # Report:
        if self.__verbose: print("[Info] Retrieving HGNC dataset from %s" % self.__HGNCFile )

        df = pd.read_table(self.__HGNCFile, dtype = str)
        
        def concatenate(x):
            row = x.loc[~ x.isna()].tolist()
            return("|".join([str(x) for x in row]))

        # Adding other IDs:
        df['alternativeIDs'] = df[['hgnc_id', 'vega_id', 'ucsc_id', 
                                   'ena', 'refseq_accession', 'ccds_id', 
                                   'mgd_id', 'uniprot_ids']].apply(concatenate, axis=1)

        # Adding alternative names and synonyms:
        df['synonyms'] = df[['symbol', 'alias_symbol', 
                             'alias_name','prev_symbol', 'prev_name']].apply(concatenate, axis=1)

        # Filtering columns:
        df = df[['entrez_id', 'ensembl_gene_id', 'alternativeIDs', 'synonyms']]
        
        # Remove duplicates:
        df.drop_duplicates(inplace=True)
        
        # Adding Ensembl IDs as index:
        df.index = df.ensembl_gene_id.tolist()

        # Report:
        if self.__verbose: print("[Info] Number of genes in the HGNC dataset: %s" % len(df))
        
        # Saving data:
        self.__HGNC_data = df

    def __get_Entrez_lookup_table(self):
        
        if self.__verbose: print("[Info] Generate Entrez/Ensembl lookup table.")
        

        # Read file into a dataframe and filter:
        entrezdf = pd.read_csv(self.__EntrezFile, sep ="\t", compression='infer')
        entrezdf = entrezdf[['gene_stable_id', 'xref']].sort_values(['gene_stable_id']).drop_duplicates()
        entrezdf.reset_index(drop=True, inplace= True)
        entrezdf.index = entrezdf.gene_stable_id.tolist()
        
        self.__Entrez_lookup = entrezdf

    def __get_cytoband(self):

        if self.__verbose: print("[Info] Extracting cytobands from Ensembl REST...")

        # Extracting cytobands from Ensembl REST API:
        cytobands = self.EnsemblREST.getAssembly('cytobands')
        cytobands_df = pd.DataFrame(cytobands)
        cytobands_df = cytobands_df[['chromosome', 'start', 'end', 'id', 'stain']].sort_values(by = ['chromosome', 'start'])
        self.__cytobands = cytobands_df

        if self.__verbose: print("[Info] Number of cytobands: %s" % len(self.__cytobands))

    def create_document(self, inputData):
        print ("[Info] Creating gene documents.")

        # Testing the input:
        self.__input_data_validator(inputData)
            
        def generate_description(gene):
            try:
                description = [
                    str(gene['ensemblDescription']),
                    "%s:%s-%s" % (gene['chromosomeName'],
                    gene['chromosomeStart'],
                    gene['chromosomeEnd']),
                    str(gene['cytobands']),
                    str(gene['biotype'])]
                return("|".join(description))
            except: 
                print("[Warning] description generation was failed for %s" % gene['id'])
                return("NA")

        # Extracting data from HGNC dataFrame:
        def add_Entrez_id(gene, annot = {'gene_stable_id': '', 'xref' : ''}):
            gene['entrez_id'] = annot['xref']

        # Adding entrez ID to the document:
        def add_HGNC_annot(gene, annot = {'entrez_id' : '', 'alternativeIDs': '', 'synonyms' : ''}):
            gene['crossRefs'] = annot['alternativeIDs']
            gene['synonymsGene'] = annot['synonyms']    

        def add_Ensembl_annot(gene, annot = {'start' : '', 'end' : '', 'seq_region_name': '',
                                            'biotype' : '', 'display_name' : '', 'description': ''}):
            gene['chromosomeStart'] = annot['start']
            gene['chromosomeEnd'] = annot['end']
            gene['chromosomeName'] = annot['seq_region_name']
            gene['biotype'] = annot['biotype']
            gene['title']= annot['display_name']
            if not 'description' in annot:
                gene['ensemblDescription'] = 'No description available'
            elif isinstance(annot['description'], float):
                gene['ensemblDescription'] = 'No description available'
            else:
                gene['ensemblDescription'] = str(annot['description']).split(' [Source')[0]                

        def get_cytoband(gene, cytoband):
            chrom = gene['chromosomeName']
            start = gene['chromosomeStart']
            end = gene['chromosomeEnd']

            bands = cytoband.loc[(cytoband.chromosome == str(chrom)) 
                                 & (cytoband.start <= int(start)) 
                                 & (cytoband.end >= int(end)),'id'].tolist()
            gene['cytobands'] = "/".join(bands)      

        GeneDocuments = []
        
        # Checking which IDs are missing from the gff file:
        missingIDs = [x for x in list(inputData.keys()) if not self.__ensembl_data.index.isin([x]).any()]
        missing_annotation = {}
        if len(missingIDs) > 0: 
            print("[Warning] %s IDs are missing from the gff file. Downloading annotation from REST." % len(missingIDs))
            missing_annotation = self.EnsemblREST.postID(missingIDs)
            print("[Info] Annotation for %s genes downloaded successfuly." % len(missing_annotation))
            
        
        for gene_ID, x in tqdm(inputData.items(), desc = "Gene document generation"):

            # Initializing gene document with basic stuffs:
            gene = {
                'resourcename' : 'gene',
                'id' : 'gene:' + gene_ID,
                'ensemblID' : gene_ID,
                'rsIDs' : x['rsIDs'],
                'studyCount' : x['studyCount'],
                'associationCount' : x['associationCount']
                 }

            # Adding Ensembl annotation:
            try:    
                add_Ensembl_annot(gene, self.__ensembl_data.loc[[gene_ID]].iloc[0])
            except:
                # If the gene was not found in the 
                try:
                    add_Ensembl_annot(gene, missing_annotation[gene_ID])
                except:
                    print("[Warning] Ensembl annotation has failed for %s" % gene_ID)

            # Keeping only genes that are mapped to the chromosomes. Patches are excluded.
            if len(gene['chromosomeName']) > 2 : 
                continue

            # Adding HGNC annotation:
            try:
                add_HGNC_annot(gene, self.__HGNC_data.loc[[gene_ID]].iloc[0])
            except:
                add_HGNC_annot(gene)
                
            # Adding entrez ID:
            try:
                add_Entrez_id(gene, self.__Entrez_lookup.loc[[gene_ID]].iloc[0])
            except:
                #print("[Index] Entrez ID was not found for %s" % gene_ID)
                add_Entrez_id(gene)

            # Adding entrez ID:
            try:
                get_cytoband(gene, self.__cytobands)
            except:
                print("[Warning] cytobands were not found for %s" % gene_ID)
                gene['cytobands'] = '-'

            # Adding formatted description:
            gene['description'] = generate_description(gene)

            # Adding gene to gene document:
            if gene == None:
                print(x)
                print(gene_ID)

            GeneDocuments.append(gene)

        print("[Info] Gene documents are done.")
        return(GeneDocuments)

    def export_data(self, data = '-'):
        if data == 'Ensembl_annot':
            return(self.__ensembl_data)
        elif data == 'cytoband':
            return(self.__cytobands)
        elif data == 'HGNC':
            return(self.__HGNC_data)
        elif data == 'entrez':
            return(self.__Entrez_lookup)
        else:
            print('[Info] Available fields to return: Ensembl_annot, cytoband, HGNC, entrez')

    def save_data(self, filename):
        try:
            # Saving the whole object:
            current_dir = os.getcwd()
            path = os.path.join(current_dir, "data/%s" % (filename))
            dump = open(filename, 'wb')
            pickle.dump(self, dump)
            return(0)
        except:
            print("[Warning] Saving data has failed.")
            return(1)