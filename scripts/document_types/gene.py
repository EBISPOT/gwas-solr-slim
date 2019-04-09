import os.path
import pandas as pd
import pickle
from tqdm import tqdm
from document_types import gene_annotator

def get_gene_data(connection, RESTURL, limit=0, testRun = False):
    # Importing shell variables:
    try:
        HGNC_file = os.environ["HGNC_file"]
    except:
        raise(ValueError("[Error] \"HGNC_file\" shell variable is not defined."))

    try:
        EnsemblFtpPath = os.environ["EnsemblFtpPath"]
    except:
        raise(ValueError("[Error] \"EnsemblFtpPath\" shell variable is not defined."))

    # Extract gene/mapping data from database:
    geneSQL = gene_sql(connection=connection, testRun = testRun, limit = limit)
    mappedGenes = geneSQL.get_results()
    
    ## For testing purposes the mapped genes and variants can be serialized:
    # geneSQL.save_results('data/gene_mapping.pkl')
    # mappedPickleFile = 'data/gene_mapping.pkl'
    # with (open(mappedPickleFile, 'rb'))as f:
    #     mappedGenes = pickle.load(f)

    # Initialize annotator object:
    geneAnnotObj = gene_annotator.GeneAnnotator(verbose=1, RESTServer= RESTURL,
                        EnsemblFtpPath=EnsemblFtpPath, HGNCFile=HGNC_file)

    ## For testing purposes the annotator object can be serialized and re-loaded:
    # geneAnnotObj.save_data('data/gene_annotator.pkl')
    # annotatorFile = 'gene_annotator.plk'
    # with (open(annotatorFile, 'rb'))as f:
    #     geneAnnotObj = pickle.load(f)

    # Generating documents:
    geneDocuments = geneAnnotObj.create_document(mappedGenes)

    return(geneDocuments)

class gene_sql(object):
    '''
    A class to extract gene related data from the database
    '''

    # Test cases, rsIDs where the generation of the gene document might be complicated:
    testRsIds = [
        'rs11352199',
        'rs558163981',
        'rs4767902',
        'rs2739472',
        'rs4622308',
        'rs2292239',
        'rs11171739',
        'rs34379766',
        'rs10783779',
        'rs877636',
        'rs12580100',
        'rs7312770',
        'rs7302200',
        'rs890076',
        'rs116175783',
        'rs204888', # Document of the mapped gene is missing from the slim solr..
    ]
    
    # Extract associations for DYNLL1 (as a test suite):
    sql_test_query = '''SELECT A.ID as ASSOCIATION_ID, A.STUDY_ID, SNP.RS_ID
        FROM ASSOCIATION A,
          ASSOCIATION_SNP_VIEW ASV,
          SINGLE_NUCLEOTIDE_POLYMORPHISM SNP
        WHERE
          SNP.RS_ID IN (%s)
          AND SNP.ID = ASV.SNP_ID
          AND ASV.ASSOCIATION_ID = A.ID
    '''
    
    # Extract all association ID with the corresponding study ID
    sql_associatinos_studies = '''
        SELECT A.ID as ASSOCIATION_ID, A.STUDY_ID
        FROM ASSOCIATION A
        '''

    # Get all rsIDs for an association:
    sql_get_rsIDs = '''
        SELECT SNP.RS_ID, SNP.ID as SNP_ID
        FROM ASSOCIATION_SNP_VIEW ASV,
            SINGLE_NUCLEOTIDE_POLYMORPHISM SNP
        WHERE ASV.ASSOCIATION_ID = :assoc_id
          AND ASV.SNP_ID = SNP.ID
        '''

    # Get all genes for a variant:
    sql_get_genes = '''
        SELECT GCont.*, EG.ENSEMBL_GENE_ID
        FROM GENE_ENSEMBL_GENE GEG,
          ENSEMBL_GENE EG,
          (SELECT GC.SNP_ID, GC.GENE_ID, GC.IS_CLOSEST_GENE, GC.IS_INTERGENIC
              FROM GENOMIC_CONTEXT GC
              WHERE GC.SNP_ID = :snp_ID
              AND GC.SOURCE = 'Ensembl'
              AND (
              GC.IS_INTERGENIC = 0
              OR GC.IS_CLOSEST_GENE = 1
              )
          ) GCont
        WHERE GCont.GENE_ID = GEG.GENE_ID
          AND GEG.ENSEMBL_GENE_ID = EG.ID
    '''
    
    def __init__(self, connection, limit = 0, testRun = False):

        # Initialize return variables:
        self.gene_container = {}
        self.rsID_container = {}        
        self.connection = connection

        if limit == 12:
          test = True

        # We extract the list of studies and associations:
        if testRun:
            in_vars = ','.join(':%d' % i for i in range(len(self.testRsIds)))
            self.association_df = pd.read_sql(self.sql_test_query % in_vars, self.connection, params = self.testRsIds)
        else:
            self.association_df = pd.read_sql(self.sql_associatinos_studies, self.connection)
                
        tqdm.pandas(desc="Extracting mapped genes...")
        
        # Looping through all associations and return genomic context:
        if limit != 0:
            x = self.association_df.sample(n = limit).progress_apply(self.__process_association_row, axis = 1)
        else:
            x = self.association_df.progress_apply(self.__process_association_row, axis = 1)

    def get_results(self):
        for EnsemblID in self.gene_container.keys():
            self.gene_container[EnsemblID]['rsIDs'] = list(set(self.gene_container[EnsemblID]['rsIDs']))
            self.gene_container[EnsemblID]['studyID'] = list(set(self.gene_container[EnsemblID]['studyID']))
            self.gene_container[EnsemblID]['studyCount'] = len(self.gene_container[EnsemblID]['studyID'])
            
        return(self.gene_container)

    def save_results(self, filename):
        try:
            # Saving the whole object:
            current_dir = os.getcwd()
            path = os.path.join(current_dir, "data/%s" % (filename))
            dump = open(filename, 'wb')
            pickle.dump(self.gene_container, dump)
            return(0)
        except:
            print("[Warning] Saving data failed.")
            return(1)

    def __process_rsID_row(self, row):
        rsID  = str(row['RS_ID'])
        snpID = str(row['SNP_ID'])    

        # Extracting genomic context:
        if rsID in self.rsID_container:
            return([rsID, self.rsID_container[rsID]])

        genomicContext = pd.read_sql(self.sql_get_genes, self.connection, params = {'snp_ID': snpID})
        genomicContext = genomicContext.drop_duplicates()
        mappedGenes = []
        if genomicContext.IS_INTERGENIC.isin([0]).any():
            mappedGenes = genomicContext.loc[genomicContext.IS_INTERGENIC == 0, 'ENSEMBL_GENE_ID'].tolist()
        else:
            mappedGenes = genomicContext.loc[genomicContext.IS_CLOSEST_GENE == 1, 'ENSEMBL_GENE_ID'].tolist()

        return([rsID, mappedGenes])

    # Looping through all the associations and 
    def __process_association_row(self, row):
        associationID = str(row['ASSOCIATION_ID'])
        studyID = str(row['STUDY_ID'])

        # Extract rsIDs:
        rsIDs = pd.read_sql(self.sql_get_rsIDs, self.connection, params = {'assoc_id': associationID})

        # Extract mapped genes for every rsIDs in the association:
        mapped_genes = rsIDs.apply(self.__process_rsID_row, axis = 1)

        # Parsing out mapped genes:
        gene_assoc = []
        
        for index, row in mapped_genes.iteritems():
            rsID = row[0]
            genes = row[1]

            # Updating the rsID container:
            self.rsID_container[rsID] = genes

            # Adding gene to the gene container:
            for gene in genes:
                try:
                    self.gene_container[gene]['rsIDs'].append(rsID)
                    if not gene in gene_assoc:
                        self.gene_container[gene]['studyID'].append(studyID)
                        self.gene_container[gene]['associationCount'] += 1
                        self.gene_container[gene]['assocID'].append(associationID)
                        gene_assoc.append(gene)
                except:
                    self.gene_container[gene] = {
                        'rsIDs' : [rsID],
                        'studyID' : [studyID],
                        'assocID' : [associationID],
                        'associationCount' : 1
                    }
                    gene_assoc.append(gene)


