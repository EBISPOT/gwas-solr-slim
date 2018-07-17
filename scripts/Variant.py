
import pandas as pd

class Variant(object):
    '''
    Retrieve Variant data. 
    '''

    ### The following class variables are stores the sql queries:
    snp_sql = """
        SELECT SNP.ID, SNP.RS_ID, SNP.FUNCTIONAL_CLASS, 'variant' as resourcename
        FROM SINGLE_NUCLEOTIDE_POLYMORPHISM SNP
    """
    snp_location_sql = """
        SELECT L.CHROMOSOME_NAME, L.CHROMOSOME_POSITION, R.NAME
        FROM LOCATION L, SNP_LOCATION SL, SINGLE_NUCLEOTIDE_POLYMORPHISM SNP, REGION R
        WHERE L.ID = SL.LOCATION_ID and SL.SNP_ID = SNP.ID and L.REGION_ID = R.ID
            and length(l.CHROMOSOME_NAME) < 3 and SNP.ID = :snp_id
    """

    genomic_context_sql = """
        SELECT G.GENE_NAME, GC.GENE_ID, GC.IS_DOWNSTREAM, GC.IS_UPSTREAM, GC.IS_INTERGENIC, GC.IS_CLOSEST_GENE, GC.DISTANCE,
          L.CHROMOSOME_NAME as CHR, L.CHROMOSOME_POSITION as POS
        FROM GENOMIC_CONTEXT GC, GENE G, LOCATION L
        WHERE  GC.SNP_ID = :SNP_ID AND G.ID = GC.GENE_ID AND L.ID = GC.LOCATION_ID AND length(L.CHROMOSOME_NAME) < 3
    """

    association_count_sql = """
        SELECT asv.SNP_ID, COUNT(asv.ASSOCIATION_ID) AS count
        FROM ASSOCIATION_SNP_VIEW asv
        WHERE asv.SNP_ID = :snp_id
        group by asv.SNP_ID
    """

    ensembl_entr_ID_map_sql = """
        SELECT ENS.GENE_NAME as ENS_NAME, ENS.ENSEMBL_ID, ENTR.GENE_NAME as ENT_NAME, ENTR.ENTREZ_ID FROM
          (SELECT G.GENE_NAME as GENE_NAME, E.ENSEMBL_GENE_ID as ENSEMBL_ID
           FROM GENE G, GENE_ENSEMBL_GENE GE, ENSEMBL_GENE E
           WHERE G.ID = GE.GENE_ID AND GE.ENSEMBL_GENE_ID = E.ID) ENS
        FULL OUTER JOIN ( SELECT G.GENE_NAME as GENE_NAME, EN.ENTREZ_GENE_ID as ENTREZ_ID
        FROM GENE G, ENTREZ_GENE EN, GENE_ENTREZ_GENE GEN
        WHERE G.ID = GEN.GENE_ID AND GEN.ENTREZ_GENE_ID = EN.ID
        ) ENTR
        ON ENS.GENE_NAME = ENTR.GENE_NAME
    """

    def __init__(self, connection):
        self.connection = connection


        # We extract the mapping table:
        gene_map_df = pd.read_sql(self.ensembl_entr_ID_map_sql, self.connection)
        gene_map_df['GENE_NAME'] = gene_map_df['ENS_NAME']
        gene_map_df['GENE_NAME'][pd.isnull(gene_map_df['ENS_NAME'])] = gene_map_df[pd.isnull(gene_map_df['ENS_NAME'])]['ENT_NAME']
        self.gene_map_df = gene_map_df[['GENE_NAME', 'ENSEMBL_ID', 'ENTREZ_ID']]

    def get_snps(self):
        return pd.read_sql(self.snp_sql, self.connection)

    def get_variant_location(self, variant_id):
        location = {'chromosome' : 'NA', 'position' : 'NA', 'region' : 'NA'}
        location_df = pd.read_sql(self.snp_location_sql, self.connection, params = {'snp_id': variant_id})
        if len(location_df) > 0:
            location['chromosome'] = location_df['CHROMOSOME_NAME'].tolist()[0]
            location['position'] = location_df['CHROMOSOME_POSITION'].tolist()[0]
            location['region'] = location_df['NAME'].tolist()[0]

        return(location)

    def get_association_count(self, variant_id):
        assoc_count = '0'
        df = pd.read_sql(self.association_count_sql, self.connection, params = {'snp_id': variant_id})
        if len(df) > 0:
            assoc_count = df.COUNT.tolist()[0]
        return(assoc_count)

    def get_mapped_genes(self, variant_id):

        df = pd.read_sql(self.genomic_context_sql, self.connection, params = {'snp_id': variant_id})

        # Get a list overlapping genes:
        mappedGenes = []
        mappedGenes = df[df.IS_INTERGENIC == 0].GENE_NAME.unique().tolist()
        
        # If there's no overlapping gene, let's get the two closest genes:
        if not mappedGenes:
            mappedGenes += df[(df.IS_UPSTREAM == 1) & (df.DISTANCE == df[df.IS_UPSTREAM == 1].DISTANCE.min())].GENE_NAME.tolist()
            mappedGenes += df[(df.IS_DOWNSTREAM == 1) & (df.DISTANCE == df[df.IS_DOWNSTREAM == 1].DISTANCE.min())].GENE_NAME.tolist()
            mappedGenes = list(set(mappedGenes))

        # If the array is still empty, the variant is considered intergenic:
        if not mappedGenes: 
           mappedGenes.append('intergenic')
        else:
            mappedGenes = self.gene_map_df.loc[self.gene_map_df.GENE_NAME.isin(mappedGenes)].apply(lambda row: "|".join(filter(None, row.tolist())), axis = 1).tolist()

        # Return mapped genes:
        return(mappedGenes)

