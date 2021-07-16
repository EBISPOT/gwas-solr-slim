import pandas as pd


class published_study(object):
    """
    Class for handling published studies
    """

    pub_study_sql = """
        SELECT S.ID, S.ACCESSION_ID, P.TITLE
        FROM STUDY S, PUBLICATION P, AUTHOR A
        WHERE S.PUBLICATION_ID=P.ID and P.FIRST_AUTHOR_ID=A.ID
    """

    # below are some currently unused sql queries - but should we want to
    # add more fields to the doc, we can use these

    study_sql = """
        SELECT S.ID, S.ACCESSION_ID, 'TODO-Title-Generation' as title, 'study' as resourcename,
        A.FULLNAME, TO_CHAR(P.PUBLICATION_DATE, 'yyyy'), P.PUBLICATION, P.PUBMED_ID, S.INITIAL_SAMPLE_SIZE,
        S.FULL_PVALUE_SET
        FROM STUDY S, PUBLICATION P, AUTHOR A
        WHERE S.PUBLICATION_ID=P.ID and P.FIRST_AUTHOR_ID=A.ID
    """

    pub_study_platform_types_sql = """
        SELECT DISTINCT S.ID, listagg(P.MANUFACTURER, ', ') WITHIN GROUP (ORDER BY P.MANUFACTURER) AS PLATFORM_TYPES
        FROM STUDY S, PLATFORM P, STUDY_PLATFORM SP
        WHERE S.ID=SP.STUDY_ID and SP.PLATFORM_ID=P.ID
            and S.ID= :study_id
        GROUP BY S.ID
    """


    pub_study_ancestral_groups_sql = """
        SELECT  x.ID, listagg(x.ANCESTRAL_GROUP, ', ') WITHIN GROUP (ORDER BY x.ANCESTRAL_GROUP)
        FROM (
                SELECT DISTINCT S.ID, AG.ANCESTRAL_GROUP
                FROM STUDY S, ANCESTRY A, ANCESTRY_ANCESTRAL_GROUP AAG, ANCESTRAL_GROUP AG
                WHERE S.ID=A.STUDY_ID and A.ID=AAG.ANCESTRY_ID and AAG.ANCESTRAL_GROUP_ID=AG.ID
                    and S.ID= :study_id
            ) x
        GROUP BY x.ID
    """

    pub_study_reported_trait_sql = """
        SELECT DISTINCT S.ID, listagg(DT.TRAIT, ', ') WITHIN GROUP (ORDER BY DT.TRAIT)
        FROM STUDY S, STUDY_DISEASE_TRAIT SDT, DISEASE_TRAIT DT
        WHERE S.ID=SDT.STUDY_ID and SDT.DISEASE_TRAIT_ID=DT.ID
              and S.ID= :study_id
        GROUP BY S.ID
    """

    pub_study_mapped_trait_sql = """
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

    def __init__(self, connection, limit = 0):
        self.connection = connection

    def get_study_data(self):
        self.study_df = pd.read_sql(self.pub_study_sql, self.connection)
        self.study_df.rename(columns={'ID': 'id', 'TITLE': 'title', 'ACCESSION_ID': 'accessionId'}, inplace=True)
        return self.study_df


class unpublished_study():
    """
    Class for handling unpublished studies
    """

    unpub_study_sql = """
        SELECT S.ID, S.ACCESSION, B.TITLE
        FROM UNPUBLISHED_STUDY S, BODY_OF_WORK B, UNPUBLISHED_STUDY_TO_WORK J
        WHERE S.ID = J.STUDY_ID 
        AND J.WORK_ID = B.ID
        """

    def __init__(self, connection, limit = 0):
        self.connection = connection

    def get_study_data(self):
        self.study_df = pd.read_sql(self.unpub_study_sql, self.connection)
        self.study_df.rename(columns={'ID': 'id', 'TITLE': 'title', 'ACCESSION': 'accessionId'}, inplace=True)
        return self.study_df


def get_study_data(connection, limit=0):
    published_study_df = published_study(connection=connection).get_study_data()
    unpublished_study_df = unpublished_study(connection=connection).get_study_data()
    study_df = published_study_df.append(unpublished_study_df, ignore_index=True)
    study_df['resourcename'] = 'study'
    study_df['description'] = study_df['accessionId']
    study_document = study_df.to_dict('records')
    return study_document