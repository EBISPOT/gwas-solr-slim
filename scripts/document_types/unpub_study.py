import cx_Oracle
import contextlib
from tqdm import tqdm


def get_unpub_study_data(connection, limit=0):
    '''
    Get unpublished study data for Solr document.
    '''

    # List of queries
    unpub_study_sql = """
        SELECT S.ID, S.ACCESSION, B.TITLE
        FROM UNPUBLISHED_STUDY S, BODY_OF_WORK B, UNPUBLISHED_STUDY_TO_WORK J
        WHERE S.ID = J.STUDY_ID 
        AND J.WORK_ID = B.ID
        """

    all_unpub_study_data = []

    try:
        with contextlib.closing(connection.cursor()) as cursor:
            cursor.execute(unpub_study_sql)
            unpub_study_data = cursor.fetchall()

            for study in tqdm(unpub_study_data, desc='Get unpubublished study data'):
                # Data object for each Study
                unpub_study_document = {}

                # Add items to Study document
                unpub_study_document['id'] = study[0]
                unpub_study_document['accessionId'] = study[1]
                unpub_study_document['title'] = study[2]
                unpub_study_document['resourcename'] = 'unpublished_study'
                unpub_study_document['description'] = study[2]


                all_unpub_study_data.append(unpub_study_document)

        return all_unpub_study_data

    except cx_Oracle.DatabaseError as e:
        print(e)



