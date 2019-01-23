class DataFormatter:
    """
    Format results into JSON for further analysis.
    """

    def __init__(self, data):
        self.data = data


    def get_term_information(self, type):
        term_result = self.data

        results_list = []
        result_obj = {}

        keys = term_result.keys()
        
        label_key = "label"
        iri_key = "iri"
        synonyms_key = "synonyms"
        short_form_key = "short_form"
        definition_key = "description"
        ancestors = "ancestors"
        descendants = "descendants"


        if label_key in keys:
            result_obj[label_key] = term_result[label_key]
        if iri_key in keys:
            result_obj[iri_key] = term_result[iri_key]
        if synonyms_key in keys:
            result_obj[synonyms_key] = term_result[synonyms_key]
        if short_form_key in keys:
            result_obj[short_form_key] = term_result[short_form_key]
        if definition_key in keys:
            result_obj[definition_key] = term_result[definition_key]

        # Get link for ancestors
        if type == 'ancestors':
            result_obj[ancestors] = term_result['_links']['ancestors']['href']
        
        # Get link for descendants
        if type == 'descendants':
            if 'descendants' in term_result['_links'].keys():
                result_obj[descendants] = term_result['_links']['descendants']['href']
            else:
                # print "** No descendants"
                pass

        results_list.append(result_obj)

        return result_obj


    def get_ancestor_labels(self):
        '''
        Parse and return only term labels from the OLS "ancestors" link.
        '''

        ancestors = self.data

        ancestor_labels = []

        for term in ancestors['_embedded']['terms']:
            # TODO: Do not include ontology metadata 
            # terms, e.g. Thing, disposition, experimental factor ontology
            ancestor_labels.append(term['label'])

        return ancestor_labels



    def get_descendant_ids(self):
        '''
        Parse and return only term ids from the OLS "descendants" link.

        '''

        descendants = self.data

        descendant_ids = []

        for term in descendants['_embedded']['terms']:
            # TODO: Do not include ontology metadata 
            # terms, e.g. Thing, disposition, experimental factor ontology
            descendant_ids.append(term['short_form'])

        return descendant_ids



