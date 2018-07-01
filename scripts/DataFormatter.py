class DataFormatter:
    """
    Format results into JSON for further analysis.
    """

    def __init__(self, data):
        self.data = data


    def create_term_result_obj(self):
        term_result = self.data

        results_list = []
        result_obj = {}

        keys = term_result.keys()
        
        label_key = "label"
        iri_key = "iri"
        synonyms_key = "synonyms"
        short_form_key = "short_form"


        if label_key in keys:
            result_obj[label_key] = term_result[label_key]
        if iri_key in keys:
            result_obj[iri_key] = term_result[iri_key]
        if synonyms_key in keys:
            result_obj[synonyms_key] = term_result[synonyms_key]
        if short_form_key in keys:
            result_obj[short_form_key] = term_result[short_form_key]
        
        results_list.append(result_obj)

        return result_obj