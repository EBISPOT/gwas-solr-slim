class DataFormatter:
    """
    Format results into JSON for further analysis.
    """

    def __init__(self, attr_type, num_results, data):
        self.attr_type = attr_type
        self.num_results = num_results
        self.data = data


    def create_term_result_obj(self):
        formatted_attr_type = self.attr_type.lower()
        formatted_attr_type = formatted_attr_type.replace(" ", "_")

        value_obj = {}
        results_list = []
        
        for term_result in self.data:
            result_obj = {}

            keys = term_result.keys()
            # op_key = "ontology_prefix"
            label_key = "label"
            iri_key = "iri"
            synonym_key = "synonym"

            if op_key in keys:
                result_obj[op_key] = term_result[op_key]
            if label_key in keys:
                result_obj[label_key] = term_result[label_key]
            if iri_key in keys:
                result_obj[iri_key] = term_result[iri_key]
            if synonym_key in keys:
                result_obj[synonym_key] = term_result[synonym_key]
            
            results_list.append(result_obj)

        value_obj["results"] = results_list

        return value_obj
