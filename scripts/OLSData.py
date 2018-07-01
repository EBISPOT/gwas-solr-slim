import requests, json
import urllib

import DataFormatter



class OLSData:
    def __init__(self, term_iri):
        self.term_iri = term_iri

    def get_ols_term(self):
        '''
        Use OLS Term API to get details about an ontology term. 
        '''

        term_iri = self.term_iri
        term_iri_double_encoded = urllib.quote_plus(urllib.quote_plus(term_iri))

        # TODO: Handle terms from other ontologies, e.g. GO, PATO, etc
        # Parse ontology prefix from term iri


        # TODO: Make robust to the term/ontology being removed from OLS
        OLS_URL = "http://www.ebi.ac.uk/ols/api/ontologies/{ontology_prefix:s}/terms/"\
            "{term_iri:s}".format(ontology_prefix='efo',term_iri=term_iri_double_encoded)

        no_results = {'iri': None, 'synonyms': None, 'short_form': None, 'label': None}

        try:
            response = requests.get(OLS_URL)
            if response.status_code == 200:
                results = json.loads(response.content)

                if results:
                    data_formatter = DataFormatter.DataFormatter(results)
                    return (data_formatter.create_term_result_obj())

                else:
                    print "** No data returned!!!"
                    return no_results
            
            else:
                # TODO: Handle case when EFO term is not yet in production EFO and OLS
                # and OLS 500 return error
                # print "\n--> ReTry OLS...", term_iri_double_encoded, "\n"
                # get_ols_term(term_iri_double_encoded)
                return no_results
        
        except requests.exceptions.RequestException as e:
            print e


