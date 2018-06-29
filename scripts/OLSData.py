import requests, json
import urllib

import DataFormatter



class OLSData:
    def __init__(self, search_term):
        self.search_term = search_term

    def get_ols_results(self):
        '''
        Use OLS Search API to get details about ontology terms.
        '''
        search_value = self.search_term

        params = "exact=true&fieldList=iri,label,short_form,obo_id,ontology_name,ontology_prefix,description,type,synonym"

        search_value = urllib.quote(search_value)
        OLS_URL = " http://www.ebi.ac.uk/ols/api/search?q={search_value:s}&ontology=efo&" \
                    "{params}".format(search_value=search_value, params=params)

        print "*** Searching with: ", search_value
        print "*** OLS_URL: ", OLS_URL


        try:
            response = requests.get(OLS_URL)
            if response.status_code == 200:
                results = json.loads(response.content)
                if results:
                    num_results = results["response"]["numFound"]
                    # print "** NumResults: ", num_results
                    if num_results == 0:
                        # TODO: Handle this as an error case
                        return (num_results,"None")

                    elif num_results == 1:
                        terms_found = results["response"]["docs"]
                        data_formatter = DataFormatter.DataFormatter(search_value, num_results, terms_found)
                        return (num_results, data_formatter.create_term_result_obj())

                else:
                    # print "** No reponse!!!"
                    # csvout.writerow(["none", "none"])
                    pass
            else:
                print "\n--> ReTry OLS...", search_value, params, "\n"
                get_ols_results(search_value, params)
        
        except requests.exceptions.RequestException as e:
            print e


