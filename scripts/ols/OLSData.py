import requests, json
import urllib

from scripts.constants import OLS4_BASE_URL, ONTOLOGY_PREFIX, TERMS_PREFIX
from scripts.ols import DataFormatter

class OLSData:
    def __init__(self, term_iri):
        self.term_iri = term_iri

    
    def get_ols_term(self, type):
        '''
        Use OLS Term API to get details about an ontology term. 
        '''

        term_iri = self.term_iri
        term_iri_double_encoded = urllib.parse.quote_plus(urllib.parse.quote_plus(term_iri))

        # TODO: Make robust to the term/ontology being removed from OLS
        OLS_URL = f"{OLS4_BASE_URL}/{ONTOLOGY_PREFIX}/{TERMS_PREFIX}/{term_iri_double_encoded}"

        no_results = {'iri': None, 'synonyms': None, 'short_form': None, 'label': None, 'description': None}

        try:
            response = requests.get(OLS_URL)
            if response.status_code == 200:
                results = json.loads(response.content)

                if results:
                    data_formatter = DataFormatter.DataFormatter(results)
                    return (data_formatter.get_term_information(type))

                else:
                    return no_results
            
            else:
                # TODO: Handle case when EFO term is not yet in production EFO and OLS
                # and OLS 500 return error
                # print "\n--> ReTry OLS...", term_iri_double_encoded, "\n"
                # get_ols_term(term_iri_double_encoded)
                return no_results
        
        except requests.exceptions.RequestException as e:
            print(e)


    def get_ancestors(self):
        '''
        Use OLS to get ancestors for a term. NOTE: This link is from the "term"
        web service and is already URL double-encoded.
        '''

        OLS_ANCESTOR_URL = self.term_iri
        no_results = {'iri': None, 'synonyms': None, 'short_form': None, 'label': None, 'description': None}

        no_ancestor_results = []

        try:
            response = requests.get(OLS_ANCESTOR_URL)
            if response.status_code == 200:
                results = json.loads(response.content)

                if results:
                    data_formatter = DataFormatter.DataFormatter(results)
                    return (data_formatter.get_ancestor_labels())

                else:
                    # print "** No data returned!!!"
                    return no_ancestor_results
            
            else:
                return no_results
        
        except requests.exceptions.RequestException as e:
            print(e)



    def get_hierarchicalDescendants(self, page=0):
        '''
        Use OLS to get hierarchicalDescendants for a term. NOTE: This link is from the "descendants"
        web service and is already URL double-encoded.
        '''

        no_results = {'iri': None, 'synonyms': None, 'short_form': None, 'label': None, 'description': None}

        OLS_DESCENDANT_URL = self.term_iri+"?size=1000&page={}".format(page)

        no_descendant_results = []
        all_descendants = []

        try:
            response = requests.get(OLS_DESCENDANT_URL)
            if response.status_code == 200:
                results = json.loads(response.content)

                if results:
                    data_formatter = DataFormatter.DataFormatter(results)
                    total_pages = data_formatter.get_pages()

                    if total_pages == 1:
                        return (data_formatter.get_hierarchicalDescendants_ids())
                    else:
                        while page < total_pages:                            
                            efo_ids = OLSData.__get_pages(self, page)
                            all_descendants.extend(efo_ids)

                            page += 1

                    return all_descendants
                else:
                    return no_descendant_results
            
            else:
                return no_results
        
        except requests.exceptions.RequestException as e:
            print(e)


    def __get_pages(self, page):
        '''
        Get pages of data.
        '''

        no_results = {'iri': None, 'synonyms': None, 'short_form': None, 'label': None, 'description': None}
        
        OLS_DESCENDANT_URL = self.term_iri+"?size=1000&page={}".format(page)

        try:
            response = requests.get(OLS_DESCENDANT_URL)
            if response.status_code == 200:
                results = json.loads(response.content)

                data_formatter = DataFormatter.DataFormatter(results)
                child_ids = data_formatter.get_hierarchicalDescendants_ids()
                return child_ids
            else:
                return no_results
        except requests.exceptions.RequestException as e:
            print(e)



