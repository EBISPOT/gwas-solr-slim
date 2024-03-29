import sys
import requests
import json
import math
import time
from tqdm import tqdm

class REST(object):
    '''
    This class will be responsible to return data from Ensembl.
    It will be initialized by the URL of the server. So in theory it is easy to
    switch to alternative or to the GRCh37 server.
    '''

    def __init__(self, URL):
        self.URL = URL

    # https://rest.ensembl.org/info/data/?content-type=application/json
    def getEnsemblRelease(self):
        URL = ( "%s/info/data/?content-type=application/json" % self.URL)
        response = self.__get_submit(URL)
        releases = response['releases']
        releases.sort(reverse=True)
        return(releases[0])

    # https://rest.ensembl.org/ld/human/rs1042779/1000GENOMES:phase_3:EUR?content-type=application/json
    def getLD(self, rsID, population = '1000GENOMES:phase_3:EUR', window = 500):
        '''
        This function returns variants in LD with the requested variant.
        The population is by default is 1000GENOMES:phase_3:EUR, but can be specified.
        The window size by default is 500kbp.
        '''
        URL = ( "%s/ld/human/%s/%s?window_size=%s&content-type=application/json" % (self.URL,rsID,population,
                window))
        return(self.__get_submit(URL))

    # https://rest.ensembl.org/variation/human/rs56116432?content-type=application/json
    def getVariation(self, rsID, parameters = {}):
        '''
        rsID is required parameter, but param is optional
        parameters is a dictionary from which the keys and the values are keys and values for the request.
        '''

        # Compiling URL:
        URL = ( "%s/variation/human/%s?%s&content-type=application/json" % (self.URL,rsID,
                "&".join([str(x)+"="+str(parameters[x]) for x in parameters])))
        return(self.__get_submit(URL))

    # /overlap/region/human/7:140424943-140624564?feature=gene;feature=transcript;feature=cds;feature=exon;content-type=application/json
    def getOverlap(self, chromosome, start, end, features = [], parameters = {}):
        # Compiling URL:
        featurestring = ";".join(["feature="+str(x) for x in features])
        parameterstring = ";".join([str(x)+'='+str(parameters[x]) for x in parameters])
        URL = ( "%s/overlap/region/human/%s:%s-%s?%s;%s&content-type=application/json" % (self.URL,chromosome,
                start, end,  featurestring, parameterstring))
        return (self.__get_submit(URL))

    # https://rest.ensembl.org/phenotype/region/homo_sapiens/9:22125500-22136000?content-type=application/json;feature_type=Variation
    def getPhenotype(self, chromosome, start, end):
        '''
        based on the submitted window returns the variants with annotated
        phenotypes. At this point we don't care anything else but
        the variant, so no features.
        '''
        URL = ( "%s/phenotype/region/homo_sapiens/%s:%s-%s??content-type=application/json;feature_type=Variation" % (self.URL,
            chromosome, start, end))
        return (self.__get_submit(URL))

    # https://rest.ensembl.org/info/assembly/homo_sapiens?content-type=application/json&bands=1
    def getAssembly(self, feature = "all", species = "homo_sapiens"):
        '''
        This function return assembly information.
        Flexible for species and feature:
            * chromosomes: {chr_name : length, ... }
            * cytobands: [{id: <1p12>, start: 14123, end : 9898, chromosome: 1, stain: "acen"}, .... ]

        If feature name is not matched, the full dataset will be returned.
        '''

        URL = ("%s/info/assembly/%s?content-type=application/json&bands=1" %(self.URL, species))
        assemblyInfo = self.__get_submit(URL)

        # Now we have to parse the returned data:
        if feature == "all" :
            return(assemblyInfo)
        elif feature == 'chromosomes' or feature == 'cytobands':
            cytobands = []
            chromosomes = {}

            for region in assemblyInfo['top_level_region']:
                if region['coord_system'] != "chromosome":
                    continue

                # Extracting chromosome information:
                chromosomes[region['name']] = region['length']

                # Extracting cytoband informaion:
                if feature == 'chromosomes' or 'bands' not in region:
                    continue

                # Extracting cytoband info:
                for band in region['bands']:
                    cytobands.append({
                            "start" : band['start'],
                            "stain" : band['stain'],
                            "chromosome": band['seq_region_name'],
                            "id"    : band['seq_region_name'] + band['id'],
                            "end"   : band['end']
                        })

            # returning data once the read is complete:
            if feature == 'chromosomes':
                return(chromosomes)
            elif feature == 'cytobands':
                return(cytobands)

        else:
            print("[Warning] %s is an unregistered feature. Accepting 'chromosomes' or 'bands'." % feature)
            return(assemblyInfo)
    
    def postVariation(self, IDs, features = {}):
        ext = '/variation/homo_sapiens'
        return_list = {}

        # The data has to be chunked by 200:
        for i in range(int(math.ceil(len(IDs)/200.0))):
            IDs_chunk = IDs[ i * 200 : (i + 1) * 200 ]
            data = {'ids' : IDs_chunk}
            data.update(features)
            return_list.update(self.__post_submit(ext, json.dumps(data)))
        
        return(return_list)

    def postID(self, IDs, features = {}):
        '''
        Posts a list of IDs to the rEST API
        '''
        ext = '/lookup/id'
        return_list = {}
        
        # The data has to be chunked by 1000:
        for i in tqdm(range(int(math.ceil(len(IDs)/1000.0))), desc = 'Submitting IDs to Ensembl'):
            IDs_chunk = IDs[ i * 1000 : (i + 1) * 1000 ]
            data = {'ids' : IDs_chunk}
            data.update(features)
            return_list.update(self.__post_submit(ext, json.dumps(data)))

        return(return_list)

    def __post_submit(self, ext, data,
                      headers={ "Content-Type" : "application/json", "Accept" : "application/json"} ):
        '''
        Posting data to server
        '''
        max_try = 10
        current_try = 0
        response = requests.post(self.URL+ext, headers=headers, data=data)
        while not response.ok and current_try <= max_try:
            current_try += 1
            time.sleep(2)
            response = requests.post(self.URL+ext, headers=headers, data=data)

        if not response.ok:
            print ("[Error] request failed! Code: %s, Text: %s" % (response.status_code, response.text))
            print ("[Error] Reason: %s" % response.reason)
            print ("[Error] submitted data: %s" % data)
        
        try:
            return(response.json())
        except ValueError:
            print('[Error] The returned data was not a json.... ')
            print('[Info] Endpoint: ' + ext)
            print('[Info] Data: ' + data)
            print('[Info] Response: ' + response.content)
            return([])

    # Submitting request:
    def __get_submit(self, URL):
        '''
        This method needs to be improved, but let's assume everyting works just fine.
        '''
        response = requests.get(URL, headers={ "Content-Type" : "application/json"})
        if not response.ok:
            return(response.raise_for_status())
        
        try:
            return(response.json())
        except ValueError:
            print('[Error] The returned data was not a json.... ')
            print('[Info] URL: ' + URL)
            print('[Info] Response: ' + response.content)


