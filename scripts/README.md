# Overview

This directory contains scripts to generate Solr documents for the GWAS Slim Solr index. 
 

## Scripts
`generate_solr_docs.py`

*Description*: Creates JSON documents representing the minimal information needed for the GWAS Slim Solr index. The documents include: Publication, Study, Variant, and Trait. A Gene document is new and may not be included in the MVP version of the new GWAS Search site.

*Usage*: This script should be run on the EBI file server (see the (GWAS Confluence)[] for details). It can be run as:  
`python generate_solr_docs.py --help` to see additional parameters (database and data type) to use when running the script

*Output*: This will create one file for each document data type in the directory "./data". The fields in each document are specified in the [GWAS Catalog - New Solr specification](https://docs.google.com/document/d/1i7eDTVJwvdCOcL5Rptbg4B-vYJ2LX35AZyfaRZRsLb8/edit#)

*Dependencies*: This script requires the virtual environment set-up on the EBI file server. See internal [README](https://www.ebi.ac.uk/seqdb/confluence/pages/viewpage.action?spaceKey=GOCI&title=GWAS+Solr+Slim) for more details on dependencies and database connection details.

