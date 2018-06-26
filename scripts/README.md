# Overview

This directory contains scripts to generate Solr documents for the GWAS Slim Solr index. 
 

## Scripts
`generate_solr_docs.py`

*Description*: Creates JSON documents representing the minimal information needed for the GWAS Slim Solr index. The documents include: Publication, Study, Variant, Trait, Gene.

*Usage*: This script should be run on the EBI file server (see Confluence for details). It can be run as:  
`python generate_solr_docs.py` 

*Output*: This will create one file for each document type in the directory "data". 

*Dependencies*: This script requires the virtual environment set-up on the EBI file server. See internal README for more details on dependencies and database connection details.

