from setuptools import find_packages, setup

setup(
    name='gwas-solr-slim',
    description='GWAS solr slim document generator',
    version='2.0.2',
    packages=['.','scripts','scripts.document_types','scripts.EnsemblREST','scripts.ols'],
    include_package_data=True,
    license='Apache License, Version 2.0',
    entry_points={
        "console_scripts": ['generate-solr-docs = scripts.generate_solr_docs:main',
                            'solr-update-validate = solrUpdateValidate:main']
    },
    url='https://github.com/EBISPOT/gwas-solr-slim',
    author='EBI SPOT',
    author_email='gwas-info@ebi.ac.uk',
install_requires=['pandas>=0.23.4',
                  'cx-Oracle>=5.3',
                  'numpy>=1.15.4',
                  'tqdm>=4.23.4',
                  'urllib3>=1.22'
                 ]
)
