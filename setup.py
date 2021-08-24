from setuptools import find_packages, setup

setup(
    name='gwas-solr-slim',
    description='GWAS solr slim document generator',
    version='0.0.1',
    packages=find_packages(include=['scripts.*']),
    include_package_data=True,
    license='Apache License, Version 2.0',
    entry_points={
        "console_scripts": ['generate-solr-docs = scripts.generate_solr_docs:main']
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