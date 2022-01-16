import setuptools

install_pckgs = [
    'pandas',
    'beautifulsoup4',
    'PyYAML',
    'IMDbPY',
    'google-api-core',
    'google-api-python-client'
]

setuptools.setup(
    include_package_data=True,
    name='etl',
    version='0.0.1',
    description='etl python module',
    url='https://somedummysite.com/package',
    author='FMirani',
    author_email='f@mirani.com',
    packages=setuptools.find_packages(),
    install_requires=install_pckgs,
    long_description='FMirani ETL project',
    long_description_content_type="text/markdown"
)
