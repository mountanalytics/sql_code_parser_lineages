from setuptools import setup, find_packages

setup(
    name='sql_parser',
    version='0.1',
    packages=find_packages(include=['sql_parser', 'sql_parser.*']),  # Only include the sankeyapp package
    #install_requires=[
    #    'pandas'
    #    'sankeyapp @ git+https://github.com/mountanalytics/sankey-plot-lineages.git@tsql'
    #    "sqlglot @ git+https://github.com/KaiserM11/sqlglot_ma_Transformations.git",
    #],
    entry_points={
        'console_scripts': [
            'sql_parser=sql_parser.main:sql_to_lineages',  # Command to start the app
        ],
    },
)