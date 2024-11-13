# T-SQL Code Parser

This project is an **T-SQL** code parser that analyzes T-SQL queries, visualizes the data flows using **Sankey Diagrams** accessible through a dasboard generated with **Flask** and **Plotly**, and automatically generates a **Word (docx)** report summarizing the content of the T-SQL queries, their complexity and other characteristics.

## Features

- **T-SQL Parsing**: Extracts and analyzes table and column level components from the T-SQL queries, including Filters (Where expressions) Join Argument and Transformation.
- **Sankey Diagram Visualization**: Visualizes the flow of tasks and data using a Sankey diagram for easy understanding of data movement and control flow.
- **Auto-Generated Documentation**: Creates a structured Word report (docx) summarizing the T-SQL queries details.
- **Web Interface**: Provides an interactive web interface built with Flask for uploading T-SQL queries and viewing the Sankey diagrams.

## Project Structure

### Folders

- **data/queries-txts/**: folder with sample T-SQL queries saved in .txt files
- **data/output-tables**: folder with the output parsed data, used to generate the sankey graph and automatic reports
- **modules**: folder with the python scripts that handle the parsing, sankey graph dashboard and report generation

## Installation

### Prerequisites

Ensure you have **Python > 3.10** installed. You will also need the following Python libraries:

- `Flask`
- `Plotly`
- `python-docx` (for report generation)
- `Pandas`
- `Sqlglot` (https://github.com/KaiserM11/sqlglot_ma_Transformations)
- `SankeApp` (https://github.com/mountanalytics/sankey-plot-lineages)


## Usage

1. Clone the repository: 

```bash
git clone https://github.com/mountanalytics/sql_code_parser_lineages.git  
cd sql_code_parser_lineages
```

2. You can install the required packages by running:

```bash
pip install -r requirements.txt
```

3. Access the main.py file, input the path to your T-SQL queries, and run the python script

4. Access the dashboard through

```bash
http://127.0.0.1:5000
```







