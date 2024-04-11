## ETL For Airline Loyalty Program
Developed an automated scheduler for data pre-processing using Apache Airflow for Airline Loyalty Program behaviour analysis.

### Project Overview
This ETL projects perform a batch processing with scheduler for automation using Airflow, starting from data extraction from PostgreSQL Database, data cleaning using pandas and data validation using Great Expectation to ensure data quality and consistency. Those process generate a clean data which will then saved to Elasticsearch to be analyzed and visualized using Kibana. This analysis aims to help e-commerce to optimizing female customers sales performance through customers behavior analysis to provide actionable insight and strategic recommendations.
### Tools and Technology
- Docker
- Airflow
- Elasticsearch
- Kibana
- Python
- Jupyter Notebook
- Pandas

### File Description
- `Airflow_DAG.py` : Airflow DAGs containing the code to extract data from database, perform data cleaning and save data to Elasticsearch.
- `Data_Validation_GX.ipynb` : Jupyter Notebook containing the code for data validation using Great Expectation.
- `airline_loyalty_data_raw.csv` : CSV file containing the raw data of airline customer loyalty transaction.
- `airline_loyalty_data_clean.csv` : CSV file containing the clean data of airline customer loyalty transaction.
- `images/` : Folder containing data visualization and analysis result.
- `Airline Loyalty Program Analysis.pdf` : PDF containing data visualization and analysis result for presentation.

### Exploratory Data Analysis
<img width="1434" alt="Plot   Insight 01" src="https://github.com/YolandaKrisnadita/ETL-for-Airline-Loyalty-Program/assets/92908655/17a48625-e8a3-4918-9cf7-cd1c2978dff4">
<img width="1432" alt="Plot   Insight 02" src="https://github.com/YolandaKrisnadita/ETL-for-Airline-Loyalty-Program/assets/92908655/4d66fd24-6ace-444c-b57e-530d8eee5a62">
<img width="1430" alt="Plot   Insight 03" src="https://github.com/YolandaKrisnadita/ETL-for-Airline-Loyalty-Program/assets/92908655/8763366c-aa27-4149-9c79-fd27e05189c2">
<img width="719" alt="Plot   Insight 04" src="https://github.com/YolandaKrisnadita/ETL-for-Airline-Loyalty-Program/assets/92908655/6cd2bcfb-e7ad-4630-af12-37f82aea5a8b">
<img width="717" alt="Plot   Insight 05" src="https://github.com/YolandaKrisnadita/ETL-for-Airline-Loyalty-Program/assets/92908655/e22cf689-07e7-466e-bad0-e6600848c280">
<img width="1435" alt="Plot   Insight 06" src="https://github.com/YolandaKrisnadita/ETL-for-Airline-Loyalty-Program/assets/92908655/4796c628-e22c-4d77-8a5f-c81be905b612">
<img width="1434" alt="Plot   Insight 07" src="https://github.com/YolandaKrisnadita/ETL-for-Airline-Loyalty-Program/assets/92908655/cd5e7b2b-ca4a-4bef-aaa5-f63de3bacb78">
<img width="1431" alt="Conclusion" src="https://github.com/YolandaKrisnadita/ETL-for-Airline-Loyalty-Program/assets/92908655/b04c3421-bd32-4d88-9cc5-b14dc5d8b0e8">








