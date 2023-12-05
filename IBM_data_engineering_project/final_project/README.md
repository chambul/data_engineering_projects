# IBM Data Engineering Final Project
This is the ETL pipeline I have developed for the final project of the course "IBM: Python for Data Engineering Project". I have developed an ETL pipeline utilising the following skills: 
- Webscraping and data extraction using APIs such as Beautiful Soup.
- Logging data operations at different stages of the pipeline.
- Transform and store the data in a database.

## Project Description 
A multi-national firm has hired you as a data engineer. Your job is to access and process data as per requirements. Your boss asked you to compile the list of the top 10 largest banks in the world ranked by market capitalization in billion USD. Further, you need to transform the data and store it in USD, GBP, EUR, and INR per the exchange rate information made available to you as a CSV file. You should save the processed information table locally in a CSV format and as a database table. Managers from different countries will query the database table to extract the list and note the market capitalization value in their own currency.

## Preliminaries: Installing libraries and downloading data


```
python3.11 -m pip install numpy
python3.11 -m pip install pandas

wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv

```
