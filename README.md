# SparkETL

Steps : 

1. Cretaed tables in Redshift Named XTRA_MSTR, XTRA_SOLD 
2. Created a Crawler in AWS GLUE, the crawler has data source as JDBC and the two tables as the data source. 
3. Whenever data is inserted into Redshift tables, Crawler triggers a AWS Glue ETL job that runs the spark script. The script gets the data from the tables joins them and inserts the rigjht columns into the third table calles XTRA_DATA
4. Once this is done The data gets stored in Redshift for faster access..

