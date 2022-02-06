This directory contains the homework files for week 2 of the Data Engineering Zoomcamp organized by DataTalksClub. \#dezoomcamp

In this homework, 3 Airflow DAGs are prepared to download open source TLC Trip Record Data datasets provided to the 
NYC Taxi and Limousine Commission (TLC) by technology providers.  
The particular datasets of interest are the Yellow Taxi Trip Records ("YTT") - from Jan. 2019 to Dec. 2020, 
For-Hire Vehicle (“FHV”) trip records - from Jan. 2019 to Dec. 2019, and the Taxi Zone Lookup Table dataset.

The assignment is to create data pipeline workflows to download these files, which are all in csv format, convert them to 
parquet format, and upload them to a Google Cloud Platform storage bucket.
Three separate DAGs are created to perform these tasks for each of the datasets: Yellow Taxi Trip Records, For-Hire Vehicle trip records,
and the Taxi Zone Lookup Table.


Airflow is installed and run via Docker Compose containers. The specifications for building the Airflow images, are defined in `docker-compose.yaml` and `Dockerfile` files.  
The project dependencies defined in `requirements.txt` file is used by Docker to install the required packages while building the Airflow images. 

Full homework description [here](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/week_2_data_ingestion/homework.md).