# bigquery-etl-pipeline
This repository contains a data pipeline scripts that extracts data, and loads it into Google BigQuery.


<h2>The Architecture</h2>

![architecture](https://github.com/Abdelrahman7000/bigquery-etl-pipeline/assets/61333407/ff4a62a0-c6eb-495c-a75d-f8c8af4fcd19)

<h3>Script</h3>
<ul>
<li><b>build_db.py</b>: Creates the database for our data, and contains SQL queries to create the tables.</li>
<li><b>db_conf.py</b> : Python script for creating the database connection, closing the connection, and executing different queries.</li>
<li><b>Extract.py</b> : Python script for extracting the data from the CSV files and loading it into the staging database.</li>
<li><b>Transform_and_load.py</b>: Transforms the extracted data, and loads the transformed data into BigQuery.</li>
</ul>
<h2>Star Schema model</h2>

![Untitled](https://github.com/Abdelrahman7000/bigquery-etl-pipeline/assets/61333407/e4429944-e251-4cd6-a56a-20ef5091f828)


<h2>The Reporting</h2>

![Screenshot from 2024-06-02 21-27-39](https://github.com/Abdelrahman7000/bigquery-etl-pipeline/assets/61333407/19e81189-da44-412d-add9-e876c8b542e0)

![Screenshot from 2024-06-02 21-26-26](https://github.com/Abdelrahman7000/bigquery-etl-pipeline/assets/61333407/cde692a0-5eef-47a4-a0ff-692484f597e3)
