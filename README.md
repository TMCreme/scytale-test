# Exploring the Github organisation repository APIs with PySpark

## Description

### Extract
1. Get the all repository from Organization: Scytale-exercise (https://github.com/Scytale-exercise)
2. For each repository in the organization, Get all pull requests.
3. Save the data on JSON files.

### Transform
**Please use Pyspark

1. Get the JSON files (from the extract phase) .
2. Transform the separate files into this table using Spark (Use schema).
3. Please save the data to a parquet file.

## Running the application
* Clone the repository.
* [Optional] If you wish to create an AWS EC2 instance to run the application on, I have created a terraform script that can create a jupyter notebook with apache spark in the `iac` directory you can use for the infrastructure as code. Once created, you can login with the AWS Session Manager from the AWS System Manager service to have SSH access into the EC2 instance. You can thereafter clone the repository on to the EC2 instance. 
* Ensure you have created the `.env` file with the github access token and organization name set. You can see a sample in `.env.sample` file
* Run the docker compose command
```sh
    docker-compose up
```
OR 
```sh
    docker-compose up --build
```
* This will extract and transform the data and output the parquet files in a `result` directory


## The End
