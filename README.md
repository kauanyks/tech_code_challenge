# Introduction 

Project developed from a Data Engineering challenge, in which there was a need to create a complete pipeline.

# The Challenge

Two data sources were provided: a Postgres database and a CSV file. The CSV file represents order details from an ecommerce system. The database is provided by Microsoft for educational purposes, called Northwind, the only difference is that the order_detail table does not exist in the database, since it is being provided in CSV.

Original database schema:

![image](https://user-images.githubusercontent.com/49417424/105997621-9666b980-608a-11eb-86fd-db6b44ece02a.png)

The challenge is to build a pipeline that extracts data every day from both sources and writes it first to local disk and then to a database of your choice.

It is important that all steps are isolated, that is, it must be possible to execute one step without executing the others.

## Step 1

For this first step, where you write the data to the local disk, there should be one file for each database table and one for the CSV file. This pipeline should have a separate file path for each data source (Postgres or CSV), table name, and execution date, for example:

```
/data/postgres/{table}/2021-01-01/file.format
/data/postgres/{table}/2021-01-02/file.format
/data/csv/2021-01-02/file.format
```

You have the freedom to choose the name and format of the files that will be used.

## Step 2

In the second step, you must load the data from the local disk and write it to the chosen database. Always remember to justify the choices made during the process.

## Step 3

The ultimate goal is to perform a query that shows the orders and their details. The order information is in a table called "orders", which comes from the Northwind database and the details in the CSV file provided. The relationship between the tables is through the "order_id" field.

The pipeline should follow this format:

![image](https://user-images.githubusercontent.com/49417424/105993225-e2aefb00-6084-11eb-96af-3ec3716b151a.png)


## Requirements

- All tasks must be idempotent, the pipeline must be able to run more than once a day and the result must be exactly the same
- Step 2 depends on step 1, so it should not be possible to run the second step if the first one is not successful
- You must extract all tables from the available sources, regardless of whether they will be used later
- It must be possible to identify where the pipeline fails, in case of an error in any step
- Instructions for executing the pipeline must be made available; the easier, the better
- A CSV or JSON file must be generated with the result of the final query
- There is no need to schedule the execution of the pipeline, but it must be possible to run it on different days, that is, an argument with the desired date must be passed and the pipeline must reprocess the data from that specific day.

## Required Setup

The database can be configured using docker compose. For more instructions: https://docs.docker.com/compose/install/

The credentials are available in the docker-compose.yml file

# Pipeline

## Instructions for running the pipeline:

- the .ipynb file must be saved in the code-challenge-main folder and can be run via Visual Studio Code, as long as the Jupyter extension is installed.
- pip install of the following libraries must be run, if they are not already installed:
        - pip install psycopg2
        - pip install pandas
        - pip install pymongo
- the date parameter must be passed in the processing_date variable, following the format instructions commented in the code ('yyyy-MM-dd' or str(date.today())).

## Database and file format used:

The MongoDB system was used as the database, since this system's storage base is in documents. The network access was configured to allow all IP addresses and access to the database through the following username and password: northwind_user and thewindisblowing, respectively.

The choice to use MongoDB was made with a view to the exponential growth of data (scalability) and the capacity of this database to guarantee high availability through a replication process. This database also provides
official driver support for practically all popular languages, ensuring greater ease of work.

The document structure was organized based on collections, in which each table generated a collection named {table}_{processing_date}_collection, for better organization within the database. The documents were modeled through JSON formatting and then inserted into MongoDB where they are converted to a binary format for storage. The JSON format adds flexibility to documents, providing a semi-structured file that allows schema evolution, according to the company's needs.

## Step 1

The following libraries were used: psycopg2 (PostgreSQL database connection), os (interaction with the Operating System and local disk), datetime (generation of the current date) and pandas (CSV writing).

Initially, a variable called processing_date was created to define which date would be executed.

Later, a function save_postgres_to_csv was created, with the parameters of table name and processing date. The function defines the path to be saved to the files from the directory where the .ipynb file is saved and tries to create the folder for each step (source - postgres/csv, table name, processing date) if it does not exist.
```
current_path = os.getcwd()
source = 'postgres'
source_path = os.path.join(current_path, source)

#create source folder if not exists
try:
    os.mkdir(source_path)
    print(f'path created: ', source_path)
except:
    pass

#create specific table folder if not exists
table_path = os.path.join(source_path, table)

try:
    os.mkdir(table_path)
    print(f'path created: ', table_path)
except:
    pass

date_path = os.path.join(table_path, processing_date)

#create processing date folder if not exists
try:
    os.mkdir(date_path)
    print(f'path created: ', date_path)
except:
    pass

filename = f"{table}.csv"      
final_path = os.path.join(date_path, filename)
```
With the path to save the files created, the function connects to the database and performs the query that exports to CSV.
```
conn = psycopg2.connect(host = "localhost", database="northwind", user="northwind_user", password=<password>)
```
Finally, the function writes the generated CSV to the defined path, with UTF-8 encoding.
```
cur = conn.cursor()
sql = f"COPY (select * from {table}) TO STDOUT WITH CSV HEADER"  

with open(final_path, "w", encoding = 'utf-8') as file:
    cur.copy_expert(sql, file)
    file.close 
```

The second function is specific to the CSV file, called save_csv_to_csv and receives the same parameters as the previous one. The logic for creating the path to save the files is the same and for writing, the to_csv function is used, with the index = False option.

Finally, if the connection to the Postgres database is successful, a list with the name of each of the tables is read and the functions described above are executed.

## Step 2

The following libraries were used: pymongo (connection to the MongoSB database), os (interaction with the Operating System and local disk), datetime (generation of the current date) and pandas (writing CSV).

The variable processing_date is called again, considering the execution of the steps separately.

The function csv_to_mongo was created, with the parameters for the table name, source and processing date. Within this function, the execution of the previous step is validated, ensuring that the csv is written locally before continuing with the execution.

The connection to the database is then made:
```
client = pymongo.MongoClient("mongodb+srv://northwind_user:<password>@cluster0.ti9zwzb.mongodb.net/?retryWrites=true&w=majority")
```
After connecting, the function checks if the table has data and proceeds to create the database and collection in mongo, if they do not exist. Finally, the data is written to the specific collection of each table.
```
if data.empty:
    print(f'{table} is an empty table, not added to the mongo collection')
else:
    #create mongodb database if not exists
    if "northwind_db" not in client.list_database_names():
        client["northwind_db"]
        print("northwind_db dabatase created")

    #create or overwrite mongodb collection 
    db = client["northwind_db"]
    collection = db[f"{table}_{processing_date}_collection"]
    
    if f"{table}_{processing_date}_collection" not in client["northwind_db"].list_collection_names():
        collection
        print(f"{table}_{processing_date}_collection created")
    else:
        collection.drop()
        collection
        print(f"{table}_{processing_date}_collection overwritten")

    collection.insert_many(data.to_dict('records'))
```

To call the function execution, a list with the table names is used again.

## Step 3

The pymongo (MongoSB database connection) and pandas (CSV writing) libraries were used.

A connection to MongoDB is made again and, this time, the "orders" and "order_details" tables are read, passing the schema of each of them.

Finally, a left join is performed between the tables using the "order_id" field as the key and a csv of the final file is saved.
```
final_df = orders.join(order_details.set_index('order_id'), on = 'order_id', how = 'left')

current_path = os.getcwd()
filename = f"full_order_table.csv"
file_path = os.path.join(current_path, filename)

final_df.to_csv(file_path, sep = ',', index = False, encoding = 'utf-8')
```
