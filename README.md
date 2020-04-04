# Million Song dataset with AWS redshift

The used dataset is the million songs dataset (http://millionsongdataset.com/). Also log data was created using this event simulator (https://github.com/Interana/eventsim) that fits to the song data. 

**This program consisist of 3 python scripts and 1 config (.cfg).:**
    1. dwh.cfg
    2. sql_queries.py
    3. create_tables.py
    4. etl.py 

1. **dwh.cfg:**
In this config is data stored: 
* the path of the song data and log data
* the address of the redshift cluster (endpoint)
* data to connect to redshift cluster (username, password, port, db name)
* IAM Role ARN to get access to s3 reading and redshift full access

2. **sql_queries.py:**
In this .py file are all the sql queries stored: 
* creation queries: to create all the tables, creation queries are specified:
    * staging_events_table_create
    * staging_songs_table_create
    * songplay_table_create
    * user_table_create
    * song_table_create
    * artist_table_create
    * time_table_create
* copy queries: to copy data from s3 bucket to staging tables:
    * staging_events_copy
    * staging_songs_copy
* insertion queries: gets data from the two staging tables and sorts it into the Data Mart:\
    * songplay_table_insert
    * user_table_insert
    * song_table_insert
    * artist_table_insert
    * time_table_insert
    * drop queries for all created tables
\
3. **create_tables.py[before running make sure to create a redshift cluster and fill in the connection data to the config]:**\
In this python script are 3 functions defined:
    * drop_tables(cur, conn): 
        * All drop queries are executed. So redshift is cleared from all prior created tables.
        Cur is the cursor and conn is the Connection
    * create_tables(cur, conn): 
        All creation queries are executed. So all tables that will be used are created. Progress is
        printed to the Terminal. Cur is the cursor and conn is the Connection
    * main(): 
        * reads the config, connects to the redshift cluster and creates a cursor
        * executes the functions drop_tables and create_tables
        * At last closes the connection\
    
4. **etl.py:**\
In this python script 3 functions are defined: 
* load_staging_table(cur, conn): 
    The data set is loaded from a public s3 bucket into the staging tables. This is
    achieved by executing the copy queries. Cur is the cursor and conn is the Connection.
* insert_table(cur, conn):All insertion queries are executed. So the data is moved from the staging tables to the data 
    mart. Cur is the cursor and conn is the Connection.
* main():
    * reads the config, connects to the redshift cluster and creates a cursor
    * functions load_staging_table and insert_table are executed
    * At last closes the connection
 
                                                             
**The scripts must be used in this order to work:** \
    1. create a redshift cluster in AWS Console\
    2. create a IAM Role with s3 read permission and redshift full access\
    3. attach the Role to the redshift cluster\
    4. fill out the config with connection data regarding the redshift cluster and the ARN of the created role\
    5. run create_tables.py in the Terminal\
    6. run etl.py in the Terminal\
    7. cluster is ready to use

                                                        
                                                        
