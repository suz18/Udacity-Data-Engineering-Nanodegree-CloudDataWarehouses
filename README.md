### Project: Sparkify Data Warehouse   

#### Overview     
Sparkify is a music streaming startup, which has grown their user base and song database and want to move their processes and data onto AWS as the cloud can store and process a large quantity of data.     
This project is to build a ETL pipeline that extracts log and song data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for the analytics team to continue finding insights in what songs users are listening to.

#### Database Schema Design     
The database schema design adopted in this is a star schema so that it can be optimized for queries on song play analysis.     
There is one fact table, songplays, which records in event data associated with song plays.      
The songplays fact table is accompanied with four dimension tables, users, songs, artists and time.     

#### ETL Pipeline
1. Two tables staging_events and staging_songs that are used for staging data are created, so are the dimension tables and the fact table.     
2. Log data and song data that reside in S3 are copied into those two staging tables.      
3. Then, data within the staging tables are processed and loaded into the analytix tables (fact and dimention tables).    

#### Example Queries   
1. To know which user listens to the most songs and how many?       
        select count(*) as nsongs, user_id
        from songplays
        group by user_id
        order by nsongs desc
        limit 1;
2. To know which artist has been listened to the most and how many times?
        select count(artists.name) as atimes, artists.name as artist
        from artists
        join songplays
        on artists.artist_id = songplays.artist_id
        group by artists.name
        order by atimes desc
        limit 1;
