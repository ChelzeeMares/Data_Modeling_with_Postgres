Purpose of database for Sparkify
-----------------------------------------------
For Sparkify, this database will allow them to see what songs are being played the most often by users. This will help the company promote their most popular songs, promote popular artists, see when songs are being played the most, and much more information. If Sparkify can continue to include songs that are frequently played, users will continue to listen, which will Sparkify's revenue. Understanding their users can help create a better service that users want to interact with above all other competitors.


Schema design and ETL pipeline
-----------------------------------------------
A **star schema** was used due to having one fact table and four dimension tables. All four dimension tables will work off the fact table, which is best represented by a star schema. 
- The fact table is 'songplays' which includes nine elements. 
- The dimension tables are 'time' with seven elements, 'users' with five elements, 'songs' with five elements, and 'artists' with five elements. 
- Each of the dimension tables had a primary key that referenced the fact table. 'time' has primary key 'start_time, 'users' has primary key 'user_id', 'songs' has primary key 'song_id', and 'artists' has primary key 'artist_id'. 

Each of these primary keys implies a NOT NULL, and NOT NULL was also added to songplays.start_time, songplays.user_id, and songs.artist_id for reference purposes. In the songplays fact table, songplay_id is defined as 'serial' so that it will autoincrement. In the users table, a ON CONFLICT DO UPDATE needed to be added to allow those moving from "free" accounts to "paid" accounts to reflect as such. This allowing of updates is only reflected for 'level'. There were some attributes that seemed like they would be integers, such as all of the IDs. After trying to run the ETL pipeline with those defined as integers, I quickly found that these IDs included numbers and letter, which led to a varchar definition. It was important to break up the timestamp into different variables for data analysis. With being able to see the day of the week, hour, etc., Sparkify will be able to see which days have their highest listens and what they can do to promote listening on days that are less popular. 

The scripts should be ran from "Data Modeling.ipynb" and will run 'create_tables.py', 'sql_queries.py', and 'etl.py'.

