This project I created a docker image containing a postgres database, and a Pgadmin. These are hosted locally on their relevant ports:

5432 for posgres 
8080 for Pgadmin

The python script pipeline.py then accesses a football stats Api,
finds some relevant data and sends it to my database.

To use this, you first run the docker-compose command to fire up the docker images and then 
To run the file in the terminal use the following command.
python pipeline.py \
    --user=root \
    --password=root \
    --host=localhost \ 
    --port=5432 \
    --db=premier_league \
    --table_name=Goals_by_time