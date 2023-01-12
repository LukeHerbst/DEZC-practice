import os
import argparse
import pandas as pd
from sqlalchemy import create_engine
import keep_safe
import requests



def main(params):
    # define all the parameters needed for the sql database
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    table_name = params.table_name
    
    
    # api url for the 
    url_api_1 = "https://v3.football.api-sports.io/teams?league=39&season=2018"

    payload_1={}
    headers_1 = {
    'x-rapidapi-key': keep_safe.api_key,
    'x-rapidapi-host': 'v3.football.api-sports.io'
        }

    response_teams = requests.request("GET", url_api_1, headers=headers_1, data=payload_1)
    df = pd.DataFrame() 
    for i in range(20):
        temp_df = pd.DataFrame(response_teams.json()['response'][i]['team'],index =[i])
        df = df.append(temp_df)

    df_final = pd.DataFrame()

    teams_names = ["Crystal Palace","Manchester City","Liverpool"]
    for i in teams_names:
        team = df[df["name"] == i]
        team_int = team.id.values[0]

        url_api_2 = f"https://v3.football.api-sports.io/teams/statistics?season=2019&league=39&team={team_int}"

        payload_2={}
        headers_2 = {
        'x-rapidapi-key': keep_safe.api_key,
        'x-rapidapi-host': 'v3.football.api-sports.io'
        }

        response_stats = requests.request("GET", url_api_2, headers=headers_2, data=payload_2)
        df_goals_minutes = pd.DataFrame(response_stats.json()['response']['goals']['for']['minute'])
        df_goals_minutes['team'] = i
        df_final = df_final.append(df_goals_minutes)


    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    

    df_final.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    df_final.to_sql(name=table_name, con=engine, if_exists='append')


     

        

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest Api data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    

    args = parser.parse_args()

    main(args)

    
    
    