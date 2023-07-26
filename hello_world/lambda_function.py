import json
import boto3
import csv
import psycopg2 as psy
import pandas as pd

conn = psy.connect(
    host = 'demo.cfjny80bko8u.us-east-1.rds.amazonaws.com',
    database = 'postgres',
    user = 'postgres',
    password = 'Man0nFire2012!'
)


s3 = boto3.resource('s3')


def lambda_handler(event, context):
    bucket = s3.Bucket(name="robertdemobucket")
    upload_bucket = s3.Bucket(name="robertcsvbucket")
    #key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    key = 'Demo.csv'
    cursor = conn.cursor()
    try:
        local_file_name = '/tmp/test.csv' 
        bucket.download_file(key,local_file_name)
        
        with open(local_file_name, 'a', newline='') as outfile:
            writer = csv.writer(outfile, delimiter=',', quotechar='"')
            writer.writerow([2, 'World'])
        
        with open(local_file_name, 'r') as infile:
            reader = csv.reader(infile, delimiter = ',')
            
            rows = list(reader)

            for row in rows:
                print(row)
        
        df = pd.read_csv(local_file_name, delimiter=',')
        print(df)
        for col1, data in df.itertuples(index=False):
            cursor.execute("""INSERT INTO demo.demo(key,data) VALUES(%s, %s)""", (col1,data))
            print(col1,data)
        upload_bucket.upload_file(local_file_name, key)
        conn.close()
    except Exception as e:
        print(e)
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
