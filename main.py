import base64
import json
import requests
from resume_download import download_blob
import pandas as pd
from google.cloud import bigquery
APIURL="https://rest.rchilli.com/RChilliParser/Rchilli/parseResumeBinary"
USERKEY = 'YLJA8V6R'
VERSION = '8.0.0'
subUserId = 'Jahanzeb'

def hello_pubsub(event, context):
    client = bigquery.Client() 
    job_config = bigquery.LoadJobConfig(

        autodetect=True
    )

    pubsub_message = json.loads(base64.b64decode(event['data']).decode('utf-8'))
    bucket_name=pubsub_message['bucket']
    blob_name=pubsub_message['name']
    destination_file_name=blob_name.split('/')[-1] #for local deployment
    #destination_file_name="/tmp/"+blob_name.split('/')[-1] # for cloud deployment
    print(destination_file_name)
    uri="gs://"+bucket_name+"/"+blob_name
    print(uri)
    
    #filePath=destination_file_name #deprecated uses when we are using download as a file
    data=download_blob(bucket_name, blob_name, destination_file_name)

    #with open(filePath, "rb") as filePath: #deprecated uses when we are using download as a file
    encoded_string = base64.b64encode(data)
    data64 = encoded_string.decode('UTF-8')

    headers = {'content-type': 'application/json'}

    body =  """{"filedata":\""""+data64+"""\","filename":\""""+ destination_file_name+"""\","userkey":\""""+ USERKEY+"""\",\"version\":\""""+VERSION+"""\",\"subuserid\":\""""+subUserId+"""\"}"""

    response = requests.post(APIURL,data=body,headers=headers)
    resp =json.loads(response.text)
    #print(resp)
    #please handle error too
    Resume =resp["ResumeParserData"]
    #read values from response
    # print (Resume["Name"]["FirstName"])
    # print (Resume["Name"]["LastName"])
    # print (Resume["Email"])
    # print (Resume["SegregatedExperience"])
    dictt={"First_Name":Resume["Name"]["FirstName"],"Last_Name":Resume["Name"]["LastName"],"Email":Resume["Email"]}
    print(type(Resume))
    json_name=json.dumps(dictt)
    print(type(json_name))
    #print(json_name)
    df=pd.read_json(json_name)
    print(df)
    load_job = client.load_table_from_dataframe(
    df, 'rchilli-etl.resumes.data', job_config=job_config
    )
    load_job.result()
    destination_table = client.get_table('rchilli-etl.resumes.data')  # Make an API request.
    print("Loaded {} rows.".format(destination_table.num_rows))

