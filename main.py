from ast import Index
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
    try:

        print("step 1 : getting link from pubsub message")
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
        
        print("step 2 : downloading file from cloud storage")
        #filePath=destination_file_name #deprecated uses when we are using download as a file
        data=download_blob(bucket_name, blob_name, destination_file_name)

        #with open(filePath, "rb") as filePath: #deprecated uses when we are using download as a file
        encoded_string = base64.b64encode(data)
        data64 = encoded_string.decode('UTF-8')

        headers = {'content-type': 'application/json'}

        body =  """{"filedata":\""""+data64+"""\","filename":\""""+ destination_file_name+"""\","userkey":\""""+ USERKEY+"""\",\"version\":\""""+VERSION+"""\",\"subuserid\":\""""+subUserId+"""\"}"""
        print("step 3 : Sending Response")
        response = requests.post(APIURL,data=body,headers=headers)
        resp =json.loads(response.text)
        #print(resp)
        #please handle error too
        Resume =resp["ResumeParserData"]
        print("step 4 : Preparing dictionary for pandas")
        dictt={}
        dictt["ResumeFileName"]=str(resp["ResumeParserData"]["ResumeFileName"])
        dictt["Language"]=str(resp["ResumeParserData"]["ResumeLanguage"]["Language"])
        dictt["ParsingDate"]=str(resp["ResumeParserData"]["ParsingDate"])
        dictt["ResumeCountry"]=str(resp["ResumeParserData"]["ResumeCountry"]["Country"])
        dictt["FullName"]=str(resp["ResumeParserData"]["Name"]["FullName"])
        dictt["TitleName"]=str(resp["ResumeParserData"]["Name"]["TitleName"])
        dictt["FirstName"]=str(resp["ResumeParserData"]["Name"]["FirstName"])
        dictt["MiddleName"]=str(resp["ResumeParserData"]["Name"]["MiddleName"])
        dictt["LastName"]=str(resp["ResumeParserData"]["Name"]["LastName"])
        dictt["FormattedName"]=str(resp["ResumeParserData"]["Name"]["FormattedName"])
        dictt["DateOfBirth"]=str(resp["ResumeParserData"]["DateOfBirth"])
        dictt["Gender"]=str(resp["ResumeParserData"]["Gender"])
        dictt["FatherName"]=str(resp["ResumeParserData"]["FatherName"])
        dictt["MotherName"]=str(resp["ResumeParserData"]["MotherName"])
        dictt["MaritalStatus"]=str(resp["ResumeParserData"]["MaritalStatus"])
        dictt["Nationality"]=str(resp["ResumeParserData"]["Nationality"])
        dictt["LicenseNo"]=str(resp["ResumeParserData"]["LicenseNo"])
        dictt["PassportNumber"]=str(resp["ResumeParserData"]["PassportDetail"]["PassportNumber"])
        dictt["PassportDetail_PlaceOfIssue"]=str(resp["ResumeParserData"]["PassportDetail"]["PlaceOfIssue"])
        email_lst=[]
        for x in resp["ResumeParserData"]["Email"]:
            email_lst.append(x["EmailAddress"])
        dictt["EmailAddress"]=str(email_lst ) 
        PhoneNumber_lst=[]
        for x in resp["ResumeParserData"]["PhoneNumber"]:
            PhoneNumber_lst.append(x["Number"])
        dictt["PhoneNumber_Number"]=str(PhoneNumber_lst )
        PhoneNumber_ISD_Code_lst=[]
        for x in resp["ResumeParserData"]["PhoneNumber"]:
            PhoneNumber_ISD_Code_lst.append(x["ISDCode"])
        dictt["PhoneNumber_ISDCode"]=str(PhoneNumber_ISD_Code_lst) 
        PhoneNumber_FormattedNumber_lst=[]
        for x in resp["ResumeParserData"]["PhoneNumber"]:
            PhoneNumber_FormattedNumber_lst.append(x["FormattedNumber"])
        dictt["PhoneNumber_FormattedNumber"]=str(PhoneNumber_FormattedNumber_lst) 
        PhoneNumber_Type_lst=[]
        for x in resp["ResumeParserData"]["PhoneNumber"]:
            PhoneNumber_FormattedNumber_lst.append(x["Type"])
        dictt["PhoneNumber_Type"]=str(PhoneNumber_Type_lst) 
        WebSite_lst=[]
        for x in resp["ResumeParserData"]["WebSite"]:
            WebSite_lst.append(x["Type"])
        dictt["WebSite"]=str(WebSite_lst) 
        Address_Street_lst=[]
        for x in resp["ResumeParserData"]["Address"]:
            Address_Street_lst.append(x["Street"])
        dictt["Address_Street"]=str(Address_Street_lst) 
        Address_City_lst=[]
        for x in resp["ResumeParserData"]["Address"]:
            Address_City_lst.append(x["City"])
        dictt["Address_City"]=str(Address_City_lst) 
        Address_State_lst=[]
        for x in resp["ResumeParserData"]["Address"]:
            Address_State_lst.append(x["State"])
        dictt["Address_State"]=str(Address_State_lst) 
        Address_Country_lst=[]
        for x in resp["ResumeParserData"]["Address"]:
            Address_Country_lst.append(x["Country"])
        dictt["Address_Country"]=str(Address_Country_lst) 

        Address_ZipCode_lst=[]
        for x in resp["ResumeParserData"]["Address"]:
            Address_ZipCode_lst.append(x["ZipCode"])
        dictt["Address_ZipCode"]=str(Address_ZipCode_lst) 

        Address_FormattedAddress_lst=[]
        for x in resp["ResumeParserData"]["Address"]:
            Address_FormattedAddress_lst.append(x["FormattedAddress"])
        dictt["Address_FormattedAddress"]=str(Address_FormattedAddress_lst) 

        Address_Type_lst=[]
        for x in resp["ResumeParserData"]["Address"]:
            Address_Type_lst.append(x["Type"])
        dictt["Address_Type"]=str(Address_Type_lst) 
        dictt["Category"]=str(resp["ResumeParserData"]["Category"])
        dictt["SubCategory"]=str(resp["ResumeParserData"]["SubCategory"])
        dictt["CurrentSalary_Amount"]=str(resp["ResumeParserData"]["CurrentSalary"]["Amount"])
        dictt["CurrentSalary_Symbol"]=str(resp["ResumeParserData"]["CurrentSalary"]["Symbol"])
        dictt["CurrentSalary_Currency"]=str(resp["ResumeParserData"]["CurrentSalary"]["Currency"]   )
        dictt["CurrentSalary_Unit"]=str(resp["ResumeParserData"]["CurrentSalary"]["Unit"])
        dictt["CurrentSalary_Text"]=str(resp["ResumeParserData"]["CurrentSalary"]["Text"])
        dictt["ExpectedSalary_Amount"]=str(resp["ResumeParserData"]["ExpectedSalary"]["Amount"])
        dictt["ExpectedSalary_Symbol"]=str(resp["ResumeParserData"]["ExpectedSalary"]["Symbol"])
        dictt["ExpectedSalary_Currency"]=str(resp["ResumeParserData"]["ExpectedSalary"]["Currency"])
        dictt["ExpectedSalary_Unit"]=str(resp["ResumeParserData"]["ExpectedSalary"]["Unit"])
        dictt["ExpectedSalary_Text"]=str(resp["ResumeParserData"]["ExpectedSalary"]["Text"])
        dictt["Qualification"]=str(resp["ResumeParserData"]["Qualification"])
        SegregatedQualification_Institution_Name_lst=[]
        for x in resp["ResumeParserData"]["SegregatedQualification"]:
            SegregatedQualification_Institution_Name_lst.append(x["Institution"]["Name"])
        dictt["SegregatedQualification_Institution_Name"]=str(SegregatedQualification_Institution_Name_lst) 

        SegregatedQualification_Institution_Type_lst=[]
        for x in resp["ResumeParserData"]["SegregatedQualification"]:
            SegregatedQualification_Institution_Type_lst.append(x["Institution"]["Type"])
        dictt["SegregatedQualification_Institution_Type"]=str(SegregatedQualification_Institution_Type_lst) 
        SegregatedQualification_DegreeName_lst=[]
        for x in resp["ResumeParserData"]["SegregatedQualification"]:
            SegregatedQualification_DegreeName_lst.append(x["Degree"]["DegreeName"])
        dictt["SegregatedQualification_DegreeName"]=str(SegregatedQualification_DegreeName_lst) 
        SegregatedQualification_FormattedDegreePeriod_lst=[]
        for x in resp["ResumeParserData"]["SegregatedQualification"]:
            SegregatedQualification_FormattedDegreePeriod_lst.append(x["FormattedDegreePeriod"])
        dictt["SegregatedQualification_FormattedDegreePeriod"]=str(SegregatedQualification_FormattedDegreePeriod_lst)
        dictt["Certification"]=str(resp["ResumeParserData"]["Certification"])
        dictt["SkillBlock"]=str(resp["ResumeParserData"]["SkillBlock"])
        dictt["SkillKeywords"]=str(resp["ResumeParserData"]["SkillKeywords"])
        dictt["Experience"]=str(resp["ResumeParserData"]["Experience"])
        SegregatedExperience_Employer_EmployerName_lst=[]
        for x in resp["ResumeParserData"]["SegregatedExperience"]:
            SegregatedExperience_Employer_EmployerName_lst.append(x["Employer"]["EmployerName"])
        dictt["SegregatedExperience_Employer_EmployerName"]=str(SegregatedExperience_Employer_EmployerName_lst)
        SegregatedExperience_Employer_JobProfile_lst=[]
        for x in resp["ResumeParserData"]["SegregatedExperience"]:
            SegregatedExperience_Employer_JobProfile_lst.append(x["JobProfile"]["Title"])
        dictt["SegregatedExperience_Employer_JobProfile"]=str(SegregatedExperience_Employer_JobProfile_lst)
        SegregatedExperience_JobPeriod_lst=[]
        for x in resp["ResumeParserData"]["SegregatedExperience"]:
            SegregatedExperience_JobPeriod_lst.append(x["FormattedJobPeriod"])
        dictt["SegregatedExperience_JobPeriod"]=str(SegregatedExperience_JobPeriod_lst)
        SegregatedExperience_IsCurrentEmployer_lst=[]
        for x in resp["ResumeParserData"]["SegregatedExperience"]:
            SegregatedExperience_IsCurrentEmployer_lst.append(x["IsCurrentEmployer"])
        dictt["SegregatedExperience_IsCurrentEmployer"]=str(SegregatedExperience_IsCurrentEmployer_lst)
        dictt["CurrentEmployer"]=str(resp["ResumeParserData"]["CurrentEmployer"])
        dictt["JobProfile"]=str(resp["ResumeParserData"]["JobProfile"])
        dictt["WorkedPeriod_TotalExperienceInMonths"]=str(resp["ResumeParserData"]["WorkedPeriod"]["TotalExperienceInMonths"])
        dictt["WorkedPeriod_TotalExperienceInYear"]=str(resp["ResumeParserData"]["WorkedPeriod"]["TotalExperienceInYear"])
        dictt["WorkedPeriod_TotalExperienceRange"]=str(resp["ResumeParserData"]["WorkedPeriod"]["TotalExperienceRange"])
        dictt["WorkedPeriod_GapPeriod"]=str(resp["ResumeParserData"]["GapPeriod"])
        dictt["AverageStay"]=str(resp["ResumeParserData"]["AverageStay"])
        dictt["LongestStay"]=str(resp["ResumeParserData"]["LongestStay"])
        dictt["Summary"]=str(resp["ResumeParserData"]["Summary"])
        dictt["ExecutiveSummary"]=str(resp["ResumeParserData"]["ExecutiveSummary"])
        dictt["ManagementSummary"]=str(resp["ResumeParserData"]["ManagementSummary"])
        dictt["Coverletter"]=str(resp["ResumeParserData"]["Coverletter"])
        dictt["Publication"]=str(resp["ResumeParserData"]["Publication"])
        CurrentLocation_City_lst=[]
        for x in resp["ResumeParserData"]["CurrentLocation"]:
            CurrentLocation_City_lst.append(x["City"])
        dictt["CurrentLocation_City"]=str(CurrentLocation_City_lst)
        PreferredLocation_City_lst=[]
        for x in resp["ResumeParserData"]["PreferredLocation"]:
            PreferredLocation_City_lst.append(x["City"])
        dictt["PreferredLocation_City"]=str(PreferredLocation_City_lst)
        dictt["Availability"]=str(resp["ResumeParserData"]["Availability"])
        dictt["Hobbies"]=str(resp["ResumeParserData"]["Hobbies"])
        dictt["Objectives"]=str(resp["ResumeParserData"]["Objectives"])
        dictt["References"]=str(resp["ResumeParserData"]["References"])
        dictt["CustomFields"]=str(resp["ResumeParserData"]["CustomFields"])
        dictt["ApiInfo_Metered"]=str(resp["ResumeParserData"]["ApiInfo"]["Metered"])
        dictt["ApiInfo_CreditLeft"]=str(resp["ResumeParserData"]["ApiInfo"]["CreditLeft"])
        dictt["ApiInfo_AccountExpiryDate"]=str(resp["ResumeParserData"]["ApiInfo"]["AccountExpiryDate"])        
        print("step 5 : Pandas reading json")
        df=pd.DataFrame(dictt,index=[0])
        print(df)
        print("step 6 : loading dataframe into bigquery")
        load_job = client.load_table_from_dataframe(
        df, 'rchilli-etl.resumes.Resume_data', job_config=job_config
        )
        load_job.result()
        destination_table = client.get_table('rchilli-etl.resumes.Resume_data')  # Make an API request.
        print("Loaded {} rows.".format(destination_table.num_rows))
    except Exception as e:
        print("exception main:  ", e)

