import base64
import json
import requests
from resume_download import download_blob
from resume_upload import upload_blob
import pandas as pd
from google.cloud import bigquery
import os
import time

APIURL="https://rest.rchilli.com/RChilliParser/Rchilli/parseResumeBinary"
USERKEY = 'YLJA8V6R'
VERSION = '8.0.0'
subUserId = 'Jahanzeb'
table='rchilli-etl.staging.resume_data_stg'


def hello_pubsub(event, context):
    try:

        print("step 1 : getting link from pubsub message")
        client = bigquery.Client() 
        job_config = bigquery.LoadJobConfig(
            #autodetect=True,

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
        print(Resume)
        print("step 4 : Preparing dictionary for pandas")
        dictt={}
        dictt["ResumeFileName"]=resp["ResumeParserData"]["ResumeFileName"]
        dictt["Language"]=resp["ResumeParserData"]["ResumeLanguage"]["Language"]
        dictt["ParsingDate"]=resp["ResumeParserData"]["ParsingDate"]
        dictt["ResumeCountry"]=resp["ResumeParserData"]["ResumeCountry"]["Country"]
        dictt["FullName"]=resp["ResumeParserData"]["Name"]["FullName"]
        dictt["TitleName"]=resp["ResumeParserData"]["Name"]["TitleName"]
        dictt["FirstName"]=resp["ResumeParserData"]["Name"]["FirstName"]
        dictt["MiddleName"]=resp["ResumeParserData"]["Name"]["MiddleName"]
        dictt["LastName"]=resp["ResumeParserData"]["Name"]["LastName"]
        dictt["FormattedName"]=resp["ResumeParserData"]["Name"]["FormattedName"]
        dictt["DateOfBirth"]=resp["ResumeParserData"]["DateOfBirth"]
        dictt["Gender"]=resp["ResumeParserData"]["Gender"]
        dictt["FatherName"]=resp["ResumeParserData"]["FatherName"]
        dictt["MotherName"]=resp["ResumeParserData"]["MotherName"]
        dictt["MaritalStatus"]=resp["ResumeParserData"]["MaritalStatus"]
        dictt["Nationality"]=resp["ResumeParserData"]["Nationality"]

        languageknown_lst=[]
        for x in resp["ResumeParserData"]["LanguageKnown"]:
            languageknown_lst.append(x["Language"])
        dictt["LanguageKnown"]=(str(languageknown_lst).replace("[","")).replace("]","") 

        dictt["UniqueID"]=resp["ResumeParserData"]["UniqueID"]

        dictt["LicenseNo"]=resp["ResumeParserData"]["LicenseNo"]
        dictt["PassportDetail_PassportNumber"]=resp["ResumeParserData"]["PassportDetail"]["PassportNumber"]
        dictt["PassportDetail_PlaceOfIssue"]=resp["ResumeParserData"]["PassportDetail"]["PlaceOfIssue"]
        dictt["PassportDetail_DateOfIssue"]=resp["ResumeParserData"]["PassportDetail"]["DateOfIssue"]
        dictt["PassportDetail_DateOfExpiry"]=resp["ResumeParserData"]["PassportDetail"]["DateOfExpiry"]
        dictt["PanNo"]=resp["ResumeParserData"]["PanNo"]
        dictt["VisaStatus"]=resp["ResumeParserData"]["VisaStatus"]

        email_lst=[]
        for x in resp["ResumeParserData"]["Email"]:
            email_lst.append(x["EmailAddress"])
        dictt["EmailAddress"]=(str(email_lst).replace("[","")).replace("]","")


        # PhoneNumber_lst=[]
        # for x in resp["ResumeParserData"]["PhoneNumber"]:
        #     PhoneNumber_lst.append(x["Number"])
        # dictt["PhoneNumber_Number"]=str(PhoneNumber_lst )


        # PhoneNumber_ISD_Code_lst=[]
        # for x in resp["ResumeParserData"]["PhoneNumber"]:
        #     PhoneNumber_ISD_Code_lst.append(x["ISDCode"])
        # dictt["PhoneNumber_ISDCode"]=str(PhoneNumber_ISD_Code_lst) 


        PhoneNumber_FormattedNumber_lst=[]
        for x in resp["ResumeParserData"]["PhoneNumber"]:
            PhoneNumber_FormattedNumber_lst.append(x["FormattedNumber"])
        dictt["PhoneNumber_FormattedNumber"]=(str(PhoneNumber_FormattedNumber_lst).replace("[","")).replace("]","")


        # PhoneNumber_Type_lst=[] #deprecated
        # for x in resp["ResumeParserData"]["PhoneNumber"]:
        #     PhoneNumber_FormattedNumber_lst.append(x["Type"])
        # dictt["PhoneNumber_Type"]=str(PhoneNumber_Type_lst) 


        WebSite_lst=[]
        for x in resp["ResumeParserData"]["WebSite"]:
            WebSite_lst.append(x["Type"])
        dictt["WebSite"]=(str(WebSite_lst).replace("[","")).replace("]","")
        
        ##Adress street#####
        y=1
        for x in resp["ResumeParserData"]["Address"]:
            if y<3:
                dictt["Address_Street"+str(y)]=x["Street"] 
                y=y+1
            else:
                print("address columns are full")
        ##Adress street#####

        ##Adress City#####
        y=1
        for x in resp["ResumeParserData"]["Address"]:
            if y<3:
                dictt["Address_City"+str(y)]=x["City"] 
                y=y+1
            else:
                print("address columns are full")
        ##Adress City#####

        ##Adress State#####
        y=1
        for x in resp["ResumeParserData"]["Address"]:
            if y<3:
                dictt["Address_State"+str(y)]=x["State"] 
                y=y+1
            else:
                print("address columns are full")
        ##Adress State#####

        y=1
        for x in resp["ResumeParserData"]["Address"]:
            if y<3:
                dictt["Address_Country"+str(y)]=x["Country"] 
                y=y+1
            else:
                print("address columns are full")

        ##Adress ZipCode#####
        y=1
        for x in resp["ResumeParserData"]["Address"]:
            if y<3:
                dictt["Address_ZipCode"+str(y)]=x["ZipCode"] 
                y=y+1
            else:
                print("address columns are full")
        ##Adress ZipCode#####

        # Address_FormattedAddress_lst=[]   #deprecated
        # for x in resp["ResumeParserData"]["Address"]:
        #     Address_FormattedAddress_lst.append(x["FormattedAddress"])
        # dictt["Address_FormattedAddress"]=str(Address_FormattedAddress_lst) 

        # Address_Type_lst=[]  #deprecated
        # for x in resp["ResumeParserData"]["Address"]:
        #     Address_Type_lst.append(x["Type"])
        #dictt["Address_Type"]=str(Address_Type_lst) 


        
        dictt["Category"]=resp["ResumeParserData"]["Category"]
        dictt["SubCategory"]=resp["ResumeParserData"]["SubCategory"]
        dictt["CurrentSalary_Amount"]=resp["ResumeParserData"]["CurrentSalary"]["Amount"]
        #dictt["CurrentSalary_Symbol"]=str(resp["ResumeParserData"]["CurrentSalary"]["Symbol"])
        dictt["CurrentSalary_Currency"]=resp["ResumeParserData"]["CurrentSalary"]["Currency"]   
        #dictt["CurrentSalary_Unit"]=str(resp["ResumeParserData"]["CurrentSalary"]["Unit"])
        #dictt["CurrentSalary_Text"]=str(resp["ResumeParserData"]["CurrentSalary"]["Text"])
        dictt["ExpectedSalary_Amount"]=resp["ResumeParserData"]["ExpectedSalary"]["Amount"]
        #dictt["ExpectedSalary_Symbol"]=str(resp["ResumeParserData"]["ExpectedSalary"]["Symbol"])
        #dictt["ExpectedSalary_Currency"]=str(resp["ResumeParserData"]["ExpectedSalary"]["Currency"])
        #dictt["ExpectedSalary_Unit"]=str(resp["ResumeParserData"]["ExpectedSalary"]["Unit"])
        #dictt["ExpectedSalary_Text"]=str(resp["ResumeParserData"]["ExpectedSalary"]["Text"])
        dictt["Qualification"]=resp["ResumeParserData"]["Qualification"]

        y=1
        for x in resp["ResumeParserData"]["SegregatedQualification"]:
            if y<4:
                dictt["SegregatedQualification_Institution_Name"+str(y)]=x["Institution"]["Name"] #university name
                y=y+1
            else:
                print("SegregatedQualification_Institution_Name columns are full")
        y=1
        for x in resp["ResumeParserData"]["SegregatedQualification"]:
            if y<4:
                dictt["SegregatedQualification_Institution_Location_City"+str(y)]=x["Institution"]["Location"]["City"] 
                y=y+1
            else:
                print("SegregatedQualification_Location_City columns are full")
        y=1
        for x in resp["ResumeParserData"]["SegregatedQualification"]:
            if y<4:
                dictt["SegregatedQualification_Institution_Location_State"+str(y)]=x["Institution"]["Location"]["State"] 
                y=y+1
            else:
                print("SegregatedQualification_Location_State columns are full")
        y=1
        for x in resp["ResumeParserData"]["SegregatedQualification"]:
            if y<4:
                dictt["SegregatedQualification_Institution_Location_Country"+str(y)]=x["Institution"]["Location"]["Country"] 
                y=y+1
            else:
                print("SegregatedQualification_Location_Country columns are full")

        y=1
        for x in resp["ResumeParserData"]["SegregatedQualification"]:
            if y<4:
                try:
                    dictt["SegregatedQualification_SubInstitution_Name"+str(y)]=x["SubInstitution"]["Name"]
                except:
                    print("sub institution not found")
                y=y+1
            else:
                print("SegregatedQualification_Institution_Name columns are full")

        y=1
        for x in resp["ResumeParserData"]["SegregatedQualification"]:
            if y<4:
                try:
                    dictt["SegregatedQualification_SubInstitution_Location_City"+str(y)]=x["SubInstitution"]["Location"]["City"] 
                except:
                    print("sub institution not found")
                y=y+1
            else:
                print("SegregatedQualification_SubInstitution_Location_City columns are full")

        y=1
        for x in resp["ResumeParserData"]["SegregatedQualification"]:
            if y<4:
                try:
                    dictt["SegregatedQualification_SubInstitution_Location_State"+str(y)]=x["SubInstitution"]["Location"]["State"] 
                except:
                    print("sub institution not found")
                y=y+1
            else:
                print("SegregatedQualification_SubInstitution_Location_State columns are full")
        y=1
        for x in resp["ResumeParserData"]["SegregatedQualification"]:
            if y<4:
                try:
                    dictt["SegregatedQualification_SubInstitution_Location_Country"+str(y)]=x["SubInstitution"]["Location"]["Country"] 
                except:
                    print("sub institution not found")
                y=y+1
            else:
                print("SegregatedQualification_SubInstitution_Location_Country columns are full")
        # y=1
        # for x in resp["ResumeParserData"]["SegregatedQualification"]:
        #     if y<4:
        #         dictt["SegregatedQualification_Institution_Type"+str(y)]=x["Institution"]["Type"] 
        #         y=y+1
        #     else:
        #         print("SegregatedQualification_Institution_Name columns are full")

        y=1
        for x in resp["ResumeParserData"]["SegregatedQualification"]:
            if y<4:
                dictt["SegregatedQualification_Degree_DegreeName"+str(y)]=x["Degree"]["DegreeName"] #DegreeName
                y=y+1
            else:
                print("SegregatedQualification_Degree_Name columns are full")

        y=1
        for x in resp["ResumeParserData"]["SegregatedQualification"]:
            if y<4:
                dictt["SegregatedQualification_StartDate"+str(y)]=x["StartDate"]
                y=y+1
            else:
                print("SegregatedQualification_StartDate columns are full")

        y=1
        for x in resp["ResumeParserData"]["SegregatedQualification"]:
            if y<4:
                dictt["SegregatedQualification_EndDate"+str(y)]=x["EndDate"]
                y=y+1
            else:
                print("SegregatedQualification_EndDate columns are full")
        y=1
        for x in resp["ResumeParserData"]["SegregatedQualification"]:
            if y<4:
                dictt["SegregatedQualification_Aggregate"+str(y)]=x["Aggregate"]["Value"]
                dictt["SegregatedQualification_MeasureType"+str(y)]=x["Aggregate"]["MeasureType"]
                y=y+1
            else:
                print("SegregatedQualification_Aggregate columns are full")


        # SegregatedQualification_FormattedDegreePeriod_lst=[]
        # for x in resp["ResumeParserData"]["SegregatedQualification"]:
        #     SegregatedQualification_FormattedDegreePeriod_lst.append(x["FormattedDegreePeriod"])
        # dictt["SegregatedQualification_FormattedDegreePeriod"]=str(SegregatedQualification_FormattedDegreePeriod_lst)

        dictt["Certification"]=str(resp["ResumeParserData"]["Certification"])
        SegregatedCertification_Title_lst=[]
        for x in resp["ResumeParserData"]["SegregatedCertification"]:
            SegregatedCertification_Title_lst.append(x["CertificationTitle"])
        dictt["SegregatedCertification_Title"]=(str(SegregatedCertification_Title_lst).replace("[","")).replace("]","") 
        dictt["SkillBlock"]=resp["ResumeParserData"]["SkillBlock"]
        dictt["SkillKeywords"]=resp["ResumeParserData"]["SkillKeywords"]

        SegregatedSkill_Skill_lst=[]
        for x in resp["ResumeParserData"]["SegregatedSkill"]:
            SegregatedSkill_Skill_lst.append(x["Skill"])
        dictt["SegregatedSkill_Skill"]=(str(SegregatedSkill_Skill_lst).replace("[","")).replace("]","") 



        dictt["Experience"]=resp["ResumeParserData"]["Experience"]



        # SegregatedExperience_Employer_EmployerName_lst=[]
        # for x in resp["ResumeParserData"]["SegregatedExperience"]:
        #     SegregatedExperience_Employer_EmployerName_lst.append(x["Employer"]["EmployerName"])
        # dictt["SegregatedExperience_Employer_EmployerName"]=str(SegregatedExperience_Employer_EmployerName_lst)
        # SegregatedExperience_Employer_JobProfile_lst=[]
        # for x in resp["ResumeParserData"]["SegregatedExperience"]:
        #     SegregatedExperience_Employer_JobProfile_lst.append(x["JobProfile"]["Title"])
        # dictt["SegregatedExperience_Employer_JobProfile"]=str(SegregatedExperience_Employer_JobProfile_lst)
        # SegregatedExperience_JobPeriod_lst=[]
        # for x in resp["ResumeParserData"]["SegregatedExperience"]:
        #     SegregatedExperience_JobPeriod_lst.append(x["FormattedJobPeriod"])
        # dictt["SegregatedExperience_JobPeriod"]=str(SegregatedExperience_JobPeriod_lst)
        # SegregatedExperience_IsCurrentEmployer_lst=[]
        # for x in resp["ResumeParserData"]["SegregatedExperience"]:
        #     SegregatedExperience_IsCurrentEmployer_lst.append(x["IsCurrentEmployer"])
        # dictt["SegregatedExperience_IsCurrentEmployer"]=str(SegregatedExperience_IsCurrentEmployer_lst)

        y=1
        for x in resp["ResumeParserData"]["SegregatedExperience"]:
            if y<6:
                dictt["SegregatedExperience_Employer_EmployerName"+str(y)]=x["Employer"]["EmployerName"]
                dictt["SegregatedExperience_JobProfile_Title"+str(y)]=x["JobProfile"]["Title"]
                dictt["SegregatedExperience_Location_City"+str(y)]=x["Location"]["City"]
                dictt["SegregatedExperience_Location_State"+str(y)]=x["Location"]["State"]
                dictt["SegregatedExperience_Location_Country"+str(y)]=x["Location"]["Country"]
                dictt["SegregatedExperience_StartDate"+str(y)]=x["StartDate"]
                dictt["SegregatedExperience_EndDate"+str(y)]=x["EndDate"]
                y=y+1
            else:
                print("SegregatedExperience columns are full")




        dictt["CurrentEmployer"]=resp["ResumeParserData"]["CurrentEmployer"]
        dictt["JobProfile"]=resp["ResumeParserData"]["JobProfile"]
        dictt["WorkedPeriod_TotalExperienceInMonths"]=resp["ResumeParserData"]["WorkedPeriod"]["TotalExperienceInMonths"]
        dictt["WorkedPeriod_TotalExperienceInYear"]=resp["ResumeParserData"]["WorkedPeriod"]["TotalExperienceInYear"]
        dictt["WorkedPeriod_TotalExperienceRange"]=resp["ResumeParserData"]["WorkedPeriod"]["TotalExperienceRange"]
        dictt["WorkedPeriod_GapPeriod"]=resp["ResumeParserData"]["GapPeriod"]
        dictt["AverageStay"]=resp["ResumeParserData"]["AverageStay"]
        dictt["LongestStay"]=resp["ResumeParserData"]["LongestStay"]
        dictt["Summary"]=resp["ResumeParserData"]["Summary"]
        dictt["ExecutiveSummary"]=resp["ResumeParserData"]["ExecutiveSummary"]
        dictt["ManagementSummary"]=resp["ResumeParserData"]["ManagementSummary"]
        dictt["Coverletter"]=resp["ResumeParserData"]["Coverletter"]
        dictt["Publication"]=resp["ResumeParserData"]["Publication"]


        CurrentLocation_City_lst=[]
        CurrentLocation_State_lst=[]
        for x in resp["ResumeParserData"]["CurrentLocation"]:
            CurrentLocation_City_lst.append(x["City"])
            CurrentLocation_State_lst.append(x["State"])
        dictt["CurrentLocation_City"]=(str(CurrentLocation_City_lst).replace("[","")).replace("]","") 
        dictt["CurrentLocation_State"]=(str(CurrentLocation_State_lst).replace("[","")).replace("]","") 

        PreferredLocation_City_lst=[]
        PreferredLocation_State_lst=[]
        for x in resp["ResumeParserData"]["PreferredLocation"]:
            PreferredLocation_City_lst.append(x["City"])
            PreferredLocation_State_lst.append(x["State"])
        dictt["PreferredLocation_City"]=(str(PreferredLocation_State_lst).replace("[","")).replace("]","") 
        dictt["PreferredLocation_State"]=(str(PreferredLocation_State_lst).replace("[","")).replace("]","")


        # dictt["PreferredLocation_City"]=str(PreferredLocation_City_lst)
        dictt["Availability"]=resp["ResumeParserData"]["Availability"]
        dictt["Hobbies"]=resp["ResumeParserData"]["Hobbies"]
        dictt["Objectives"]=resp["ResumeParserData"]["Objectives"]
        dictt["Achievements"]=resp["ResumeParserData"]["Achievements"]
        SegregatedAchievement_AwardTitle_lst=[]
        for x in resp["ResumeParserData"]["SegregatedAchievement"]:
            SegregatedAchievement_AwardTitle_lst.append(x["AwardTitle"])
        dictt["SegregatedAchievement_AwardTitle"]=(str(SegregatedAchievement_AwardTitle_lst).replace("[","")).replace("]","") 

        # dictt["References"]=str(resp["ResumeParserData"]["References"])
        # dictt["CustomFields"]=str(resp["ResumeParserData"]["CustomFields"])
        # dictt["ApiInfo_Metered"]=str(resp["ResumeParserData"]["ApiInfo"]["Metered"])
        # dictt["ApiInfo_CreditLeft"]=str(resp["ResumeParserData"]["ApiInfo"]["CreditLeft"])
        # dictt["ApiInfo_AccountExpiryDate"]=str(resp["ResumeParserData"]["ApiInfo"]["AccountExpiryDate"])        
        print("step 5 : Pandas reading json")
        df=pd.DataFrame(dictt,index=[0])
        excel_conversion="resume_data.xlsx"
        source_excel_conversion="/tmp/"+destination_file_name+'.xlsx'#cf
        #source_excel_conversion=destination_file_name+'.xlsx' #local env
        

        # print("step 6 : creating excel file")
        # df.to_excel(source_excel_conversion)
    
        # upload_blob("rchilli_excel_data",source_excel_conversion,"resumes/"+excel_conversion)
        # os.remove(source_excel_conversion)
        print("step 6 : truncating bigquery staging")
        truncate_stg = """TRUNCATE TABLE  rchilli-etl.staging.resume_data_stg;"""
        load_job_trunc = client.query(truncate_stg)
        load_job_trunc.result()
        

        print("step 7 : loading dataframe into bigquery staging")
        load_job = client.load_table_from_dataframe(
        df, table, job_config=job_config
        )
        load_job.result()
        

        print("step 8 : loading bigquery main")
        loading_main = """CALL `rchilli-etl.resumes.sp_load_Resume_data`();"""
        load_job_main = client.query(loading_main)
        load_job_main.result()

        print("step 10 : creating excel file")
        query = """SELECT  * FROM `rchilli-etl.resumes.Resume_data2` ;"""
        load_job_excel = client.query(query)
        load_job_excel.result()
        df_excel=load_job_excel.to_dataframe()
        df_excel.to_excel(source_excel_conversion)

        upload_blob("rchilli_excel_data",source_excel_conversion,"resumes/"+excel_conversion)
        os.remove(source_excel_conversion)
        
    except Exception as e:
        print("exception main:  ", e)
        return("exception main:  ", e)

