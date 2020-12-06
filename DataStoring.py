import os
import DataProcessing

import pandas as pd
import json

'''
    Import the Mongo MedicalPortal Database
'''
from pymongo import MongoClient
client = MongoClient()
db = client.MedicalPortal
PatientDatabase = db.patients

# data = PatientDatabase.find({})
# print(data[0])

# Read all files from the directory
files_list = os.listdir(r'/home/bizzzzzzzzzzzzu/Music/MedicalPortal/MedicPortal DataProcessing/FetchedData')


def tuple_comp(df):
    return [tuple(x) for x in df.to_numpy()]

def StoreData():

    patientDBSize = PatientDatabase.count_documents({})
    print(patientDBSize)

    if patientDBSize > 0:

        print('Patient COllection filled',patientDBSize)
        
        # Get all patient document From the database and change to Panda DataFrame
        patientData = PatientDatabase.find({})
        
        data = pd.DataFrame(list(patientData))

        for item in files_list:
            result = pd.read_csv('/home/bizzzzzzzzzzzzu/Music/MedicalPortal/MedicPortal DataProcessing/FetchedData/'+item)
            matchedResult = DataProcessing.ProcessData(data,item)

            # Link the datas if there are any matches between the stored and the fetched
            
            if matchedResult is None:
                print('None is Matched')

                '''Create New Document in Patients Collection'''

                # result = pd.read_csv('/home/bizzzzzzzzzzzzu/Music/MedicalPortal/MedicPortal DataProcessing/FetchedData/'+item)
                
                # remove the _id column so no redendency appear
                del result['_id']
                # print('done creating new',hospitalData)
                PatientDatabase.insert_many(result.to_dict('records'))
                

            else:
                # Update the Patient Document
                print('Possible Matches',matchedResult, item)
                
                findPatientData = PatientDatabase.find({})
                    
                for matchList in matchedResult:
                    # print(matchList[0],matchList[1])

                    # Get the history from the left data frame
                    history = json.loads(result.iloc[matchList[1]]['history'])

                    id = findPatientData[matchList[0]]['_id']
                    PatientDatabase.find_one_and_update({'_id':id},{'$push':{'history':history[0]}},upsert=True)

    else:
        hospitalData = pd.read_csv('/home/bizzzzzzzzzzzzu/Music/MedicalPortal/MedicPortal DataProcessing/FetchedData/'+files_list[0])
        del hospitalData['_id']

        print(type(hospitalData))
        for i in range(0,len(hospitalData)):
            history = json.loads(hospitalData.iloc[i]['history'])
            hospitalData.loc[i,'history']=[history]
            print('EDITED LIST',hospitalData.iloc[i])

            # print(hospitalData)
        PatientDatabase.insert_many(hospitalData.to_dict('records'))
    
    return True