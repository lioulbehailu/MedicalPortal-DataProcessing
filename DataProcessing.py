import recordlinkage as p
import pandas as pd
from recordlinkage.index import Block
from recordlinkage.datasets import load_febrl4
from recordlinkage.datasets import load_krebsregister

'''
Import the Mongo MedicalPortal Database
'''
from pymongo import MongoClient
client = MongoClient()
db = client.MedicalPortal
PatientDatabase = db.patients


krebs_X, krebs_true_links = load_krebsregister(missing_values=0)

golden_pairs = krebs_X[0:5000]
golden_matches_index = golden_pairs.index & krebs_true_links # 2093 matching pairs

def ProcessData(patientDataList, fetchedHospitalData):
    # Read from the directory 
    filelist = pd.read_csv('/home/bizzzzzzzzzzzzu/Music/MedicalPortal/MedicPortal DataProcessing/FetchedData/'+fetchedHospitalData)
    
    
    # Indexation step
    indexer = p.Index()
    indexer.add(Block(left_on='fatherName',right_on='fatherName'))
    candidate_links = indexer.index(patientDataList,filelist)
    
    # print((candidate_links))

    # Comparison step
    compare_cl = p.Compare()

    # compare_cl.exact('_id','_id',label='_id')
    compare_cl.exact('name','name', label='name')
    compare_cl.exact('fatherName','fatherName',label='fatherName')
    compare_cl.exact('grandFatherName','grandFatherName',label='grandFatherName')
    compare_cl.exact('gender','gender',label='gender')
    compare_cl.exact('dateOfBirth','dateOfBirth',label='dateOfBirth')
    compare_cl.exact('dayOfBirth','dayOfBirth',label='dayOfBirth')
    compare_cl.exact('monthOfBirth','monthOfBirth',label='monthOfBirth')
    compare_cl.exact('yearOfBirth','yearOfBirth',label='yearOfBirth')
    compare_cl.exact('age','age',label='age')
    # compare_cl.exact('address','address',label='address')
    # compare_cl.exact('phoneNumber','phoneNumber',label='phoneNumber')

    features = compare_cl.compute(candidate_links, patientDataList, filelist)

    if features.empty:
        return None
    else:
        
        # Classification step
        '''
            Use the KMeans Classifier
            This classifier is equivalent to the Unsupervised record linkage approach
        '''
        
        # # classifier = p.LogisticRegressionClassifier(coefficients=coefficients,intercept=intercept)
        classifier = p.LogisticRegressionClassifier()    
        classifier.fit(golden_pairs,golden_matches_index )
        
        links = classifier.predict(features)
        
        return links
