import recordlinkage as p
import pandas as pd

from recordlinkage.index import Block
from recordlinkage.datasets import load_febrl4
from recordlinkage.datasets import load_krebsregister

krebs_X, krebs_true_links = load_krebsregister(missing_values=0)

golden_pairs = krebs_X[0:5000]
golden_matches_index = golden_pairs.index & krebs_true_links # 2093 matching pairs

#load the jsons from file
patient1 = pd.read_csv('/home/bizzzzzzzzzzzzu/Music/MedicalPortal/MedicPortal DataProcessing/FetchedData/Black Lion Referral Hospital.csv')
patient2 = pd.read_csv('/home/bizzzzzzzzzzzzu/Music/MedicalPortal/MedicPortal DataProcessing/FetchedData/Adama General Referral Hospital .csv')

def ProcessData():


    # #Indexation step
    indexer = p.Index()
    indexer.add(Block(left_on='name',right_on='name'))
    indexer.add(Block(left_on='fatherName',right_on='fatherName'))
    # indexer.add(Block(left_on='gender',right_on='gender'))
    candidate_links = indexer.index(patient1,patient2)
    # print(candidate_links)

    # #Comparison step
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

    features = compare_cl.compute(candidate_links, patient1, patient2)



    # #Classification step

    intercept = -11.0
    # # coefficients = [1.5, 1.5, 8.0, 6.0, 2.5, 6.5, 5.0]
    coefficients = [1.0, 8.0, 8.0, 8.0, 9.0,9.0,9.0,9.0,9.0,9.0,1.0,1.0]

    # # use the Logistic Regression Classifier
    # # this classifier is equivalent to the deterministic record linkage approach
    classifier = p.LogisticRegressionClassifier(coefficients=coefficients,intercept=intercept)
    classifier.fit(golden_pairs,golden_matches_index )
    
    links = classifier.predict(features)

    # print('Detail of the classification',links)

    # classifier=p.SVMClassifier()
    # classifier.fit(golden_pairs,golden_matches_index)

    links = classifier.predict(features)
    print(links)
    return links 