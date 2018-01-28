import utils
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import AdaBoostRegressor
from sklearn.naive_bayes import GaussianNB
from sklearn.metrics import *
from sklearn.svm import LinearSVC,SVC
import pandas as pd
import numpy as np
from datetime import timedelta
from sklearn import preprocessing

#Note: You can reuse code that you wrote in etl.py and models.py and cross.py over here. It might help.
# PLEASE USE THE GIVEN FUNCTION NAME, DO NOT CHANGE IT

'''
You may generate your own features over here.
Note that for the test data, all events are already filtered such that they fall in the observation window of their respective patients. Thus, if you were to generate features similar to those you constructed in code/etl.py for the test data, all you have to do is aggregate events for each patient.
IMPORTANT: Store your test data features in a file called "test_features.txt" where each line has the
patient_id followed by a space and the corresponding feature in sparse format.
Eg of a line:
60 971:1.000000 988:1.000000 1648:1.000000 1717:1.000000 2798:0.364078 3005:0.367953 3049:0.013514
Here, 60 is the patient id and 971:1.000000 988:1.000000 1648:1.000000 1717:1.000000 2798:0.364078 3005:0.367953 3049:0.013514 is the feature for the patient with id 60.

Save the file as "test_features.txt" and save it inside the folder deliverables

input:
output: X_train,Y_train,X_test
'''
def my_features(filtered_events,feature_map):
    #aggregate test data
    idx_events = pd.merge(filtered_events,feature_map, on = 'event_id')
    idx_events = idx_events[['patient_id','idx','value']]
    idx_events = idx_events[pd.notnull(idx_events['value'])]
    
    sum_events = idx_events[idx_events['idx'] > 2680]
    count_events = idx_events[idx_events['idx'] <=2680]
    
    sum_group = sum_events.groupby(['patient_id','idx']).agg(sum)
    sum_group.reset_index(inplace = True) 
    count_group = count_events.groupby(['patient_id','idx']).agg('count')
    count_group.reset_index(inplace = True)
    
    #min-max normalization 
    sum_group['value'] = sum_group['value'].values.astype(float)
    min_max_scaler = preprocessing.MinMaxScaler()
    sum_scaled = min_max_scaler.fit_transform(sum_group[['value']])
    sum_group['value'] = sum_scaled
    
    count_group['value'] = count_group['value'].values.astype(float)
    count_scaled = min_max_scaler.fit_transform(count_group[['value']])
    count_group['value'] = count_scaled
    #concat sum feature values and count feature values
    aggregated_events = pd.concat([sum_group,count_group]).reset_index(drop = True)
    #rename columns
    aggregated_events = aggregated_events.rename(columns = {'idx':'feature_id','value':'feature_value'})
    
    #create features
    patient_features = aggregated_events
    patient_features['zipped'] = list(zip(patient_features.feature_id, patient_features.feature_value ))
    #drop feature_id and feature_value
    patient_features = patient_features.drop(['feature_id','feature_value'], axis = 1)
    #form the dict
    patient_features  = {k : list(v) for k,v in patient_features.groupby('patient_id')['zipped']}
    
    #write to text_features.txt
    deliverable3 = open('../deliverables/test_features.txt','wb')
    deliverable1 = open('../deliverables/test_features.train','wb')
    n=0
    for key in sorted(patient_features):
        line = "%d" %(key)
        line1 = "%d" %(0)
        for value in sorted(patient_features[key]):
            pairs = "%d:%.7f" %(value[0], value[1])
            line = line + " "+pairs+" "
            line1 = line1 + " "+pairs+" "
        deliverable3.write(line+"\n")
        deliverable1.write(line1+"\n")
        n = n+1
        if n > 632:
            break   
        
    


'''
You can use any model you wish.

input: X_train, Y_train, X_test
output: Y_pred
'''
def my_classifier_predictions(X_train,Y_train,X_test):
	#TODO: complete this
    
    regr_1 = DecisionTreeRegressor(max_depth=4)
    regr_2 = AdaBoostRegressor(regr_1,n_estimators= 600)
    regr_2.fit(X_train,Y_train)
    Y_pred = regr_2.predict(X_test)
    
    return Y_pred

 


def main():
    test_events = pd.read_csv('../data/test/events.csv')
    feature_map = pd.read_csv('../data/test/event_feature_map.csv')
    my_features(test_events,feature_map)
    X_train, Y_train = utils.get_data_from_svmlight("../deliverables/features_svmlight.train")
    X_test, Y_test = utils.get_data_from_svmlight("../deliverables/test_features.train")
    Y_pred= my_classifier_predictions(X_train,Y_train,X_test)
    utils.generate_submission("../deliverables/test_features.txt",Y_pred)
#The above function will generate a csv file of (patient_id,predicted label) and will be saved as "my_predictions.csv" in the deliverables folder.

if __name__ == "__main__":
    main()

	