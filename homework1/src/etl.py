import pandas as pd
import numpy as np
from datetime import timedelta
from sklearn import preprocessing

# PLEASE USE THE GIVEN FUNCTION NAME, DO NOT CHANGE IT

def read_csv(filepath):
    
    '''
    TODO: This function needs to be completed.
    Read the events.csv, mortality_events.csv and event_feature_map.csv files into events, mortality and feature_map.
    
    Return events, mortality and feature_map
    '''

    #Columns in events.csv - patient_id,event_id,event_description,timestamp,value
    events = pd.read_csv(filepath+'events.csv')
    
    #Columns in mortality_event.csv - patient_id,timestamp,label
    mortality = pd.read_csv(filepath+'mortality_events.csv')

    #Columns in event_feature_map.csv - idx,event_id
    feature_map = pd.read_csv(filepath+'event_feature_map.csv')

    return events, mortality, feature_map


def calculate_index_date(events, mortality, deliverables_path):
    
    '''
    TODO: This function needs to be completed.

    Refer to instructions in Q3 a

    Suggested steps:
    1. Create list of patients alive ( mortality_events.csv only contains information about patients deceased)
    2. Split events into two groups based on whether the patient is alive or deceased
    3. Calculate index date for each patient
    
    IMPORTANT:
    Save indx_date to a csv file in the deliverables folder named as etl_index_dates.csv. 
    Use the global variable deliverables_path while specifying the filepath. 
    Each row is of the form patient_id, indx_date.
    The csv file should have a header 
    For example if you are using Pandas, you could write: 
        indx_date.to_csv(deliverables_path + 'etl_index_dates.csv', columns=['patient_id', 'indx_date'], index=False)

    Return indx_date
    '''
    dead_dates= mortality[['patient_id','timestamp']]
    dead_dates['timestamp']= pd.to_datetime(dead_dates['timestamp'])
    dead_dates['timestamp']= dead_dates['timestamp']-timedelta(days = 30)
    
    alive_dates = events[['patient_id','timestamp']]
    alive_dates =alive_dates.loc[~alive_dates['patient_id'].isin(mortality['patient_id'])]
    alive_dates = alive_dates.groupby(['patient_id']).max().reset_index()
    
    indx_date = pd.concat([alive_dates,dead_dates]).reset_index(drop=True)
    indx_date = indx_date.rename(columns = {'timestamp':'indx_date'})
    indx_date.to_csv(deliverables_path+'etl_index_dates.csv',columns = ['patient_id','indx_date'],index = False)
    return indx_date


def filter_events(events, indx_date, deliverables_path):
    
    '''
    TODO: This function needs to be completed.

    Refer to instructions in Q3 b

    Suggested steps:
    1. Join indx_date with events on patient_id
    2. Filter events occuring in the observation window(IndexDate-2000 to IndexDate)
    
    
    IMPORTANT:
    Save filtered_events to a csv file in the deliverables folder named as etl_filtered_events.csv. 
    Use the global variable deliverables_path while specifying the filepath. 
    Each row is of the form patient_id, event_id, value.
    The csv file should have a header 
    For example if you are using Pandas, you could write: 
        filtered_events.to_csv(deliverables_path + 'etl_filtered_events.csv', columns=['patient_id', 'event_id', 'value'], index=False)

    Return filtered_events
    '''
    events['timestamp'] = pd.to_datetime(events['timestamp'])
    indx_date['indx_date'] = pd.to_datetime(indx_date['indx_date'])
    events_merge = pd.merge(events,indx_date, on = ['patient_id'])
    filtered_events = events_merge[(events_merge.timestamp <= events_merge.indx_date) & (events_merge.timestamp >= (events_merge.indx_date-timedelta(days = 2000)))]
    filtered_events = filtered_events[['patient_id','event_id','value']]
    filtered_events.to_csv(deliverables_path + 'etl_filtered_events.csv',index = False)
    return filtered_events


def aggregate_events(filtered_events, mortality,feature_map, deliverables_path):
    
    '''
    TODO: This function needs to be completed.

    Refer to instructions in Q3 c

    Suggested steps:
    1. Replace event_id's with index available in event_feature_map.csv
    2. Remove events with n/a values
    3. Aggregate events using sum and count to calculate feature value
    4. Normalize the values obtained above using min-max normalization(the min value will be 0 in all scenarios)
    
    
    IMPORTANT:
    Save aggregated_events to a csv file in the deliverables folder named as etl_aggregated_events.csv. 
    Use the global variable deliverables_path while specifying the filepath. 
    Each row is of the form patient_id, event_id, value.
    The csv file should have a header .
    For example if you are using Pandas, you could write: 
        aggregated_events.to_csv(deliverables_path + 'etl_aggregated_events.csv', columns=['patient_id', 'feature_id', 'feature_value'], index=False)

    Return filtered_events
    '''
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
    #write to csv
    aggregated_events.to_csv(deliverables_path + 'etl_aggregated_events.csv',index = False)
    return aggregated_events

def create_features(events, mortality, feature_map):
    
    deliverables_path = '../deliverables/'

    #Calculate index date
    indx_date = calculate_index_date(events, mortality, deliverables_path)

    #Filter events in the observation window
    filtered_events = filter_events(events, indx_date,  deliverables_path)
    
    #Aggregate the event values for each patient 
    aggregated_events = aggregate_events(filtered_events, mortality, feature_map, deliverables_path)

    '''
    TODO: Complete the code below by creating two dictionaries - 
    1. patient_features :  Key - patient_id and value is array of tuples(feature_id, feature_value)
    2. mortality : Key - patient_id and value is mortality label
    '''
    
    #zip feature_id and feature_value to form a new column
    patient_features = aggregated_events
    patient_features['zipped'] = list(zip(patient_features.feature_id, patient_features.feature_value ))
    #drop feature_id and feature_value
    patient_features = patient_features.drop(['feature_id','feature_value'], axis = 1)
    #form the dict
    patient_features  = {k : list(v) for k,v in patient_features.groupby('patient_id')['zipped']}
    #delete timestamp
    mortality = mortality.drop('timestamp',axis = 1)
    #transform mortality into dict
    mortality = pd.Series(mortality.label.values, index = mortality.patient_id).to_dict()

    return patient_features, mortality

def save_svmlight(patient_features, mortality, op_file, op_deliverable):
    
    '''
    TODO: This function needs to be completed

    Refer to instructions in Q3 d

    Create two files:
    1. op_file - which saves the features in svmlight format. (See instructions in Q3d for detailed explanation)
    2. op_deliverable - which saves the features in following format:
       patient_id1 label feature_id:feature_value feature_id:feature_value feature_id:feature_value ...
       patient_id2 label feature_id:feature_value feature_id:feature_value feature_id:feature_value ...  
    
    Note: Please make sure the features are ordered in ascending order, and patients are stored in ascending order as well.     
    '''
    deliverable1 = open(op_file, 'wb')
    deliverable2 = open(op_deliverable, 'wb')
    
    for key in sorted(patient_features):
        if key in mortality:
            line1 = "%d" %(1)
            line2 = "%d %d" %(key,1)
        else:
            line1 = "%d" %(0)
            line2 = "%d %d" %(key,0)
        for value in sorted(patient_features[key]):
            pairs = "%d:%.7f" %(value[0], value[1])
            line1 = line1 + " "+pairs+" "
            line2 = line2 + " "+pairs+" "
        deliverable1.write(line1+"\n")
        deliverable2.write(line2+"\n")
        

    

def main():
    train_path = '../data/train/'
    events, mortality, feature_map = read_csv(train_path)
    patient_features, mortality = create_features(events, mortality, feature_map)
    save_svmlight(patient_features, mortality, '../deliverables/features_svmlight.train', '../deliverables/features.train')
    
if __name__ == "__main__":
    main()