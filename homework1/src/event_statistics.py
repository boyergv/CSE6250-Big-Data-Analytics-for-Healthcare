import time
import pandas as pd
import numpy as np

# PLEASE USE THE GIVEN FUNCTION NAME, DO NOT CHANGE IT

def read_csv(filepath):
    '''
    TODO : This function needs to be completed.
    Read the events.csv and mortality_events.csv files. 
    Variables returned from this function are passed as input to the metric functions.
    '''
    events = pd.read_csv(filepath + 'events.csv')
    mortality = pd.read_csv(filepath + 'mortality_events.csv')

    return events, mortality

def event_count_metrics(events, mortality):
    '''
    TODO : Implement this function to return the event count metrics.
    Event count is defined as the number of events recorded for a given patient.
    '''

    total_counts = events['patient_id'].value_counts()
    alive_counts = total_counts.drop(mortality['patient_id'])
    dead_counts = total_counts[mortality['patient_id']]
    avg_dead_event_count = dead_counts.mean()
    max_dead_event_count = dead_counts.max()
    min_dead_event_count = dead_counts.min()
    avg_alive_event_count = alive_counts.mean()
    max_alive_event_count = alive_counts.max()
    min_alive_event_count = alive_counts.min()

    return min_dead_event_count, max_dead_event_count, avg_dead_event_count, min_alive_event_count, max_alive_event_count, avg_alive_event_count

def encounter_count_metrics(events, mortality):
    '''
    TODO : Implement this function to return the encounter count metrics.
    Encounter count is defined as the count of unique dates on which a given patient visited the ICU. 
    '''
    unique_dates = events[['patient_id','timestamp']].groupby(['patient_id']).timestamp.nunique()
    alive_dates = unique_dates.drop(mortality['patient_id'])
    dead_dates = unique_dates[mortality['patient_id']]
    avg_dead_encounter_count = dead_dates.mean()
    max_dead_encounter_count = dead_dates.max()
    min_dead_encounter_count = dead_dates.min()
    avg_alive_encounter_count = alive_dates.mean()
    max_alive_encounter_count = alive_dates.max()
    min_alive_encounter_count = alive_dates.min()

    return min_dead_encounter_count, max_dead_encounter_count, avg_dead_encounter_count, min_alive_encounter_count, max_alive_encounter_count, avg_alive_encounter_count

def record_length_metrics(events, mortality):
    '''
    TODO: Implement this function to return the record length metrics.
    Record length is the duration between the first event and the last event for a given patient. 

    '''
    events1 = events
    events1['timestamp'] = pd.to_datetime(events1['timestamp'])
    group = events1[['patient_id','timestamp']].groupby('patient_id').agg([max,min])
    group['days'] = group['timestamp']['max']-group['timestamp']['min']
    group = group['days']       
    alive_dates = group.drop(mortality['patient_id'])
    alive_dates = alive_dates.dt.days
    dead_dates = group[mortality['patient_id']]
    dead_dates = dead_dates.dt.days
    avg_dead_rec_len = dead_dates.mean()
    max_dead_rec_len = dead_dates.max()
    min_dead_rec_len = dead_dates.min()
    avg_alive_rec_len = alive_dates.mean()
    max_alive_rec_len = alive_dates.max()
    min_alive_rec_len = alive_dates.min()

    return min_dead_rec_len, max_dead_rec_len, avg_dead_rec_len, min_alive_rec_len, max_alive_rec_len, avg_alive_rec_len

def main():
    '''
    You may change the train_path variable to point to your train data directory.
    OTHER THAN THAT, DO NOT MODIFY THIS FUNCTION.
    '''
    # You may change the following line to point the train_path variable to your train data directory
    train_path = '../data/train/'

    # DO NOT CHANGE ANYTHING BELOW THIS ----------------------------
    events, mortality = read_csv(train_path)

    #Compute the event count metrics
    start_time = time.time()
    event_count = event_count_metrics(events, mortality)
    end_time = time.time()
    print("Time to compute event count metrics: " + str(end_time - start_time) + "s")
    print event_count

    #Compute the encounter count metrics
    start_time = time.time()
    encounter_count = encounter_count_metrics(events, mortality)
    end_time = time.time()
    print("Time to compute encounter count metrics: " + str(end_time - start_time) + "s")
    print encounter_count

    #Compute record length metrics
    start_time = time.time()
    record_length = record_length_metrics(events, mortality)
    end_time = time.time()
    print("Time to compute record length metrics: " + str(end_time - start_time) + "s")
    print record_length
    
if __name__ == "__main__":
    main()
