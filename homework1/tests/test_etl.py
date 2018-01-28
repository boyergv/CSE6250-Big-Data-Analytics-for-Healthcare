from nose.tools import with_setup, eq_, ok_,nottest
from src.etl import read_csv, create_features, save_svmlight, calculate_index_date, filter_events, aggregate_events
import datetime 
import pandas as pd
import datetime
import filecmp 
import os, errno

def silentremove(filename):
    """ Copied from the internet. """
    try:
        os.remove(filename)
    except OSError as e: # this would be "except OSError, e:" before Python 2.6
        if e.errno != errno.ENOENT: # errno.ENOENT = no such file or directory
            raise # re-raise exception if a different error occured

VALIDATION_FEATURES = "tests/features_svmlight.train"
VALIDATION_DELIVERABLE = "tests/features.train"

def setup_module ():
    global deliverables_path 
    deliverables_path = 'tests/'

@nottest
def teardown_features_order():
    silentremove (VALIDATION_FEATURES)
    silentremove (VALIDATION_DELIVERABLE)

@nottest
def date_convert(x):
    return datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S')

@nottest
def setup_index_date ():
    global events_df, mortality_df,feature_map_df
    events_df, mortality_df,feature_map_df = read_csv('tests/data/etl_1/')    

@nottest
def teardown_index_date ():
    silentremove (deliverables_path + 'etl_index_dates.csv')

@nottest
def setup_filtered_events ():
    global events_df, mortality_df,feature_map_df
    events_df, mortality_df,feature_map_df = read_csv('tests/data/etl_2/')    

@nottest
def teardown_filtered_events ():
    teardown_index_date()
    silentremove (deliverables_path + 'etl_filtered_events.csv')

@nottest
def setup_aggregate_events ():
    global events_df, mortality_df,feature_map_df
    events_df, mortality_df,feature_map_df = read_csv('tests/data/etl_3/')    

@nottest
def teardown_aggregate_events ():
    teardown_filtered_events()
    silentremove (deliverables_path + 'etl_aggregated_events.csv')

@with_setup (setup_index_date, teardown_index_date)
def test_index_date ():
    
    expected_indx_dates = {19: datetime.datetime(2014, 2, 2, 0, 0), 99: datetime.datetime(2013, 10, 13, 0, 0), 24581: datetime.datetime(2013, 11, 07, 0, 0), 3014: datetime.datetime(2015, 11, 17, 0, 0)}
    calculate_index_date(events_df, mortality_df, deliverables_path)
    
    indx_date_df = pd.read_csv(deliverables_path + 'etl_index_dates.csv', parse_dates=['indx_date'])

    indx_date = dict(zip(indx_date_df.patient_id, indx_date_df.indx_date))

    if isinstance(indx_date, pd.DataFrame):
        indx_date =  dict(zip(indx_date.patient_id, indx_date.indx_date))
    res= True
    
    if len(indx_date) != len(expected_indx_dates):
        res = False
    else: 
        for key, value in expected_indx_dates.iteritems():
            if key  not in  indx_date:
                res = False;
                break;
            if not abs(indx_date[key].date() == value.date()):
                res = False;
                break;

    eq_(res, True, "Index dates do not match")

@with_setup (setup_filtered_events, teardown_filtered_events)
def test_filtered_events ():
    
    expected_data = []
    with open('tests/expected_etl_filtered_events.csv') as expected_file:
        expected_data = expected_file.readlines()
        expected_data = expected_data[1:]
        expected_data.sort()

    indx_dates_df = calculate_index_date(events_df, mortality_df, deliverables_path)
    filter_events(events_df, indx_dates_df, deliverables_path)
    actual_data = []
    with open(deliverables_path + 'etl_filtered_events.csv') as actual_file:
        actual_data = actual_file.readlines()
        actual_data = actual_data[1:]
        actual_data.sort()

    res = True
    msg = ""
    for idx,line in enumerate(expected_data):
        first = line.split(',')
        second = actual_data[idx].split(',')
        if not (float(first[0])==float(second[0]) and first[1] == second[1] and float(first[2])==float(second[2])):
            res = False
            msg = "Mistmatch on line %d. \n\nExpected: %s  \nActual: %s " %(idx+1, line, actual_data[idx])
            break   

    eq_(res, True, "Filtered events do not match. " + msg)

@with_setup (setup_aggregate_events, teardown_aggregate_events)
def test_aggregate_events ():
    
    expected_data = []
    with open('tests/expected_etl_aggregated_events.csv') as expected_file:
        expected_data = expected_file.readlines()
        expected_data = expected_data[1:]
        expected_data.sort()

    indx_dates_df = calculate_index_date(events_df, mortality_df, deliverables_path)
    filtered_events_df = filter_events(events_df, indx_dates_df,  deliverables_path)
    aggregated_events_df = aggregate_events(filtered_events_df, mortality_df, feature_map_df, deliverables_path)
    
    actual_data = []
    with open(deliverables_path + 'etl_aggregated_events.csv') as actual_file:
        actual_data = actual_file.readlines()
        actual_data = actual_data[1:]
        actual_data.sort()

    res = True
    msg = ""
    for idx,line in enumerate(expected_data):
        first = line.split(',')
        second = actual_data[idx].split(',')
        if not (float(first[0])==float(second[0]) and float(first[1]) == float(second[1]) and abs(float(first[2])-float(second[2]))<=0.1):
            res = False
            msg = "Mistmatch on line %d. \n\nExpected: %s  \nActual: %s " %(idx+1, line, actual_data[idx])
            break    
    eq_(res, True, "Aggregated events do not match. " + msg)

@with_setup (None, teardown_features_order)
def test_features_order():
    patient_features = {2293.0: [(2741.0, 1.0), (2751.0, 1.0), (2760.0, 1.0), (2841.0, 1.0), (2880.0, 1.0), (2914.0, 1.0), (2948.0, 1.0), (3008.0, 1.0), (3049.0, 1.0), (1193.0, 1.0), (1340.0, 1.0), (1658.0, 1.0), (1723.0, 1.0), (2341.0, 1.0), (2414.0, 1.0)]}
    mortality = {2293.0: 1}
    save_svmlight(patient_features, mortality, VALIDATION_FEATURES, VALIDATION_DELIVERABLE)
    result = filecmp.cmp('tests/expected_features.train', VALIDATION_DELIVERABLE)
    eq_(True, result, "Features are not same")
