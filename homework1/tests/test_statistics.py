from nose.tools import with_setup, eq_, ok_
from src.event_statistics import read_csv, record_length_metrics, encounter_count_metrics, event_count_metrics
import filecmp 
import os, errno

def silentremove(filename):
    """ Copied from the internet. """
    try:
        os.remove(filename)
    except OSError as e: # this would be "except OSError, e:" before Python 2.6
        if e.errno != errno.ENOENT: # errno.ENOENT = no such file or directory
            raise # re-raise exception if a different error occured

def setup_module ():
    global events, mortality
    events, mortality = read_csv('tests/data/statistics/')

def test_event_count():
    event_count = event_count_metrics(events, mortality)
    #print event_count
    assert event_count == (177, 562, 369.5, 238, 1786, 1012.0)

def test_encounter_count():
    encounter_count = encounter_count_metrics(events, mortality)
    #print encounter_count
    assert encounter_count == (5, 14, 9.5, 10, 83, 46.5)

def test_record_length():
    record_length = record_length_metrics(events, mortality)
    #print record_length
    assert record_length == (4, 633, 318.5, 150, 1267, 708.5)

