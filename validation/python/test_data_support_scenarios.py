import os
import ast
import random
import string
import sys

from cate.core.ds import DATA_STORE_REGISTRY
from cate.core import ds
import pytest
import time
import csv
from csv import DictWriter
from datetime import datetime

data_store = DATA_STORE_REGISTRY.get_data_store('esa_cci_odp_os')
lds = DATA_STORE_REGISTRY.get_data_store('local')
header_of_scenarios = ['dataset_collection', 'temporal_subset', 'variables_subset', 'spatial_subset']
with open("test_scenarios.csv") as csvfile:
    reader = csv.DictReader(csvfile, fieldnames=header_of_scenarios, delimiter=';')
    data_sets = []
    firstline = True
    for row in reader:
        if firstline:    #skip first line
            firstline = False
            continue
        data_sets.append(row)
# header for CSV report
header_row = ['dataset_collection', 'open_local', 'duration_open_local_s', 'open_remote',
              'duration_open_remote_s', 'time_coverage_of_collection', 'testing_time_range',
              'spatial_subset', 'variables_subset',
              'no_of_time_stamps_included', 'tot_no_of_files_in_collection']

results_csv = f'test_data_support_scenarios_results_{datetime.date(datetime.now())}.csv'


# Utility functions
def get_time_range(time_range_str):
    start_date =time_range_str.split(',')[0].replace('(', '').replace("'", "")
    end_date = time_range_str.split(',')[1].replace(')', '').replace("'", "").replace(" ", "")
    time_range = (datetime.strptime(start_date, '%Y-%m-%d'), datetime.strptime(end_date, '%Y-%m-%d'))
    return time_range

def append_dict_as_row(file_name, dict_of_elem, field_names):
    # Open file in append mode
    with open(file_name, 'a+', newline='') as write_obj:
        # Create a writer object from csv module
        dict_writer = DictWriter(write_obj, fieldnames=field_names)
        # Add dictionary as wor in the csv
        dict_writer.writerow(dict_of_elem)


def update_csv(results_csv, header_row, results_for_dataset_collection):
    if not os.path.isfile(results_csv):
        with open(results_csv, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(header_row)
    append_dict_as_row(results_csv, results_for_dataset_collection, header_row)


results_for_ds_collection = {}


# Fixtures
@pytest.fixture(scope="function", params=(line for line in data_sets))
def remote_dataset(request, record_xml_attribute, record_property):
    # leaving the xml stuff inside for the moment, might need it later.

    dataset = data_store.query(request.param['dataset_collection'])[0]
    results_for_ds_collection['dataset_collection'] = dataset.id
    record_xml_attribute('dataset', dataset.id)

    time_range_string = request.param['temporal_subset']
    time_range = get_time_range(time_range_string)
    results_for_ds_collection['testing_time_range'] = tuple(t.strftime('%Y-%m-%d') for t in time_range)
    record_xml_attribute('test_time_coverage', tuple(t.strftime('%Y-%m-%d') for t in time_range))

    variables = ast.literal_eval(request.param['variables_subset'])
    results_for_ds_collection['variables_subset'] = variables

    # region = ast.literal_eval(request.param['spatial_subset'])
    region = request.param['spatial_subset'].replace('[', '').replace(']', '')
    # region = None
    results_for_ds_collection['spatial_subset'] = region

    dataset.update_file_list()
    record_xml_attribute('files', len(dataset._file_list))
    results_for_ds_collection['tot_no_of_files_in_collection'] = len(dataset._file_list)
    record_xml_attribute('time_coverage', tuple(t.strftime('%Y-%m-%d') for t in dataset.temporal_coverage()))
    results_for_ds_collection['time_coverage_of_collection'] = tuple(t.strftime('%Y-%m-%d') for t in dataset.temporal_coverage())

    if not any(time_range):
        pytest.skip('File size too large for test')
        results_for_ds_collection['open_local'] = 'File size too large for test'
        results_for_ds_collection['open_remote'] = 'File size too large for test'
    yield dataset, time_range, variables, region


@pytest.fixture(scope='function')
def local_dataset(remote_dataset):
    dataset, time_range, variables, region = remote_dataset
    rand_string = f"test{random.choice(string.ascii_lowercase)}"  # needed when tests run in parallel
    dataset.make_local(rand_string, time_range=time_range, var_names=variables, region=region)
    yield f"local.{rand_string}"
    lds.remove_data_source(f"local.{rand_string}")


# Tests

def test_open_ds(remote_dataset, local_dataset):
    try:
        # or equivalently ds.open_xarray_dataset)
        tic = time.perf_counter()
        dataset, time_range, variables, region = remote_dataset
        remote_ds = dataset.open_dataset(time_range=time_range, var_names=variables, region=region)
        toc = time.perf_counter()
        results_for_ds_collection['open_remote'] = 'success'
        results_for_ds_collection['duration_open_remote_s'] = f'{toc - tic: 0.4f}'
        results_for_ds_collection['no_of_time_stamps_included'] = remote_ds.time.shape[0]
    except:
        results_for_ds_collection['open_remote'] = sys.exc_info()[0]
        results_for_ds_collection['no_of_time_stamps_included'] = None
        results_for_ds_collection['duration_open_remote_s'] = None
    try:
        tic = time.perf_counter()
        ds.open_dataset(local_dataset)
        toc = time.perf_counter()
        results_for_ds_collection['open_local'] = 'success'
        results_for_ds_collection['duration_open_local_s'] = f'{toc - tic: 0.4f}'
    except:
        results_for_ds_collection['open_local'] = sys.exc_info()[0]
        results_for_ds_collection['no_of_time_stamps_included'] = None
        results_for_ds_collection['duration_open_local_s'] = None
    update_csv(results_csv, header_row, results_for_ds_collection)

