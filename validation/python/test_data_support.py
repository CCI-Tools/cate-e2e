import os
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
data_sets = data_store.query()
lds = DATA_STORE_REGISTRY.get_data_store('local')

# header for CSV report
header_row = ['dataset_collection', 'open_local', 'duration_open_local_s', 'open_remote',
              'duration_open_remote_s', 'time_coverage_of_collection', 'testing_time_range',
              'no_of_time_stamps_included', 'tot_no_of_files_in_collection', 'cci_project', 'time_frequency',
              'processing_level',
              'data_type', 'sensor_id', 'product_version']

results_csv = f'test_data_support_results_{datetime.date(datetime.now())}.csv'


# Utility functions
def hr_size(size):
    """Returns human readable size
       input size in bytes
    """
    units = ["B", "kB", "MB", "GB", "TB", "PB"]
    i = 0  # index
    while (size > 1024 and i < len(units)):
        i += 1
        size /= 1024.0
    return f"{size:.2f} {units[i]}"


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


def get_testchunk_dates(data_set, max_size=40 * 1024 * 1024, max_files=100):
    "assuming files are chunked in time"
    data_set.update_file_list()
    num_files = len(data_set._file_list)

    # pick a random file index
    indx = random.randrange(0, num_files)
    tstart = data_set._file_list[indx][1]  # start date of a input file is 1
    tot_size = 0
    file_count = 0
    # increment index till tot_size reaches maximum allowed size
    # or file_count maximum number of files
    # or end of filelist
    while tot_size < max_size and file_count <= max_files and indx < num_files:
        curr_size = data_set._file_list[indx][3]

        if curr_size > max_size: break  # skip loop when each file is more then max size

        tot_size += curr_size
        file_count += 1
        indx += 1

    tend = data_set._file_list[indx - 1][2]  # end date of a input file is 2
    # sometimes end date is before start date - swapping. weird.
    if tend < tstart:
        tstart, tend = tend, tstart
    return (tstart, tend)


results_for_ds_collection = {}


# Fixtures
@pytest.fixture(scope="function", params=((dataset, get_testchunk_dates(dataset)) for dataset in data_sets))
def remote_dataset(request, record_xml_attribute, record_property):
    dataset, time_range = request.param
    results_for_ds_collection['dataset_collection'] = dataset.id
    # leaving the xml stuff inside for the moment, might need it later.
    record_xml_attribute('dataset', dataset.id)
    dkeys = ['cci_project', 'time_frequency', 'processing_level', 'data_type', 'sensor_id', 'product_version']

    for k in dkeys:
        record_xml_attribute(k, dataset.meta_info[k])
        results_for_ds_collection[k] = dataset.meta_info[k]

    dataset.update_file_list()
    record_xml_attribute('files', len(dataset._file_list))
    results_for_ds_collection['tot_no_of_files_in_collection'] = len(dataset._file_list)
    # record_xml_attribute('access_protocols', dataset.protocols) # not in new odp metadata
    record_xml_attribute('time_coverage', tuple(t.strftime('%Y-%m-%d') for t in dataset.temporal_coverage()))
    results_for_ds_collection['time_coverage_of_collection'] = tuple(
        t.strftime('%Y-%m-%d') for t in dataset.temporal_coverage())
    # record_xml_attribute('size', hr_size(dataset.meta_info['size'])) # not in new odp general metadata
    # record_xml_attribute('size_per_file', hr_size(dataset.meta_info['size'] / dataset.meta_info['number_of_files']))

    record_xml_attribute('test_time_coverage', tuple(t.strftime('%Y-%m-%d') for t in time_range))
    results_for_ds_collection['testing_time_range'] = tuple(t.strftime('%Y-%m-%d') for t in time_range)
    if not any(time_range):
        pytest.skip('File size too large for test')
        results_for_ds_collection['open_local'] = 'File size too large for test'
        results_for_ds_collection['open_remote'] = 'File size too large for test'
    yield dataset, time_range


@pytest.fixture(scope='function')
def local_dataset(remote_dataset):
    dataset, time_range = remote_dataset
    rand_string = f"test{random.choice(string.ascii_lowercase)}"  # needed when tests run in parallel
    dataset.make_local(rand_string, time_range=time_range)
    yield f"local.{rand_string}"
    lds.remove_data_source(f"local.{rand_string}")


# Tests

def test_open_ds(remote_dataset, local_dataset):
    try:
        # or equivalently ds.open_xarray_dataset)
        tic = time.perf_counter()
        dataset, time_range = remote_dataset
        remote_ds = dataset.open_dataset(time_range=time_range)
        toc = time.perf_counter()
        results_for_ds_collection['open_remote'] = 'sucess'
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
        results_for_ds_collection['open_local'] = 'sucess'
        results_for_ds_collection['duration_open_local_s'] = f'{toc - tic: 0.4f}'
    except:
        results_for_ds_collection['open_local'] = sys.exc_info()[0]
        results_for_ds_collection['no_of_time_stamps_included'] = None
        results_for_ds_collection['duration_open_local_s'] = None

    update_csv(results_csv, header_row, results_for_ds_collection)

