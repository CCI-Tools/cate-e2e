import ast
import random
import string
import sys
import traceback

import geopandas
import os
from cate.core.ds import DATA_STORE_REGISTRY
from cate.core import ds
import time
import csv
from csv import DictWriter
from datetime import datetime, timedelta
import xarray as xr
import multiprocessing as mp

pool = mp.Pool(mp.cpu_count())

data_store = DATA_STORE_REGISTRY.get_data_store('esa_cci_odp_os')
lds = DATA_STORE_REGISTRY.get_data_store('local')
header_of_scenarios = ['dataset_collection', 'temporal_subset', 'variables_subset', 'spatial_subset']
with open("test_scenarios.csv") as csvfile:
    reader = csv.DictReader(csvfile, fieldnames=header_of_scenarios, delimiter=';')
    data_sets = []
    firstline = True
    for row in reader:
        if firstline:  # skip first line
            firstline = False
            continue
        data_sets.append(row)
# header for CSV report
header_row = ['dataset_collection', 'open_local', 'duration_open_local_s', 'open_remote', 'duration_open_remote_s',
              'open_via_CLI_from_local', 'open_via_CLI_from_remote',
              'open_via_GUI_from_local', 'open_via_GUI_from_remote',
              'time_coverage_of_collection', 'testing_time_range',
              'spatial_subset', 'variables_subset',
              'no_of_time_stamps_included', 'tot_no_of_files_in_collection']

results_csv = f'test_data_support_scenarios_results_{datetime.date(datetime.now())}.csv'

results_for_ds_collection = {}


# Utility functions
def get_time_range(time_range_str):
    start_date = time_range_str.split(',')[0].replace('(', '').replace("'", "")
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
        write_obj.close()


def update_csv(results_csv, header_row, results_for_dataset_collection):
    if not os.path.isfile(results_csv):
        with open(results_csv, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(header_row)
    append_dict_as_row(results_csv, results_for_dataset_collection, header_row)


def remote_dataset(line):
    dataset = data_store.query(line['dataset_collection'])[0]
    results_for_ds_collection['dataset_collection'] = dataset.id

    time_range_string = line['temporal_subset']
    time_range_1 = get_time_range(time_range_string)
    time_range = tuple(t.strftime('%Y-%m-%d') for t in time_range_1)
    results_for_ds_collection['testing_time_range'] = time_range

    variables = ast.literal_eval(line['variables_subset'])
    results_for_ds_collection['variables_subset'] = variables

    # region = line['spatial_subset'].replace('[', '').replace(']', '')
    region = line['spatial_subset']
    # region = None
    results_for_ds_collection['spatial_subset'] = region

    dataset.update_file_list()
    results_for_ds_collection['tot_no_of_files_in_collection'] = len(dataset._file_list)
    results_for_ds_collection['time_coverage_of_collection'] = tuple(
        t.strftime('%Y-%m-%d') for t in dataset.temporal_coverage())

    return dataset, time_range, variables, region


def local_dataset(dataset, time_range, variables, region):
    if len(lds.query()) > 0:
        for local_dataset in lds.query():
            lds.remove_data_source(local_dataset)
    rand_string = f"test{random.choice(string.ascii_lowercase)}"  # needed when tests run in parallel
    dataset.make_local(rand_string, time_range=time_range, var_names=variables, region=region)
    return f"local.{rand_string}"


def open_via_cli(dataset):
    if isinstance(dataset, xr.Dataset):
        return 'success'
    if isinstance(dataset, geopandas.GeoDataFrame):
        return 'success'
    return 'No, is neither an xr.Dataset or geopandas.GeoDataFrame.'


def open_via_gui(dataset):
    if isinstance(dataset, xr.Dataset):
        if not any([var not in dataset.coords for var in dataset.variables]):
            return 'Dataset empty'
        else:
            for var in dataset.variables:
                if var not in dataset.coords:
                    if dataset[var].dims[-2:] == ('lat', 'lon'):
                        if len(dataset.lat.shape) == 1 and len(dataset.lon.shape) == 1:
                            if dataset.lat.size > 0 and dataset.lon.size > 0:
                                return 'success'
                            else:
                                return f'No, dataset.lat.size: {dataset.lat.size}, dataset.lon.size: {dataset.lon.size}.'
                        else:
                            return f'No, dataset.lat.shape: {dataset.lat.shape}, dataset.lon.shape: {dataset.lon.shape}.'
                    else:
                        return f'No, last two dimensions of variable {var}: {dataset[var].dims[-2:]}.'
    if isinstance(dataset, geopandas.GeoDataFrame):
        return 'success'
    return 'No, is neither an xr.Dataset or geopandas.GeoDataFrame.'


def test_open_ds(data_sets):
    for line in data_sets:
        dataset, time_range, variables, region = remote_dataset(line)
        tic = time.perf_counter()
        try:
            # or equivalently ds.open_xarray_dataset)
            remote_ds = dataset.open_dataset(time_range=time_range, var_names=variables, region=region)
            toc = time.perf_counter()
            results_for_ds_collection['open_remote'] = 'success'
            results_for_ds_collection['duration_open_remote_s'] = f'{toc - tic: 0.4f}'
            results_for_ds_collection['no_of_time_stamps_included'] = remote_ds.time.shape[0]
            results_for_ds_collection['open_via_CLI_from_remote'] = open_via_cli(remote_ds)
            results_for_ds_collection['open_via_GUI_from_remote'] = open_via_gui(remote_ds)
            remote_ds.close()

        except:
            track = traceback.format_exc()
            if 'does not seem to have any datasets in given time range' in track:
                try:
                    time_range = (time_range[0], time_range[1] + timedelta(days=2))
                    results_for_ds_collection['testing_time_range'] = tuple(t.strftime('%Y-%m-%d') for t in time_range)
                    remote_ds = dataset.open_dataset(time_range=time_range, var_names=variables, region=region)
                    toc = time.perf_counter()
                    results_for_ds_collection['open_remote'] = 'success after increasing time range'
                    results_for_ds_collection['duration_open_remote_s'] = f'{toc - tic: 0.4f}'
                    results_for_ds_collection['no_of_time_stamps_included'] = remote_ds.time.shape[0]
                    results_for_ds_collection['open_via_CLI_from_remote'] = open_via_cli(remote_ds)
                    results_for_ds_collection['open_via_GUI_from_remote'] = open_via_gui(remote_ds)
                    remote_ds.close()
                except:
                    toc = time.perf_counter()
                    results_for_ds_collection[
                        'open_remote'] = f'failed after increasing time range {sys.exc_info()[:2]}'
                    results_for_ds_collection['no_of_time_stamps_included'] = None
                    results_for_ds_collection['duration_open_remote_s'] = f'{toc - tic: 0.4f}'
                    results_for_ds_collection['open_via_CLI_from_remote'] = 'No'
                    results_for_ds_collection['open_via_GUI_from_remote'] = 'No'
            else:
                toc = time.perf_counter()
                results_for_ds_collection['open_remote'] = sys.exc_info()[:2]
                results_for_ds_collection['no_of_time_stamps_included'] = None
                results_for_ds_collection['duration_open_remote_s'] = f'{toc - tic: 0.4f}'
                results_for_ds_collection['open_via_CLI_from_remote'] = 'No'
                results_for_ds_collection['open_via_GUI_from_remote'] = 'No'

        tic = time.perf_counter()
        try:
            local_ds_string = local_dataset(dataset, time_range, variables, region)
            local_ds = ds.open_dataset(local_ds_string)
            toc = time.perf_counter()
            results_for_ds_collection['open_local'] = 'success'
            results_for_ds_collection['duration_open_local_s'] = f'{toc - tic: 0.4f}'
            results_for_ds_collection['open_via_CLI_from_local'] = open_via_cli(local_ds)
            results_for_ds_collection['open_via_GUI_from_local'] = open_via_gui(local_ds)
            local_ds.close()
            lds.remove_data_source(local_ds_string)

        except:
            track = traceback.format_exc()
            if 'does not seem to have any datasets in given time range' in track:
                try:
                    time_range = (time_range[0], time_range[1] + timedelta(days=2))
                    local_ds_string = local_dataset(dataset, time_range, variables, region)
                    local_ds = ds.open_dataset(local_ds_string)
                    toc = time.perf_counter()
                    results_for_ds_collection['open_local'] = 'success after increasing time range'
                    results_for_ds_collection['duration_open_local_s'] = f'{toc - tic: 0.4f}'
                    results_for_ds_collection['open_via_CLI_from_local'] = open_via_cli(local_ds)
                    results_for_ds_collection['open_via_GUI_from_local'] = open_via_gui(local_ds)
                    local_ds.close()
                    lds.remove_data_source(local_ds_string)
                except:
                    toc = time.perf_counter()
                    results_for_ds_collection['open_local'] = f'failed after increasing time range {sys.exc_info()[:2]}'
                    results_for_ds_collection['no_of_time_stamps_included'] = None
                    results_for_ds_collection['duration_open_local_s'] = f'{toc - tic: 0.4f}'
                    results_for_ds_collection['open_via_CLI_from_local'] = 'No'
                    results_for_ds_collection['open_via_GUI_from_local'] = 'No'
            else:
                toc = time.perf_counter()
                results_for_ds_collection['open_local'] = sys.exc_info()[:2]
                results_for_ds_collection['no_of_time_stamps_included'] = None
                results_for_ds_collection['duration_open_local_s'] = f'{toc - tic: 0.4f}'
                results_for_ds_collection['open_via_CLI_from_local'] = 'No'
                results_for_ds_collection['open_via_GUI_from_local'] = 'No'

        update_csv(results_csv, header_row, results_for_ds_collection)


# test_open_ds(data_sets)

results = pool.apply(test_open_ds(data_sets))
pool.close()
