import csv
import json
import multiprocessing as mp
import os
import pandas as pd
import random
import signal
import string
import sys
import traceback
from datetime import datetime
from itertools import repeat

import nest_asyncio
import numpy as np
from cate.core import DATA_STORE_POOL
from cate.ops.io import open_dataset
from cate.core.ds import DataAccessError
from xcube.core.store import TYPE_SPECIFIER_CUBE
from xcube.core.store import TYPE_SPECIFIER_DATASET
from xcube.core.store import DataStoreError

nest_asyncio.apply()

# header for CSV report
header_row = ['ECV-Name', 'Dataset-ID', 'supported', 'open(1)', 'open_temp(2)', 'open_bbox(3)',
              'cache(4)', 'map(5)', 'comment']

# Not supported vector data:
vector_data = [
    'esacci.ICESHEETS.mon.IND.GMB.GRACE-instrument.GRACE.VARIOUS.1-3.greenland_gmb_time_series',
    'esacci.ICESHEETS.unspecified.Unspecified.CFL.multi-sensor.multi-platform.UNSPECIFIED.v3-0.greenland',
    'esacci.ICESHEETS.unspecified.Unspecified.GLL.multi-sensor.multi-platform.UNSPECIFIED.v1-3.greenland',
    'esacci.ICESHEETS.yr.Unspecified.GMB.GRACE-instrument.GRACE.UNSPECIFIED.1-2.greenland_gmb_timeseries',
    'esacci.ICESHEETS.yr.Unspecified.GMB.GRACE-instrument.GRACE.UNSPECIFIED.1-4.greenland_gmb_time_series',
    'esacci.ICESHEETS.yr.Unspecified.GMB.GRACE-instrument.GRACE.UNSPECIFIED.1-5.greenland_gmb_time_series']

# time out in order to cancel datasets which are taking longer than a certain time
TIMEOUT_TIME = 120


class TimeOutException(Exception):
    pass


def alarm_handler(signum, frame):
    print(f'[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] ALARM signal received')
    raise TimeOutException(f'Time out after {TIMEOUT_TIME} seconds.')


# Utility functions

def append_dict_as_row(file_name, dict_of_elem, field_names):
    # Open file in append mode
    with open(file_name, 'a+', newline='') as write_obj:
        # Create a writer object from csv module
        dict_writer = csv.DictWriter(write_obj, fieldnames=field_names)
        # Add dictionary as wor in the csv
        dict_writer.writerow(dict_of_elem)
        write_obj.close()


def update_csv(results_csv, header_row, results_for_dataset_collection):
    if not os.path.isfile(results_csv):
        with open(results_csv, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(header_row)
    append_dict_as_row(results_csv, results_for_dataset_collection, header_row)


def get_time_range(data_descriptor, data):
    time_range = data_descriptor.time_range
    if time_range is not None and data_descriptor.dims is not None \
            and 'time' in data_descriptor.dims and data_descriptor.dims['time'] > 2 \
            and data_descriptor.time_period is not None:
        # we have to use this weird notation so pandas interpretes the data correctly
        time_delta = pd.Timedelta(int(data_descriptor.time_period[:-1]),
                                  data_descriptor.time_period[-1])
        time_start = pd.Timestamp(time_range[0])
        time_end = time_start + time_delta
        return time_start.strftime('%Y-%m-%d'), time_end.strftime('%Y-%m-%d')
    time_name = None
    if 'time' in data:
        time_name = 'time'
    elif 't' in data:
        time_name = 't'
    if time_name is not None:
        start_time = data[time_name][0].values
        end_time_index = min(2, len(data[time_name]) - 1)
        end_time = data[time_name][end_time_index].values
        try:
            return pd.to_datetime(start_time).strftime('%Y-%m-%d'), \
                   pd.to_datetime(end_time).strftime('%Y-%m-%d')
        except TypeError:
            return pd.to_datetime(start_time.isoformat()).strftime('%Y-%m-%d'), \
                   pd.to_datetime(end_time.isoformat()).strftime('%Y-%m-%d')
    return None


def get_region(data_descriptor):
    if data_descriptor.bbox is None:
        return None
    bbox_minx = data_descriptor.bbox[0]
    bbox_miny = data_descriptor.bbox[1]
    bbox_maxx = data_descriptor.bbox[2]
    bbox_maxy = data_descriptor.bbox[3]
    spatial_res = 1.0
    if data_descriptor.spatial_res is not None:
        spatial_res = data_descriptor.spatial_res
    indx = random.uniform(float(bbox_minx), float(bbox_maxx))
    indy = random.uniform(float(bbox_miny), float(bbox_maxy))
    if indx == float(bbox_maxx):
        if indx > 0:
            indx = indx - 1
        else:
            indx = indx + 1
    if indy == float(bbox_maxy):
        if indy > 0:
            indy = indy - 1
        else:
            indy = indy + 1
    if indx == float(bbox_minx):
        indx = indx + 1
    if indy == float(bbox_miny):
        indy = indy + 1
    region = [float("{:.5f}".format(indx)),
              float("{:.5f}".format(indy)),
              float("{:.5f}".format((indx + spatial_res * 2.))),
              float("{:.5f}".format((indy + spatial_res * 2.)))]
    return region


def check_for_processing(dataset, summary_row, time_range):
    signal.signal(signal.SIGALRM, alarm_handler)
    signal.alarm(TIMEOUT_TIME)
    try:
        try:
            var = list(dataset.data_vars)[0]
        except IndexError:
            summary_row['open_bbox(3)'] = 'no'
            comment_1 = f'Failed at getting first variable from list ' \
                        f'{list(dataset.data_vars)}: {sys.exc_info()[:2]}'
            return summary_row, comment_1
        try:
            np.sum(dataset[var].values)
            try:
                first = dataset.time.values[0].strftime("%Y-%m-%d %H:%M:%S")
                last = dataset.time.values[-1].strftime("%Y-%m-%d %H:%M:%S")
            except AttributeError:
                first = pd.to_datetime(dataset.time.values[0]).strftime("%Y-%m-%d %H:%M:%S")
                last = pd.to_datetime(dataset.time.values[-1]).strftime("%Y-%m-%d %H:%M:%S")
            except TypeError:
                first = pd.to_datetime(dataset.time.values[0]).strftime("%Y-%m-%d %H:%M:%S")
                last = pd.to_datetime(dataset.time.values[-1]).strftime("%Y-%m-%d %H:%M:%S")
            print(f'[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] '
                  f'The requested time range is {time_range}. '
                  f'The cubes actual first time stamp is {first} '
                  f'and last {last}.')
            summary_row['open_bbox(3)'] = 'yes'
            comment_1 = ''
        except:
            summary_row['open_bbox(3)'] = 'no'
            comment_1 = f'Failed executing np.sum(dataset[{var}]): {sys.exc_info()[:2]}'
    except TimeOutException:
        summary_row['open_bbox(3)'] = 'no'
        comment_1 = sys.exc_info()[:2]
    signal.alarm(0)

    return summary_row, comment_1


def check_write_to_disc(summary_row, comment_2, data_id, time_range, variables, region, lds,
                        store_name):
    signal.signal(signal.SIGALRM, alarm_handler)
    signal.alarm(TIMEOUT_TIME)
    if comment_2 is not None:
        return summary_row, comment_2

    # needed for when tests run in parallel
    local_ds_id = f'local.{data_id}.zarr'
    print(f'Saving data locally as "{local_ds_id}"')
    try:
        local_ds = open_dataset(ds_id=data_id,
                                data_store_id=store_name,
                                time_range=time_range,
                                var_names=variables,
                                region=region,
                                local_ds_id=local_ds_id,
                                force_local=True)
        local_ds.close()
        summary_row['cache(4)'] = 'yes'
        comment_2 = ''
    except DataAccessError:
        summary_row['cache(4)'] = 'no'
        comment_2 = f'{local_ds_id}: Failed saving to disc with: {sys.exc_info()[:2]}'
    except TimeOutException:
        summary_row['cache(4)'] = 'no'
        comment_2 = sys.exc_info()[:2]
    except:
        summary_row['cache(4)'] = 'no'
        comment_2 = f'Failed saving to disc with: {sys.exc_info()[:2]}'
    if lds.has_data(local_ds_id):
        lds.delete_data(local_ds_id)
    signal.alarm(0)

    return summary_row, comment_2


def _all_tests_no(summary_row,
                  results_csv,
                  general_comment=None,
                  comment_temporal='not_tested',
                  comment_spatial='not_tested'):
    summary_row['open(1)'] = 'no' if general_comment else 'yes'
    summary_row['open_temp(2)'] = 'no' if comment_temporal else 'yes'
    summary_row['open_bbox(3)'] = 'no' if comment_spatial else 'yes'
    summary_row['cache(4)'] = 'no'
    summary_row['map(5)'] = 'no'
    summary_row['comment'] = general_comment if general_comment is not None else ''
    if comment_temporal is not None and comment_temporal != 'not_tested':
        if comment_spatial is not None and comment_spatial != 'not_tested':
            summary_row['comment'] = \
                f'(1) Dataset can only open without spatial or temporal subset;' \
                f'(2) {comment_temporal};' \
                f'(3) {comment_spatial}'
        else:
            summary_row['comment'] \
                = f'(1) Dataset can open without temporal subset only; (2) {comment_temporal}'
    elif comment_spatial is not None and comment_spatial != 'not_tested':
        summary_row['comment'] \
            = f'(1) Dataset can open without spatial subset only; (2) {comment_spatial}'
    update_csv(results_csv, header_row, summary_row)


def check_for_visualization(cube, summary_row, variables):
    var_with_lat_lon_right_order = []
    vars = []
    comment_3 = None
    signal.signal(signal.SIGALRM, alarm_handler)
    signal.alarm(TIMEOUT_TIME)
    try:
        for var in cube.data_vars:
            vars.append(var)
            if cube[var].dims[-2:] == ('lat', 'lon'):
                if len(cube.lat.shape) == 1 and len(cube.lon.shape) == 1:
                    if cube.lat.size > 0 and cube.lon.size > 0:
                        var_with_lat_lon_right_order.append(var)
                    else:
                        comment_3 = f'cube.lat.size: {cube.lat.size}, cube.lon.size: {cube.lon.size}.'
                else:
                    comment_3 = f'cube.lat.shape: {cube.lat.shape}, cube.lon.shape: {cube.lon.shape}.'
            else:
                comment_3 = f'Last two dimensions of variable {var}: {cube[var].dims[-2:]}.'
        if len(var_with_lat_lon_right_order) > 0:
            summary_row['map(5)'] = 'yes'
            comment_3 = ''
        else:
            summary_row['map(5)'] = 'no'
            if len(vars) == 0:
                comment_3 = f'Dataset has none of the requested variables: {variables}.'
            if comment_3 is None:
                comment_3 = f'None of  variables: {vars} has lat and lon in correct order.'
    except TimeOutException:
        summary_row['map(5)'] = 'no'
        comment_3 = sys.exc_info()[:2]
    signal.alarm(0)
    return summary_row, comment_3


def check_for_support(data_id):
    if 'sinusoidal' in data_id:
        supported = False
        reason = f"There is no support for sinusoidal datasets, please use the equivalent dataset with 'geographic' in" \
                 f" the dataset_id."
    elif 'L2P' in data_id:
        supported = False
        reason = f"There is no support for L2P datasets, because problems are expected."
    elif 'esacci.FIRE.mon.L3S' in data_id:
        supported = False
        reason = f"There is no support for FIRE L3S datasets, because problems are expected."
    elif 'esacci.SEALEVEL.satellite-orbit-frequency.L1' in data_id:
        supported = False
        reason = f"There is no support for SEALEVEL satellite-orbit-frequency datasets, because problems are expected."
    elif data_id in vector_data:
        supported = False
        reason = f"There is no support for vector data."

    else:
        supported = True
        reason = None
    return supported, reason


def test_open_ds(data_id, store, lds, results_csv, store_name):
    comment_temporal = None
    comment_spatial = None
    summary_row = {'ECV-Name': data_id.split('.')[1], 'Dataset-ID': data_id}

    supported, reason = check_for_support(data_id)
    if not supported:
        summary_row['supported'] = 'no'
        _all_tests_no(summary_row, results_csv, general_comment=reason)
        return
    else:
        summary_row['supported'] = 'yes'

    type_specifier = None
    type_specifiers_for_data = store.get_type_specifiers_for_data(data_id)
    preferred_type_specifiers = [TYPE_SPECIFIER_CUBE, TYPE_SPECIFIER_DATASET]
    for preferred_type_specifier in preferred_type_specifiers:
        for type_specifier_for_data in type_specifiers_for_data:
            if preferred_type_specifier.is_satisfied_by(type_specifier_for_data):
                type_specifier = type_specifier_for_data
                break
    if type_specifier is None:
        comment_1 = f'Testing only supports datasets. For "{data_id}", ' \
                    f'only available type specifiers "{type_specifiers_for_data}" were found.'
        _all_tests_no(summary_row, results_csv, general_comment=comment_1)
        return
    try:
        data_descriptor = store.describe_data(data_id=data_id, type_specifier=type_specifier)
    except DataStoreError:
        comment_1 = f'Failed getting data description while executing ' \
                    f'store.describe_data(data_id=data_id, type_specifier=type_specifier) ' \
                    f'with: {sys.exc_info()[:2]}'
        _all_tests_no(summary_row, results_csv, general_comment=comment_1)
        return

    var_list = []
    if data_descriptor.data_vars is not None:
        if len(data_descriptor.data_vars) > 3:
            while len(var_list) < 1:
                for var in random.choices(list(data_descriptor.data_vars.keys()), k=2):
                    var_list.append(var)
        else:
            var_list = list(data_descriptor.data_vars.keys())

    try:
        print(f'[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] Opening cube for '
              f'data_id {data_id} with {var_list}.')
        dataset = open_dataset(ds_id=data_id,
                               data_store_id=store_name,
                               var_names=var_list,
                               force_local=False)
        vars_in_dataset = []
        for var in var_list:
            if var in dataset.data_vars:
                vars_in_dataset.append(var)
        if len(vars_in_dataset) == 0:
            comment_1 = f'Requested variables {var_list} for subset are not in dataset.'
            _all_tests_no(summary_row, results_csv, general_comment=comment_1)
            return
        summary_row['open(1)'] = 'yes'
        time_range = get_time_range(data_descriptor, dataset)
        if time_range is None:
            comment_temporal = 'Dataset has no time coordinate.'
    except:
        traceback_file_url = generate_traceback_file(store_name,
                                                     data_id,
                                                     None,
                                                     var_list,
                                                     None)
        _all_tests_no(summary_row, results_csv, general_comment=traceback_file_url)
        return

    if time_range is not None:
        try:
            print(f'[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] Opening cube for '
                  f'data_id {data_id} with {var_list} and time range {time_range}.')
            dataset = open_dataset(ds_id=data_id,
                                   data_store_id=store_name,
                                   time_range=time_range,
                                   var_names=var_list,
                                   force_local=False)
            vars_in_dataset = []
            for var in var_list:
                if var in dataset.data_vars:
                    vars_in_dataset.append(var)
            if len(vars_in_dataset) == 0:
                comment_temporal = f'Requested variables {var_list} for subset are not in dataset.'
            else:
                summary_row['open_temp(2)'] = 'yes'
        except:
            comment_temporal = generate_traceback_file(store_name, data_id, time_range, var_list,
                                                       None, '_temp')

    region = get_region(data_descriptor)

    if region is None:
        comment_spatial = 'Could not determine region subset.'
    else:
        try:
            print(f'[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] Opening cube for data_id '
                  f'{data_id} with {var_list} and region {region}.')
            dataset = open_dataset(ds_id=data_id,
                                   data_store_id=store_name,
                                   var_names=var_list,
                                   region=region,
                                   force_local=False)
            vars_in_dataset = []
            for var in var_list:
                if var in dataset.data_vars:
                    vars_in_dataset.append(var)
            if len(vars_in_dataset) == 0:
                comment_spatial = f'Requested variables {var_list} for subset are not in dataset.'
            else:
                summary_row['open_bbox(3)'] = 'yes'
        except ValueError:
            comment_spatial = generate_traceback_file(store_name, data_id, None, var_list, region,
                                                  '_spatial')
        except IndexError:
            print(f'[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] '
                  f'Index error happening at stage 2. for {data_id}')
            comment_spatial = generate_traceback_file(store_name, data_id, None, var_list, region,
                                                      '_spatial')
        except:
            comment_spatial = generate_traceback_file(store_name, data_id, None, var_list, region,
                                                      '_spatial')

    if comment_temporal is not None or comment_spatial is not None:
        _all_tests_no(summary_row,
                      results_csv,
                      comment_temporal=comment_temporal,
                      comment_spatial=comment_spatial)
        return

    dataset = open_dataset(ds_id=data_id,
                           data_store_id=store_name,
                           var_names=var_list,
                           time_range=time_range,
                           region=region,
                           force_local=False)

    print(f'[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] '
          f'Checking dataset for data_id {data_id} for processing.')
    summary_row, comment_1 = check_for_processing(dataset, summary_row, time_range)
    print(f'[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] '
          f'Checking dataset for data_id {data_id} for visualization.')
    summary_row, comment_3 = check_for_visualization(dataset, summary_row, var_list)
    print(f'[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] '
          f'Closing dataset for data_id {data_id}')
    dataset.close()
    print(f'[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] '
          f'Checking dataset {data_id} for writing to disk.')
    summary_row, comment_2 = check_write_to_disc(summary_row, None, data_id, time_range,
                                                 var_list, region, lds, store_name)

    if comment_1 and (comment_1 == comment_3):
        summary_row['comment'] = f'{comment_1}'
    else:
        if comment_1:
            if comment_1 == comment_2 == comment_3:
                summary_row['comment'] = f'{comment_1}'
            else:
                comment_1 = f'(1) {comment_1}; '
        if comment_2:
            comment_2 = f'(2) {comment_2}; '
        if comment_3:
            comment_3 = f'(3) {comment_3}; '
        summary_row['comment'] = f'{comment_1} {comment_2} {comment_3}'
    update_csv(results_csv, header_row, summary_row)


def generate_traceback_file(store_name, data_id, time_range, var_list, region, suffix=''):
    if not os.path.exists(f'{store_name}/error_traceback'):
        os.mkdir(f'{store_name}/error_traceback/')
    dir_for_traceback = f'{store_name}/error_traceback/{datetime.date(datetime.now())}'
    if not os.path.exists(dir_for_traceback):
        os.mkdir(dir_for_traceback)
    traceback_file = f'{dir_for_traceback}/{data_id}{suffix}.txt'
    with open(traceback_file, 'a') as trace_f:
        trace_f.write(f'[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] Request: \n'
                      f'open_dataset(ds_id={data_id}, store_id={store_name}, '
                      f'time_range={time_range}, var_names={var_list}, region={region})\n')
        trace_f.write('\n')
        trace_f.write(traceback.format_exc())
    traceback_file_url = \
        f'https://github.com/CCI-Tools/cate-e2e/blob/master/testing-cci-datasets/{traceback_file}'
    return traceback_file_url


def sort_csv(input_csv, output_csv):
    with open(input_csv, 'r', newline='') as f_input:
        csv_input = csv.DictReader(f_input)
        data = sorted(csv_input, key=lambda row: (row['Dataset-ID']))

    with open(output_csv, 'w', newline='') as f_output:
        csv_output = csv.DictWriter(f_output, fieldnames=csv_input.fieldnames)
        csv_output.writeheader()
        csv_output.writerows(data)


# creating summary csv
def read_all_result_rows(path, header_row):
    test_data_sets_list = []
    with open(path) as csvfile:
        reader = csv.DictReader(csvfile, fieldnames=header_row, delimiter=',')
        firstline = True
        for row in reader:
            if firstline:  # skip first line
                firstline = False
                continue
            test_data_sets_list.append(row)
    return test_data_sets_list


def get_list_of_ecvs(data_sets):
    ecvs = []
    for dataset in data_sets:
        if dataset['ECV-Name'] in ecvs:
            continue
        else:
            ecvs.append(dataset['ECV-Name'])
    ecvs.append('ALL_ECVS')
    return ecvs


def count_success_fail(data_sets, ecv):
    supported = 0
    not_supported = 0
    open_success = 0
    open_fail = 0
    open_temp_success = 0
    open_temp_fail = 0
    open_bbox_success = 0
    open_bbox_fail = 0
    cache_success = 0
    cache_fail = 0
    visualize_success = 0
    visualize_fail = 0

    if 'ALL_ECVS' not in ecv:
        for dataset in data_sets:
            if ecv in dataset['ECV-Name']:
                if 'yes' in dataset['supported']:
                    supported += 1
                    if 'yes' in dataset['open(1)']:
                        open_success += 1
                    else:
                        open_fail += 1

                    if 'yes' in dataset['open_temp(2)']:
                        open_temp_success += 1
                    else:
                        open_temp_fail += 1

                    if 'yes' in dataset['open_bbox(3)']:
                        open_bbox_success += 1
                    else:
                        open_bbox_fail += 1

                    if 'yes' in dataset['cache(4)']:
                        cache_success += 1
                    else:
                        cache_fail += 1

                    if 'yes' in dataset['map(5)']:
                        visualize_success += 1
                    else:
                        visualize_fail += 1
                else:
                    not_supported += 1
        total_number_of_datasets = sum([supported, not_supported])
    else:
        for dataset in data_sets:
            if 'yes' in dataset['supported']:
                supported += 1
                if 'yes' in dataset['open(1)']:
                    open_success += 1
                else:
                    open_fail += 1

                if 'yes' in dataset['open_temp(2)']:
                    open_temp_success += 1
                else:
                    open_temp_fail += 1

                if 'yes' in dataset['open_bbox(3)']:
                    open_bbox_success += 1
                else:
                    open_bbox_fail += 1

                if 'yes' in dataset['cache(4)']:
                    cache_success += 1
                else:
                    cache_fail += 1

                if 'yes' in dataset['map(5)']:
                    visualize_success += 1
                else:
                    visualize_fail += 1
            else:
                not_supported += 1
        total_number_of_datasets = len(data_sets)

    supported_percentage = 100 * supported / total_number_of_datasets
    open_success_percentage = 100 * open_success / (total_number_of_datasets - not_supported)
    open_temp_success_percentage = \
        100 * open_temp_success / (total_number_of_datasets - not_supported)
    open_bbox_success_percentage = \
        100 * open_bbox_success / (total_number_of_datasets - not_supported)
    visualize_success_percentage = \
        100 * visualize_success / (total_number_of_datasets - not_supported)
    cache_success_percentage = 100 * cache_success / (total_number_of_datasets - not_supported)
    summary_row_new = {'ecv': ecv,
                       'supported': supported,
                       'open_success': open_success,
                       'open_fail': open_fail,
                       'open_temp_success': open_temp_success,
                       'open_temp_fail': open_temp_fail,
                       'open_bbox_success': open_bbox_success,
                       'open_bbox_fail': open_bbox_fail,
                       'cache_success': cache_success,
                       'cache_fail': cache_fail,
                       'visualize_success': visualize_success,
                       'visualize_fail': visualize_fail,
                       'supported_percentage': supported_percentage,
                       'open_success_percentage': open_success_percentage,
                       'open_temp_success_percentage': open_temp_success_percentage,
                       'open_bbox_success_percentage': open_bbox_success_percentage,
                       'visualize_success_percentage': visualize_success_percentage,
                       'cache_success_percentage': cache_success_percentage,
                       'total_number_of_datasets': total_number_of_datasets
                       }

    return summary_row_new


def create_list_of_failed(test_data_sets, failed_csv, header_row):
    for dataset in test_data_sets:
        if dataset['supported'] == 'yes' and (dataset['open(1)'] == 'no' or
                                              dataset['open_temp(2)'] == 'no' or
                                              dataset['open_bbox(3)'] == 'no' or
                                              dataset['cache(4)'] == 'no' or
                                              dataset['map(5)'] == 'no'):
            update_csv(failed_csv, header_row, dataset)


def create_dict_of_ids_with_verification_flags(data_sets):
    dict_with_verify_flags = {}
    for dataset in data_sets:
        verify_flags = []
        if 'yes' in dataset['open(1)']:
            verify_flags.append('open')
        if 'yes' in dataset['open_temp(2)']:
            verify_flags.append('open_time')
        if 'yes' in dataset['open_bbox(3)']:
            verify_flags.append('open_bbox')
        if 'yes' in dataset['cache(4)']:
            verify_flags.append('cache')
        if 'yes' in dataset['map(5)']:
            verify_flags.append('map')

        dict_with_verify_flags[dataset['Dataset-ID']] = {'verification_flags': verify_flags}

    return dict_with_verify_flags


def main():
    store_name = 'cci-store'
    if len(sys.argv) == 2:
        store_name = sys.argv[1]
    if not os.path.exists(store_name):
        os.mkdir(store_name)
    support_file_name = f'test_{store_name}_data_support_{datetime.date(datetime.now())}.csv'
    results_csv = f'{store_name}/{support_file_name}'
    store = DATA_STORE_POOL.get_store(store_name)
    data_ids = store.get_data_ids()
    lds = DATA_STORE_POOL.get_store('local')

    start_time = datetime.now()
    if store_name == 'cci-store':
        with mp.Pool(mp.cpu_count() - 1, maxtasksperchild=1) as pool:
            pool.starmap(test_open_ds, zip(data_ids,
                                           repeat(store),
                                           repeat(lds),
                                           repeat(results_csv),
                                           repeat(store_name)))
            pool.close()
            pool.join()
    else:
        for data_id in data_ids:
            test_open_ds(data_id, store, lds, results_csv, store_name)

    sort_csv(results_csv, f'{store_name}/sorted_{support_file_name}')

    test_data_sets = read_all_result_rows(f'{store_name}/sorted_{support_file_name}', header_row)

    ecvs = get_list_of_ecvs(test_data_sets)
    failed_csv = f'{store_name}/failed_{support_file_name}'
    create_list_of_failed(test_data_sets, failed_csv, header_row)
    if os.path.exists(failed_csv):
        sort_csv(failed_csv, f'{store_name}/sorted_failed_{support_file_name}')

    with open(results_csv, 'r', newline='') as f_input:
        csv_input = csv.DictReader(f_input)
        data = sorted(csv_input, key=lambda row: (row['Dataset-ID']))
    with open(f'{store_name}/sorted_{support_file_name}', 'w', newline='') as f_output:
        csv_output = csv.DictWriter(f_output, fieldnames=csv_input.fieldnames)
        csv_output.writeheader()
        csv_output.writerows(data)

    summary_csv = f'{store_name}/summary_sorted_{support_file_name}'
    header_summary = ['ecv', 'supported', 'open_success', 'open_fail', 'open_temp_success',
                      'open_temp_fail', 'open_bbox_success', 'open_bbox_fail', 'cache_success',
                      'cache_fail', 'visualize_success', 'visualize_fail', 'supported_percentage',
                      'open_success_percentage', 'open_temp_success',
                      'open_bbox_success_percentage', 'cache_success_percentage',
                      'visualize_success_percentage', 'total_number_of_datasets']

    for ecv in ecvs:
        results_summary_row = count_success_fail(test_data_sets, ecv)
        update_csv(summary_csv, header_summary, results_summary_row)

    dict_with_verify_flags = create_dict_of_ids_with_verification_flags(test_data_sets)

    with open(f'{store_name}/'
              f'DrsID_verification_flags_{datetime.date(datetime.now())}.json', 'w') as f:
        json.dump(dict_with_verify_flags, f, indent=4)

    if os.path.exists(results_csv):
        os.remove(results_csv)
    else:
        print(f'[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] '
              f'The file {results_csv} does not exist.')

    if os.path.exists(failed_csv):
        os.remove(failed_csv)
    else:
        print(f'[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] '
              f'The file {failed_csv} does not exist.')

    print(f'[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] '
          f'Test run finished on {datetime.date(datetime.now())}.')
    print(f'[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] '
          f'Test run took {datetime.now() - start_time}')


if __name__ == '__main__':
     main()
