import json
import random
import string
import traceback

import numpy as np
from cate.core import DATA_STORE_REGISTRY, ds
from cate.core.ds import DataAccessError

from xcube.core.dsio import rimraf
import nest_asyncio

from datetime import datetime, timedelta
import sys
import os
import csv

import multiprocessing as mp

nest_asyncio.apply()

# header for CSV report
header_row = ['ECV-Name', 'Dataset-ID', 'open(1)', 'cache(2)', 'map(3)', 'comment']

results_csv = f'test_data_support_os_v2_{datetime.date(datetime.now())}.csv'


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


def get_region(dataset):
    indx = random.uniform(float(dataset.meta_info['bbox_minx']), float(dataset.meta_info['bbox_maxx']))
    indy = random.uniform(float(dataset.meta_info['bbox_miny']), float(dataset.meta_info['bbox_maxy']))
    region = None
    if indx == float(dataset.meta_info['bbox_maxx']):
        indx = indx - 1
    if indy == float(dataset.meta_info['bbox_maxy']):
        indy = indy - 1
    if indx == float(dataset.meta_info['bbox_minx']):
        indx = indx + 1
    if indy == float(dataset.meta_info['bbox_miny']):
        indy = indy + 1
    if indx > 0 and indy > 0:
        region = f'{"{:.1f}".format(indx)}, {"{:.1f}".format(indy)}, {"{:.1f}".format((indx + 0.1))}, {"{:.1f}".format((indy + 0.1))}'
    elif indx < 0 and indy < 0:
        region = f'{"{:.1f}".format((indx - 0.1))}, {"{:.1f}".format((indy - 0.1))}, {"{:.1f}".format(indx)}, {"{:.1f}".format(indy)}'
    elif indx < 0:
        region = f'{"{:.1f}".format((indx - 0.1))}, {"{:.1f}".format(indy)},{"{:.1f}".format(indx)}, {"{:.1f}".format((indy + 0.1))}'
    elif indy < 0:
        region = f'{"{:.1f}".format(indx)}, {"{:.1f}".format((indy - 0.1))}, {"{:.1f}".format((indx + 0.1))}, {"{:.1f}".format(indy)}'
    return region


def check_for_processing(cube, summary_row):
    try:
        var = list(cube.data_vars)[0]
    except IndexError:
        summary_row['open(1)'] = 'no'
        comment_1 = f'Failed at getting first variable from list {list(cube.data_vars)}: {sys.exc_info()[:2]}'
        return summary_row, comment_1
    try:
        np.sum(cube[var])
        summary_row['open(1)'] = 'yes'
        comment_1 = ''
    except:
        summary_row['open(1)'] = 'no'
        comment_1 = f'Failed executing np.sum(cube[{var}]): {sys.exc_info()[:2]}'
    return summary_row, comment_1


def check_write_to_disc(summary_row, comment_2, data_source, time_range, variables, region):
    if comment_2 is not None:
        return summary_row, comment_2

    rand_string = f"test{random.choice(string.ascii_lowercase)}{random.choice(string.octdigits)}"  # needed when tests run in parallel
    try:
        data_source.make_local(rand_string, time_range=time_range, var_names=variables, region=region)
        local_ds = ds.open_dataset(f'local.{rand_string}')
        local_ds.close()
        summary_row['cache(2)'] = 'yes'
        comment_1 = ''
    except DataAccessError:
        summary_row['cache(2)'] = 'no'
        comment_1 = f'local.{rand_string}: Failed saving to disc with: {sys.exc_info()[:2]}'
    except:
        summary_row['cache(2)'] = 'no'
        comment_1 = f'Failed saving to disc with: {sys.exc_info()[:2]}'
    lds.remove_data_source(f"local.{rand_string}")
    return summary_row, comment_1


def check_for_visualization(cube, summary_row, variables):
    var_with_lat_lon_right_order = []
    vars = []
    comment_3 = None
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
        summary_row['map(3)'] = 'yes'
        comment_3 = ''
    else:
        summary_row['map(3)'] = 'no'
        if len(vars) == 0:
            comment_3 = f'Dataset has none of the requested variables: {variables}.'
        if comment_3 is None:
            comment_3 = f'None of  variables: {vars} has lat and lon in correct order.'
    return summary_row, comment_3


store = DATA_STORE_REGISTRY.get_data_store('esa_cci_odp_os')
data_sets = store.query()
lds = DATA_STORE_REGISTRY.get_data_store('local')


def test_open_ds(data):
    comment_1 = None
    comment_2 = None
    comment_3 = None
    data_id = data.id
    summary_row = {'ECV-Name': data_id.split('.')[1], 'Dataset-ID': data_id}
    if data_id == 'esacci.OC.8-days.L3S.OC_PRODUCTS.multi-sensor.multi-platform.MERGED.3-1.sinusoidal':
        comment_1 = comment_2 = f'Dataset makes kernel die.'
        summary_row['open(1)'] = 'no'
        summary_row['map(3)'] = 'no'
        summary_row['comment'] = f'(1) & (2) & (3) {comment_1}'
        update_csv(results_csv, header_row, summary_row)
        return
    try:
        data_source = store.query(ds_id=data_id)[0]
        data_source.update_file_list()
        if len(data_source._file_list) == 0:
            comment_1 = comment_2 = f'Has no file list.'
            summary_row['open(1)'] = 'no'
            summary_row['map(3)'] = 'no'
            summary_row['comment'] = f'(1) & (2) & (3) {comment_1}'
            update_csv(results_csv, header_row, summary_row)
            return
        region = get_region(data_source)
        try:
            time_range = tuple(
                t.strftime('%Y-%m-%d') for t in [data_source._file_list[0][1], data_source._file_list[1][2]])
        except IndexError:
            try:
                time_range = tuple(
                    t.strftime('%Y-%m-%d') for t in [data_source._file_list[0][1], data_source._file_list[0][2]])
            except:
                time_range = None

        var_list = []
        s_not_to_be_in_var = ['longitude', 'latitude', 'lat', 'lon',
                              'bounds', 'bnds', 'date', 'Longitude', 'Latitude']
        if len(data_source.meta_info['variables']) > 3:
            while len(var_list) < 1:
                for var in random.choices(data_source.meta_info['variables'], k=2):
                    if not (any(s_part in var['name'] for s_part in s_not_to_be_in_var)) and var[
                        'name'] not in var_list:
                        var_list.append(var['name'])
        else:
            for var in data_source.meta_info['variables']:
                if not (any(s_part in var['name'] for s_part in s_not_to_be_in_var)) and var[
                    'name'] not in var_list:
                    var_list.append(var['name'])

        try:
            print(
                f'Opening cube for data_id {data_id} with {var_list} and region {region} and time range {time_range}.')
            cube = data_source.open_dataset(time_range=time_range, var_names=var_list, region=region)
            vars_in_cube = []
            for var in var_list:
                if var in cube.data_vars:
                    vars_in_cube.append(var)
            if len(vars_in_cube) == 0:
                comment_1 = comment_2 = f'Requested variables {var_list} for subsetting are not in dataset.'
                summary_row['open(1)'] = 'no'
                summary_row['cache(2)'] = 'no'
                summary_row['map(3)'] = 'no'
                summary_row['comment'] = f'(1) & (2) & (3) {comment_1}'
                update_csv(results_csv, header_row, summary_row)
                return
            print(f'Checking cube for data_id {data_id} for processing.')
            summary_row, comment_1 = check_for_processing(cube, summary_row)
            print(f'Checking cube for data_id {data_id} for visualization.')
            summary_row, comment_3 = check_for_visualization(cube, summary_row, var_list)
            print(f'Closing cube for data_id {data_id}')
            cube.close()
            print(f'Checking cube {data_id} for writing to disk.')
            summary_row, comment_2 = check_write_to_disc(summary_row, comment_2, data_source, time_range, var_list,
                                                         region)
        except ValueError:
            track = traceback.format_exc()
            if 'Can not select a region outside dataset boundaries.' in track:
                try:
                    region = '141.6, -18.7, 141.7, -18.6'
                    print(f'Opening cube for data_id {data_id} with {var_list} and region {region}.')
                    cube = data_source.open_dataset(time_range=time_range, var_names=var_list, region=region)
                    summary_row, comment_1 = check_for_processing(cube, summary_row)
                    summary_row, comment_3 = check_for_visualization(cube, summary_row, var_list)
                    cube.close()
                    summary_row, comment_2 = check_write_to_disc(summary_row, comment_2, data_source, time_range,
                                                                 var_list,
                                                                 region)
                except:
                    summary_row['open(1)'] = 'no'
                    summary_row['cache(2)'] = 'no'
                    summary_row['map(3)'] = 'no'
                    comment_1 = comment_2 = comment_3 = sys.exc_info()[:2]
            else:
                summary_row['open(1)'] = 'no'
                summary_row['cache(2)'] = 'no'
                summary_row['map(3)'] = 'no'
                comment_1 = comment_2 = comment_3 = sys.exc_info()[:2]
        except IndexError:
            print(f'Index error happening at stage 2. for {data_id}')
            summary_row['open(1)'] = 'no'
            summary_row['cache(2)'] = 'no'
            summary_row['map(3)'] = 'no'
            comment_1 = comment_2 = comment_3 = sys.exc_info()[:2]
        except:
            track = traceback.format_exc()
            if 'does not seem to have any datasets in given time range' in track:
                try:
                    time_range = (time_range[0], time_range[1] + timedelta(days=4))
                    print(f'Opening cube for data_id {data_id} with {var_list}.')
                    cube = data_source.open_dataset(time_range=time_range, var_names=var_list, region=region)
                    summary_row, comment_1 = check_for_processing(cube, summary_row)
                    summary_row, comment_3 = check_for_visualization(cube, summary_row, var_list)
                    cube.close()
                    summary_row, comment_2 = check_write_to_disc(summary_row, comment_2, data_source, time_range,
                                                                 var_list,
                                                                 region)
                except IndexError:
                    print(f'Index error happening at stage 3. for {data_id}')
                    summary_row['open(1)'] = 'no'
                    summary_row['cache(2)'] = 'no'
                    summary_row['map(3)'] = 'no'
                    comment_1 = comment_2 = comment_3 = sys.exc_info()[:2]
                except:
                    summary_row['open(1)'] = 'no'
                    summary_row['cache(2)'] = 'no'
                    summary_row['map(3)'] = 'no'
                    comment_1 = comment_2 = comment_3 = sys.exc_info()[:2]
            else:
                summary_row['open(1)'] = 'no'
                summary_row['cache(2)'] = 'no'
                summary_row['map(3)'] = 'no'
                comment_1 = comment_2 = comment_3 = sys.exc_info()[:2]
    except:
        summary_row['open(1)'] = 'no'
        summary_row['cache(2)'] = 'no'
        summary_row['map(3)'] = 'no'
        comment_1 = comment_2 = comment_3 = f'Failed getting data description while executing store.query(ds_id={data_id})[0] with: {sys.exc_info()[:2]}'
    if comment_1 and (comment_1 == comment_3):
        summary_row['comment'] = f'(1) & (2) & (3) {comment_1}'
    else:
        if comment_1:
            if comment_1 == comment_2 == comment_3:
                summary_row['comment'] = f'(1) & (2) & (3) {comment_1}'
            else:
                comment_1 = f'(1) {comment_1}; '
        if comment_2:
            comment_2 = f'(2) {comment_2}; '
        if comment_3:
            comment_3 = f'(3) {comment_3}; '
        summary_row['comment'] = f'{comment_1} {comment_2} {comment_3}'
    update_csv(results_csv, header_row, summary_row)


pool = mp.Pool(mp.cpu_count())
pool.map(test_open_ds, data_sets)
pool.close()

with open(results_csv, 'r', newline='') as f_input:
    csv_input = csv.DictReader(f_input)
    data = sorted(csv_input, key=lambda row: (row['Dataset-ID']))

with open(f'sorted_{results_csv}', 'w', newline='') as f_output:
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
    open_success = 0
    open_fail = 0
    cache_success = 0
    cache_fail = 0
    visualize_success = 0
    visualize_fail = 0

    if 'ALL_ECVS' not in ecv:
        for dataset in data_sets:
            if ecv in dataset['ECV-Name']:
                if 'yes' in dataset['open(1)']:
                    open_success += 1
                else:
                    open_fail += 1

                if 'yes' in dataset['cache(2)']:
                    cache_success += 1
                else:
                    cache_fail += 1

                if 'yes' in dataset['map(3)']:
                    visualize_success += 1
                else:
                    visualize_fail += 1
        total_number_of_datasets = sum([open_success, open_fail])
    else:
        for dataset in data_sets:
            if 'yes' in dataset['open(1)']:
                open_success += 1
            else:
                open_fail += 1

            if 'yes' in dataset['cache(2)']:
                cache_success += 1
            else:
                cache_fail += 1

            if 'yes' in dataset['map(3)']:
                visualize_success += 1
            else:
                visualize_fail += 1
        total_number_of_datasets = len(data_sets)

    open_success_percentage = 100 * open_success / total_number_of_datasets
    visualize_success_percentage = 100 * visualize_success / total_number_of_datasets
    cache_success_percentage = 100 * cache_success / total_number_of_datasets
    summary_row_new = {'ecv': ecv,
                       'open_success': open_success,
                       'open_fail': open_fail,
                       'cache_success': cache_success,
                       'cache_fail': cache_fail,
                       'visualize_success': visualize_success,
                       'visualize_fail': visualize_fail,
                       'open_success_percentage': open_success_percentage,
                       'visualize_success_percentage': visualize_success_percentage,
                       'cache_success_percentage': cache_success_percentage,
                       'total_number_of_datasets': total_number_of_datasets
                       }

    return summary_row_new


test_data_sets = read_all_result_rows(f'sorted_{results_csv}', header_row)
ecvs = get_list_of_ecvs(test_data_sets)

summary_csv = f'sorted_test_data_support_os_v2_{datetime.date(datetime.now())}_summary.csv'
header_summary = ['ecv', 'open_success', 'open_fail','cache_success', 'cache_fail', 'visualize_success', 'visualize_fail',
                  'open_success_percentage', 'cache_success_percentage', 'visualize_success_percentage', 'total_number_of_datasets']

for ecv in ecvs:
    results_summary_row = count_success_fail(test_data_sets, ecv)
    update_csv(summary_csv, header_summary, results_summary_row)


## creating lists with successful and failing dsrid's in order to use those for excluding the failing ones in cate.
def create_lists_of_working_and_failing_ds(data_sets):
    dict_with_verify_flags = {}
    for dataset in data_sets:
        verify_flags = []
        if 'yes' in dataset['open(1)']:
            verify_flags.append('open')
        if 'yes' in dataset['cache(2)']:
            verify_flags.append('cache')
        if 'yes' in dataset['map(3)']:
            verify_flags.append('map')

        dict_with_verify_flags[dataset['Dataset-ID']] = {'verification_flags':verify_flags}

    return dict_with_verify_flags


dict_with_verify_flags = create_lists_of_working_and_failing_ds(test_data_sets)

with open(f'DrsID_verification_flags_{datetime.date(datetime.now())}.json', 'w') as f:
    json.dump(dict_with_verify_flags, f, indent=4)

