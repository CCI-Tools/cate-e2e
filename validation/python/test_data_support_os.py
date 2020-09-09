import random
import string
import traceback

import numpy as np
from cate.core import DATA_STORE_REGISTRY, ds

from xcube.core.dsio import rimraf
import nest_asyncio

from datetime import datetime, timedelta
import sys
import os
import csv

import multiprocessing as mp

nest_asyncio.apply()

pool = mp.Pool(mp.cpu_count())

# header for CSV report
header_row = ['ECV-Name', 'Dataset-ID', 'can_open(1)', 'can_visualise(2)', 'comment']

results_csv = f'test_data_support_os_{datetime.date(datetime.now())}.csv'


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
    if indx > 0 and indy > 0:
        region = f'{"{:.1f}".format(indx)}, {"{:.1f}".format(indy)}, {"{:.1f}".format((indx + 0.1))}, {"{:.1f}".format((indy + 0.1))}'
    elif indx < 0 and indy < 0:
        region = f'{"{:.1f}".format((indx - 0.1))}, {"{:.1f}".format((indy - 0.1))}, {"{:.1f}".format(indx)}, {"{:.1f}".format(indy)}'
    elif indx < 0:
        region = f'{"{:.1f}".format((indx - 0.1))}, {"{:.1f}".format(indy)},{"{:.1f}".format(indx)}, {"{:.1f}".format((indy + 0.1))}'
    elif indy < 0:
        region = f'{"{:.1f}".format(indx)}, {"{:.1f}".format((indy - 0.1))}, {"{:.1f}".format((indx + 0.1))}, {"{:.1f}".format(indy)}'
    return region


def clean_up(random_string):
    if random_string.endswith('.zarr'):
        rimraf(random_string)


def check_for_processing(cube, summary_row):
    var = list(cube.data_vars)[0]
    try:
        np.sum(cube[var])
        summary_row['can_open(1)'] = 'yes'
        comment_1 = ''
    except:
        summary_row['can_open(1)'] = 'no'
        comment_1 = f'Failed executing np.sum(cube[{var}]): {sys.exc_info()[:2]}'
    return summary_row, comment_1


def check_write_to_disc(summary_row, data_source, time_range, variables, region):
    rand_string = f"test{random.choice(string.ascii_lowercase)}"  # needed when tests run in parallel
    try:
        data_source.make_local(rand_string, time_range=time_range, var_names=variables, region=region)
        local_ds = ds.open_dataset(f'local.{rand_string}')
        local_ds.close()
        summary_row['can_open(1)'] = 'yes'
        comment_1 = ''
    except:
        summary_row['can_open(1)'] = 'no'
        comment_1 = f'Failed saving to disc with: {sys.exc_info()[:2]}'
    lds.remove_data_source(f"local.{rand_string}")
    return summary_row, comment_1


def check_for_visualization(cube, summary_row):
    var_with_lat_lon_right_order = []
    comment_2 = None
    for var in cube.data_vars:
        if cube[var].dims[-2:] == ('lat', 'lon'):
            if len(cube.lat.shape) == 1 and len(cube.lon.shape) == 1:
                if cube.lat.size > 0 and cube.lon.size > 0:
                    var_with_lat_lon_right_order.append(var)
                else:
                    comment_2 = f'cube.lat.size: {cube.lat.size}, cube.lon.size: {cube.lon.size}.'
            else:
                comment_2 = f'cube.lat.shape: {cube.lat.shape}, cube.lon.shape: {cube.lon.shape}.'
        else:
            comment_2 = f'Last two dimensions of variable {var}: {cube[var].dims[-2:]}.'
    if len(var_with_lat_lon_right_order) > 0:
        summary_row['can_visualise(2)'] = 'yes'
        comment_2 = ''
    else:
        summary_row['can_visualise(2)'] = 'no'

    return summary_row, comment_2


store = DATA_STORE_REGISTRY.get_data_store('esa_cci_odp_os')
data_sets = store.query()
lds = DATA_STORE_REGISTRY.get_data_store('local')


def test_open_ds(data_ids):
    for data in data_ids:

        comment_1 = None
        comment_2 = None
        data_id = data.id
        summary_row = {'ECV-Name': data_id.split('.')[1], 'Dataset-ID': data_id}
        if data_id == 'esacci.OC.8-days.L3S.OC_PRODUCTS.multi-sensor.multi-platform.MERGED.3-1.sinusoidal':
            comment_1 = f'Dataset makes kernel die.'
            comment_2 = f'Dataset makes kernel die.'
            summary_row['can_open(1)'] = 'no'
            summary_row['can_visualise(2)'] = 'no'
            summary_row['comment'] = f'(1) {comment_1}; (2) {comment_2}'
            update_csv(results_csv, header_row, summary_row)
            continue

        try:
            data_source = store.query(ds_id=data_id)[0]
            data_source.update_file_list()
            region = get_region(data_source)
            try:
                time_range = tuple(
                    t.strftime('%Y-%m-%d') for t in [data_source._file_list[0][1], data_source._file_list[1][2]])
            except:
                try:
                    time_range = tuple(
                        t.strftime('%Y-%m-%d') for t in [data_source._file_list[0][1], data_source._file_list[0][2]])
                except:
                    comment_1 = f'Has no file list.'
                    comment_2 = f'Has no file list.'
                    summary_row['can_open(1)'] = 'no'
                    summary_row['can_visualise(2)'] = 'no'
                    summary_row['comment'] = f'(1) {comment_1}; (2) {comment_2}'
                    update_csv(results_csv, header_row, summary_row)
                    continue

            var_list = []
            s_not_to_be_in_var = ['longitude', 'latitude', 'lat', 'lon', 'bounds', 'bnds', 'date']
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
                print(f'Opening cube for data_id {data_id} with {var_list}.')
                cube = data_source.open_dataset(time_range=time_range, var_names=var_list, region=region)
                summary_row, comment_1 = check_for_processing(cube, summary_row)
                summary_row, comment_2 = check_for_visualization(cube, summary_row)
                cube.close()
                summary_row, comment_1 = check_write_to_disc(summary_row, data_source, time_range, var_list, region)
            except:
                track = traceback.format_exc()
                if 'does not seem to have any datasets in given time range' in track:
                    try:
                        time_range = (time_range[0], time_range[1] + timedelta(days=4))
                        print(f'Opening cube for data_id {data_id} with {var_list}.')
                        cube = data_source.open_dataset(time_range=time_range, var_names=var_list, region=region)
                        summary_row, comment_1 = check_for_processing(cube, summary_row)
                        summary_row, comment_2 = check_for_visualization(cube, summary_row)
                        cube.close()
                        summary_row, comment_1 = check_write_to_disc(summary_row, data_source, time_range, var_list,
                                                                     region)
                    except:
                        summary_row['can_open(1)'] = 'no'
                        summary_row['can_visualise(2)'] = 'no'
                        comment_1 = comment_2 = sys.exc_info()[:2]
                else:
                    summary_row['can_open(1)'] = 'no'
                    summary_row['can_visualise(2)'] = 'no'
                    comment_1 = comment_2 = sys.exc_info()[:2]

        except:
            summary_row['can_open(1)'] = 'no'
            summary_row['can_visualise(2)'] = 'no'
            comment_1 = comment_2 = f'Failed getting data description while executing store.query(ds_id={data_id})[0] with: {sys.exc_info()[:2]}'

        summary_row['comment'] = f'(1) {comment_1}; (2) {comment_2}'
        update_csv(results_csv, header_row, summary_row)


# test_open_ds(data_sets)

results = pool.apply(test_open_ds(data_sets))
pool.close()

with open(results_csv, 'r', newline='') as f_input:
    csv_input = csv.DictReader(f_input)
    data = sorted(csv_input, key=lambda row: (row['Dataset-ID']))

with open(f'sorted_{results_csv}', 'w', newline='') as f_output:
    csv_output = csv.DictWriter(f_output, fieldnames=csv_input.fieldnames)
    csv_output.writeheader()
    csv_output.writerows(data)
