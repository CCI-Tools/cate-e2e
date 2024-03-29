import csv
import json
import os
import shutil
import sys
from datetime import datetime
from datetime import timedelta

# header for CSV report
HEADER_ROW = ['ECV-Name', 'Dataset-ID', 'Dataset-Title', 'supported',
              'Data-Type', 'open(1)', 'open_temp(2)', 'open_bbox(3)',
              'cache(4)', 'map(5)', 'comment']

DATE_TODAY = datetime.date(datetime.now())


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


def sort_csv(input_csv):
    output_csv = f'{input_csv[:-4]}_sorted.csv'
    with open(input_csv, 'r', newline='') as f_input:
        csv_input = csv.DictReader(f_input)
        data = sorted(csv_input, key=lambda row: (row['Dataset-ID']))

    with open(output_csv, 'w', newline='') as f_output:
        csv_output = csv.DictWriter(f_output, fieldnames=csv_input.fieldnames)
        csv_output.writeheader()
        csv_output.writerows(data)

    return output_csv


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


def get_summary_column(column_name):
    sep = '('
    s_column_name = column_name.split(sep, 1)[0]
    return s_column_name


def update_summary_row(summary_columns, summary_row_new, dataset):
    for column in summary_columns:
        summary_column = get_summary_column(column)
        if 'yes' in dataset[column]:
            summary_row_new[summary_column] += 1
        else:
            if column == 'supported':
                column_failed = f'not_{summary_column}'
            else:
                column_failed = f'{summary_column}_failed'
            summary_row_new[column_failed] += 1
    return summary_row_new


def count_success_fail(data_sets, ecv):
    summary_row_new = {}
    summary_columns = ['supported',
                       'open(1)',
                       'open_temp(2)',
                       'open_bbox(3)',
                       'cache(4)',
                       'map(5)']
    for i in summary_columns:
        i_short = get_summary_column(i)
        summary_row_new[i_short] = 0
        if i_short == 'supported':
            summary_row_new[f'not_{i_short}'] = 0
        else:
            summary_row_new[f'{i_short}_failed'] = 0
        summary_row_new[f'{i_short}_percentage'] = 0

    if 'ALL_ECVS' not in ecv:
        for dataset in data_sets:
            if ecv in dataset['ECV-Name']:
                summary_row_new = update_summary_row(summary_columns,
                                                     summary_row_new,
                                                     dataset)
        tot_nr_datasets = sum(
            [summary_row_new['supported'], summary_row_new['not_supported']]
        )
    else:
        for dataset in data_sets:
            summary_row_new = update_summary_row(summary_columns,
                                                 summary_row_new,
                                                 dataset)
        tot_nr_datasets = len(data_sets)

    for i in summary_columns:
        i_short = get_summary_column(i)
        try:
            summary_row_new[f'{i_short}_percentage'] = \
                100 * summary_row_new[i_short] / (tot_nr_datasets -
                                                  summary_row_new[
                                                      'not_supported'])
        except ZeroDivisionError:
            summary_row_new[f'{i_short}_percentage'] = 0.0

    summary_row_new['ecv'] = ecv

    return summary_row_new


def create_list_of_failed(test_data_sets, failed_csv, header_row):
    for dataset in test_data_sets:
        if dataset['supported'] == 'yes' and (dataset['open(1)'] == 'no' or
                                              dataset['open_temp(2)'] == 'no' or
                                              dataset['open_bbox(3)'] == 'no' or
                                              dataset['cache(4)'] == 'no' or
                                              dataset['map(5)'] == 'no'):
            update_csv(failed_csv, header_row, dataset)

    sorted_csv = sort_csv(failed_csv)
    return sorted_csv


def create_json_of_ids_with_verification_flags(data_sets, results_dir):
    dict_with_verify_flags = {}
    for dataset in data_sets:
        data_type = dataset['Data-Type']
        title = dataset['Dataset-Title']
        verify_flags = []
        if 'yes' in dataset['open(1)']:
            verify_flags.append('open')
        if 'yes' in dataset['open_temp(2)']:
            verify_flags.append('constrain_time')
        if 'yes' in dataset['open_bbox(3)']:
            verify_flags.append('constrain_region')
        if 'yes' in dataset['cache(4)']:
            verify_flags.append('write_zarr')

        dict_with_verify_flags[dataset['Dataset-ID']] = \
            {'data_type': data_type,
             'verification_flags': verify_flags,
             'title': title
             }

    with open(f'{results_dir}/'
              f'{DATE_TODAY}_DrsID_verification_flags.json',
              'w') as f:
        json.dump(dict_with_verify_flags, f, indent=4)


def cleanup_result_outputs_older_than_14_days(path_to_check_for_cleanup):
    date_to_be_kept = DATE_TODAY - (timedelta(days=14))
    for item in os.listdir(path_to_check_for_cleanup):
        try:
            date_of_item = datetime.strptime(item[:10], '%Y-%m-%d')
        except ValueError:
            continue
        if date_of_item and date_of_item.date() < date_to_be_kept:
            try:
                if os.path.isdir(os.path.join(path_to_check_for_cleanup, item)):
                    shutil.rmtree(os.path.join(path_to_check_for_cleanup, item))
                if os.path.isfile(
                        os.path.join(path_to_check_for_cleanup, item)):
                    os.remove(os.path.join(path_to_check_for_cleanup, item))
            except OSError as e:
                print(f'Error removing {item}: {e.strerror}')


def main():
    start_time = datetime.now()
    store_name = 'cci-store'
    test_mode = None
    if len(sys.argv) >= 2:
        store_name = sys.argv[1]
    if len(sys.argv) == 3:
        test_mode = sys.argv[2]

    results_dir = f'{store_name}'
    if test_mode:
        results_dir = f'{test_mode}/{store_name}'

    support_file_name = f'{DATE_TODAY}_test_{store_name}_data_support'
    results_csv = f'{results_dir}/{support_file_name}.csv'

    results_sorted = sort_csv(results_csv)
    test_data_sets = read_all_result_rows(results_sorted, HEADER_ROW)

    ecvs = get_list_of_ecvs(test_data_sets)
    failed_csv = f'{results_dir}/{support_file_name}_failed.csv'
    failed_sorted = create_list_of_failed(test_data_sets,
                                          failed_csv,
                                          HEADER_ROW)

    summary_csv = f'{results_dir}/{support_file_name}_summary_sorted.csv'
    for ecv in ecvs:
        results_summary_row = count_success_fail(test_data_sets, ecv)
        header_summary = list(results_summary_row.keys())
        update_csv(summary_csv, header_summary, results_summary_row)

    create_json_of_ids_with_verification_flags(test_data_sets, results_dir)

    cleanup_result_outputs_older_than_14_days(results_dir)
    cleanup_result_outputs_older_than_14_days(f'{results_dir}/error_traceback')

    print(f'[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] '
          f'Test run finished on {DATE_TODAY}.')
    print(f'[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] '
          f'Test run took {datetime.now() - start_time}')


if __name__ == '__main__':
    main()
