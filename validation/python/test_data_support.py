import random
import string
from cate.core.ds import DATA_STORE_REGISTRY
from cate.core import ds
import pytest

data_store = DATA_STORE_REGISTRY.get_data_store('esa_cci_odp_os')
data_sets = data_store.query()
lds = DATA_STORE_REGISTRY.get_data_store('local')


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


def get_testchunk_dates(data_set, max_size=40 * 1024 * 1024, max_files=100):
    "assuming files are chunked in time"
    data_set.update_file_list()
    num_files = len(data_set._file_list)

    # pick a random file index
    indx = random.randrange(0, num_files)
    tstart = data_set._file_list[indx][1] #start date of a input file is 1
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

    tend = data_set._file_list[indx - 1][2] #start date of a input file is 2
    # because of randomisation of indecies - might happen that end date is before start date - swapping.
    if tend < tstart:
        tstart, tend = tend, tstart
    return (tstart, tend)


# Fixtures
@pytest.fixture(scope="function", params=((dataset, get_testchunk_dates(dataset)) for dataset in data_sets))
def remote_dataset(request, record_xml_attribute, record_property):
    dataset, time_range = request.param
    record_xml_attribute('dataset', dataset.id)
    dkeys = ['cci_project', 'time_frequency', 'processing_level', 'data_type', 'sensor_id', 'product_version']

    for k in dkeys:
        record_xml_attribute(k, dataset.meta_info[k])

    dataset.update_file_list()
    record_xml_attribute('files', len(dataset._file_list))
    # record_xml_attribute('access_protocols', dataset.protocols) # not in new odp metadata
    record_xml_attribute('time_coverage', tuple(t.strftime('%Y-%m-%d') for t in dataset.temporal_coverage()))
    # record_xml_attribute('size', hr_size(dataset.meta_info['size'])) # not in new odp general metadata
    # record_xml_attribute('size_per_file', hr_size(dataset.meta_info['size'] / dataset.meta_info['number_of_files']))

    record_xml_attribute('test_time_coverage', tuple(t.strftime('%Y-%m-%d') for t in time_range))
    if not any(time_range):
        pytest.skip('File size too large for test')
    yield dataset, time_range


@pytest.fixture(scope='function')
def local_dataset(remote_dataset):
    dataset, time_range = remote_dataset
    rand_string = f"test{random.choice(string.ascii_lowercase)}"  # needed when tests run in parallel
    dataset.make_local(rand_string, time_range=time_range)
    yield f"local.{rand_string}"
    lds.remove_data_source(f"local.{rand_string}")


# Tests

# disabling right row because it is tooo slow.
# def test_open_remote(remote_dataset):
#    # or equivalently ds.open_xarray_dataset)
#    dataset, time_range = remote_dataset
#    dataset.open_dataset(time_range=time_range)
#    pass

def test_open_local(local_dataset):
    ds.open_dataset(local_dataset)
    pass
