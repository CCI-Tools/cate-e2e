# test data support using opensearch
import asyncio
import math
from cate.core.ds import DATA_STORE_REGISTRY, DataAccessError, DataStoreNotice
from cate.ds.esa_cci_odp import _fetch_file_list_json, _extract_metadata_from_odd, _extract_metadata_from_odd_url, \
    _extract_metadata_from_descxml, _extract_metadata_from_descxml_url, _harmonize_info_field_names, \
    _DownloadStatistics, EsaCciOdpDataStore, find_datetime_format, _retrieve_infos_from_dds
import xarray

os_datastore = DATA_STORE_REGISTRY.get_data_store('esa_cci_odp_os')
print(os_datastore)
datasets = os_datastore.query()
lds = DATA_STORE_REGISTRY.get_data_store('local')

no_files_skip = []
cannot_subset_time = []
cannot_subset_space = []
cannot_open = []
access_error = []
def get_testchunk_dates(dataset, max_size=100 * 1024 * 1024, max_files=100):
    "assuming files are chunked in time"
    asyncio.run(dataset._init_file_list())

    try:
        #print(dataset._file_list[0][4]['Opendap'])
        odapfile = dataset._file_list[0][4]['Opendap']
        #print(odapfile)
        xda=xarray.open_dataset(odapfile)
        has_time_dim = any(["time" in xda.dims])
        print(xda.dims,has_time_dim)
        if not has_time_dim:
            cannot_subset_time.append(dataset.id)
            return
        spatial_dims1 = ['lat', 'lon']
        has_space_dims1=any([dim in xda.dims for dim in spatial_dims1])
        spatial_dims2 = ['latitude', 'longitude']
        has_space_dims2= any([dim in xda.dims for dim in spatial_dims2])
        has_space_dims = has_space_dims1 or has_space_dims2

        if not has_space_dims:
            cannot_subset_space.append(dataset.id)
            return
        #print(dataset.temporal_coverage())
        #print(dataset.title)
        #print(dataset.open_dataset(time_range=(str(xda.time.isel(time=0).values),str(xda.time.isel(time=0).values))))
        try:
            dataset.open_dataset(time_range=(dataset.temporal_coverage()[0],dataset.temporal_coverage()[0]))
        except (ValueError, DataAccessError, TypeError) as ex:
            print(ex)
            cannot_open.append(dataset.id)
            return
    except IndexError:
        no_files_skip.append(dataset.id)
        return
    except OSError:
        access_error.append(dataset.id)
        return
    return dataset.id

#for d in datasets:
#    get_testchunk_dates(d)

from concurrent.futures import ThreadPoolExecutor

with ThreadPoolExecutor(max_workers=80) as executor:
    futures = [executor.submit(get_testchunk_dates, dataset) for dataset in datasets]

results = [future.result() for future in futures]

print('Total Datasets: ', len(datasets), end='\n')

print('Cannot subset time: ', len(cannot_subset_time), end='\n')

print('Cannot subset space: ', len(cannot_subset_space), end='\n')
print('Cannot open(misc): ', len(cannot_open), end='\n')
print('Misc access errors:', len(access_error), end='\n')

print('Nofiles skip: ', len(no_files_skip), end='\n')

# some of them have just lat, lon and time is got from is from metadata then it makes hard for make local.
# some have access rights