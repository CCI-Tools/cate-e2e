import os, xml
import xml.etree.ElementTree as ET
import pandas as pd
import xml.etree.ElementTree as ET
from cate.core.ds import DATA_STORE_REGISTRY
from cate.core import ds
import random

data_store = DATA_STORE_REGISTRY.get_data_store('esa_cci_odp')
data_sets = data_store.query()


et=ET.parse(os.path.join("tests.xml"))
root=et.getroot()
test_results=[(elem.tag,elem.attrib) for elem in root.iter() if elem.tag in ["testcase","error","failure"]]

agg = []
for data_set,r in zip(data_sets,test_results):
    dkeys = ['cci_project','time_frequency','processing_level','data_type','sensor_id', 'version']
    ddict = {k: data_set.meta_info[k] for k in dkeys}
    ddict.update({'dataset': data_set.id})
    ddict.update({'files': data_set.meta_info['number_of_files']})
    ddict.update({'access_protocols': data_set.protocols})
    ddict.update({'time_coverage': tuple(t.strftime('%Y-%m-%d') for t in data_set.temporal_coverage())})
    ddict.update({'size': hr_size(data_set.meta_info['size'])})
    ddict.update({'size_per_file':hr_size(data_set.meta_info['size']/data_set.meta_info['number_of_files'])})
    
    if r[0] == 'testcase':
        ddict.update({'open_local': True})
    else:
        ddict.update({'open_local': r[1]['message']})
    agg.append(ddict)


df=pd.DataFrame(agg)

html=df.to_html()

with open('test_result.html','wt') as f:
    f.write(html)

df.to_csv('test_result.csv')
