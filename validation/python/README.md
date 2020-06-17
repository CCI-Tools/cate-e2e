There are two kind of tests: 

`pytest test_data_support.py` fetches all dataset collections from the OpenDataPortal via 
opensearch and tests a random time range based on the dataset collection. The test results are written into 
`test_data_support-date.csv`.

`test_data_support_scenarios` uses specific scenarios which are defined for each dataset collection within the 
`test_scenarios.csv`. Test results are written into `test_data_support_scenarios.csv`.



