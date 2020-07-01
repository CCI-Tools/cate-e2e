There are two kind of tests: 

`pytest test_data_support.py` fetches all dataset collections from the OpenDataPortal via 
opensearch and tests a random time range based on the dataset collection. The test results are written into 
`test_data_support_results-date.csv`.

`test_data_support_scenarios.py` uses specific scenarios which are defined for each dataset collection within the 
`test_scenarios.csv`. Test results are written into `test_data_support_scenarios_results-date.csv`.
For producing the test scenarios the code in `make_testing_scenarios_csv.ipynb` is used.
NOTE: For esacci.OC.8-days.L3S.OC_PRODUCTS.multi-sensor.multi-platform.MERGED.3-1.sinusoidal the 
testing time range was manually adjusted, because tests resulted in memory error, so it was 
adjusted to '1997-09-04', '1997-09-04'.
The following are not included in the test scenarios, because they are esri shape files, tar.gz files and png's:
* esacci.ICESHEETS.mon.IND.GMB.GRACE-instrument.GRACE.VARIOUS.1-3.time_series
* esacci.FIRE.mon.L3S.BA.MSI-(Sentinel-2).Sentinel-2A.MSI.v1-1.pixel
* esacci.FIRE.mon.L3S.BA.MODIS.Terra.MODIS_TERRA.v5-1.pixel
* esacci.ICESHEETS.satellite-orbit-frequency.L4.GLL.multi-sensor.multi-platform.VARIOUS.v1-3.r1
* esacci.ICESHEETS.yr.L4.CFL.multi-sensor.multi-platform.VARIOUS.v3-0.r1

