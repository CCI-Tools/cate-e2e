# Tool for Testing of ECVs with cate

This is a brief guideline what the tool for testing ecvs in cate 
should test, and the how to use the tool. 

The code for the testing tool is in test_cci_data_support.py 

## Testing 

Short time range access, random spatial subset and random subset of variables testing remote and local store:
 * this test loops through all dataset collections available via Opensearch
 * for each dataset collection, the first start time index is chosen, and the end time of the second available file
 * a temporal subset of the dataset collection is made based on the time range 
 * a random subset of variables is made, with a maximum of 3 variables
 * testing of opening the subset:
     * from remote source
     * including it to the local store and opening from local source
    
There are 3 categories which are tested:   
     

   **open**  
   Is given if the test 
   1. does not throw an exception.
   2. the returned value is an instance of xarray.Dataset (gridded) or geopandas.GeoDataFrame (vector).
     
   **cache**  
   Is given if the test 
   1. if **can open** is fulfilled.
   1. does not throw an exception when writing to disc with `make_local`.
   2. does not throw an exception when opening from disc with `open_dataset`.
  
   **map**  
   Is given if the test 
   1. if **can open** is fulfilled.
   2. returned value is an xarray.Dataset with the last two dimensions of a variable
      being 'lat' and 'lon '; both lat and lon are 1D coord vars of size > 0
   3. or returned value is an geopandas.GeoDataFrame.


## Output

* Test report: 
   * CSV following the pattern `sorted_test_cci_data_support_{datetime.date(datetime.now())}.csv`
   * Summary CSV following the pattern `sorted_test_cci_data_support_{datetime.date(datetime.now())}_summary.csv`
   * Json file `DrsID_verification_flags_{datetime.date(datetime.now())}.json`
   
1. Content of report:
  * `ECV-Name` 
  * `Dataset-ID` 
  * `open(1)` with `yes` or `no` as value
  * `cache(2)` with `yes` or `no` as value
  * `map(3)` with `yes` or `no` as value 
  * `comment` referring to the columns `open(1)` `cache(2)` `map(3)` with the help of a prefix
    
2. Content of summary report:  
   A summary of how many dataset ids per ECV were successful for the three different test categories 
   as well as the percentage. Last line is the summary over all ECVs.

3. Json File
   The json contains each tested dataset id with the verification flags.