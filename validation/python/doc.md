#Tool for Testing of ECVs with cate

This is a brief guideline what the tool for testing ecvs in cate 
should test and the how to use the tool. 

The testing tool is called test_data_support.py and 
## Inputs for testing

* yaml file with list of datasets and corresponding different scenarios 

## Testing 

1. Random time range access testing for remote and local store:
    * this test loops through all dataset collections available via Opensearch
    * for each dataset collection, a random start time range index is chosen based 
      on the total number of files in the dataset collection
    * a maximum size is set, then the index is increased untill maximum size is reached 
      or the last dataset of the list of the collection is reached. 
    * new index-1 is used for the end time of the time range
    * a temporal subset of the dataset collection is made based on the time range 
    * testing of opening the subset:
        * from remote source
        * including it to the local store and opening from local source
    
2. Testing predefined scenarios depending on dataset collection with following
   parameters predefined:
    * time range constraint
    * region constraint
    * variables constraint 


## Output

* Test report: CSV and HTML which link to the output of the unit-tests
* Content of report:
    * Dataset collection name
    * Indicator __sucess__ or __fail__ - in case of failure: which test failed and error message
    * Time range of dataset collection
    * Tested Time range 
    * Duration of the runtime of individual tests for each dataset collection
    * A summary row, containing total number of failures, sucess and time elapsed for testing