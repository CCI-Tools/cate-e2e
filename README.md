# cate-e2e

Cate end-to-end tests for integration testing and validation tasks.

* `happy_path` contains so called Happy Path scripts that can be used for integration testing and
  some validation activities. The scripts follow Use Cases as defined in CCI Toolbox [documentation](https://cate.readthedocs.io/en/latest/use_cases.html).
  These scripts are expected to validate that operations required for a certain Use Case are implemented
  and can be chained together in a representative manner.
  The scripts do not always use representative data, hence are not expected to be scientifically valid.
* `validation` contains extended validation scripts exploring additional scenarios around certain Use Cases.

# Running the scripts

It is recommended to clone the whole cate-e2e repository such that the scripts are downloaded together with
applicable test data and can later on be updated easily. Using a `git` command line tool this can be achieved
as such:

```shell
$ git clone https://github.com/CCI-Tools/cate-e2e.git
```

This will create a `cate-e2e` directory in the current directory.

To update the scripts and test data, go to the `cate-e2e` directory and from there invoke:
```shell
$ git pull
```

One can also use one of the many graphical GIT tools to work with the repository: [GIT tools](https://git-scm.com/downloads/guis).

For more information see `README` files in each of the sub-directories of this repository.