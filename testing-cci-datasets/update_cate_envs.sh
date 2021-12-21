#!/bin/bash
set -e
set -m

test_mode=$1
echo "Current test mode is: "
echo $test_mode

# versions which should be used for updating environments, need to be adjusted manually
version_development=master
version_stage=v3.1.2
version_production=v3.1.2

# declare -n version=version_${test_mode}
declare -n version=version_${test_mode}
cate_env=cate-env-${test_mode}
git checkout master
git pull
git checkout "$version"
~/miniconda3/bin/mamba env remove -n "$cate_env" -y
echo "env removed"
~/miniconda3/bin/mamba env create -n "$cate_env"
echo "env created"
source ~/miniconda3/bin/activate "$cate_env"
echo "env activated"
python setup.py develop
echo "Update of environment $cate_env done."



