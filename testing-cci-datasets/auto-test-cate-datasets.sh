#!/bin/bash
set -e
set -m

# export necessary environment variables
export EMAIL_PASSWORD="rxH6BDDy4UqSs2k"

test_directory=~/projects/cate-e2e/testing-cci-datasets
project_dirs=~/projects
declare -a test_modes=("development" "stage" "production")

git pull
# you need to specify here, which envs should be rebuild. For stage and production this only
# happens if a new dev or stable release has been issued. After building a new env for stage and production
# please remember to remove them from update_envs array
#declare -a update_envs=("development")
declare -a update_envs=("dev_local")
declare -a cci_stores=("cci-store" "cci-zarr-store")

for test_mode in "${test_modes[@]}";do
  cate_env=cate-env-${test_mode}
  if [[ " ${update_envs[*]} " =~  ${test_mode}  ]]; then
    cd "$project_dirs"/"$cate_env" || exit
    bash "$test_directory"/update_cate_envs.sh "$test_mode"
  fi
  cd "$test_directory" || exit
  source ~/miniconda3/bin/activate "$cate_env"
  echo "activated environment $cate_env"
  for cci_store in "${cci_stores[@]}";do
    python "$test_directory"/test_cci_data_support.py "$cci_store" "$test_mode"
  done
  source ~/miniconda3/bin/deactivate
done

source ~/miniconda3/bin/activate cate-env-production
for cci_store in "${cci_stores[@]}";do
  python generate_summary_and_send_email.py "$cci_store" "$test_mode"
done
source ~/miniconda3/bin/deactivate

git add .
git commit -m "automatic update of test results"
git push
