#!/usr/bin/env bash

# 1. load data:

cate ds copy esacci.OZONE.mon.L3.NP.multi-sensor.multi-platform.MERGED.fv0002.r1 --name OZ_UC4_1997_2008_GHG --time "1997-01-01,2008-12-31"  --region " -160,23,-69,50" --vars "O3_du_tot"
cate ds copy esacci.CLOUD.mon.L3C.CLD_PRODUCTS.multi-sensor.multi-platform.ATSR2-AATSR.2-0.r1 --name CC_UC4_1997_2008_GBT --time "1997-01-01,2008-12-31"  --region " -160,23,-69,50" --vars "cfc"

rm -rf uc04/
mkdir uc04
cd uc04

# 2. open workspace
cate ws new

# 3. open resources:
cate res open ghg local.OZ_UC4_1997_2008_GHG 
cate res open temp local.CC_UC4_1997_2008_GBT 

# 4. LTA
cate res set ghg_lta long_term_average ds=@ghg
cate res set temp_lta long_term_average ds=@temp

# 4.b Save LTA to file
cate res write ghg_lta ghg_lta.nc
cate res write temp_lta temp_lta.nc

# 5. Anomaly calculation:
cate res set anom_ghg anomaly_external ds=@ghg file="ghg_lta.nc"
cate res set anom_temp anomaly_external ds=@temp file="temp_lta.nc"

# 6. Aggregate the anomalies to seasons
cate res set anom_ghg_seasonal temporal_aggregation ds=@anom_ghg output_resolution="season"
cate res set anom_temp_seasonal temporal_aggregation ds=@anom_temp output_resolution="season"

# 6. Plot seasonal anomaly:
cate res set plot_1 plot_map ds=@anom_ghg_seasonal time="2008-03-01" region="-160,23,-69,50" file="ghg_spring_2008.jpg"
cate res set plot_2 plot_map ds=@anom_temp_seasonal time="2008-03-01" region="-160,23,-69,50" file="temp_spring_2008.jpg" 

cate ws save
cate ws close
cate ws exit -y

cd ..