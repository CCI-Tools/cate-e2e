#!/bin/bash
# Download fire data
cate ds copy esacci.CLOUD.mon.L3C.CLD_PRODUCTS.MODIS.Terra.MODIS_TERRA.2-0.r1 --name CLOUD_2007_UC06 --time '2007-01-01,2007-12-31' --region '72,8,85,17' --vars 'cfc,cot'

# Download sea surface temperature data
cate ds copy esacci.SST.day.L4.SSTdepth.multi-sensor.multi-platform.OSTIA.1-1.r1 --name SST_2006_2007 --time '2006-01-01,2007-12-31' --region ' -175,-10,-115,10' --vars 'analysed_sst,analysis_error'

# Start interactive session by initialising an empty workspace
cate ws init

# Open the datasets and assign to resource names
cate res open cloud local.CLOUD_2007_UC06
# Doesn't work see Issue #257
cate res open sst local.SST_2006_2007

# Perform temporal aggregation
cate res set sst_mon temporal_aggregation ds=@sst

# Perform Long term averaging
cate res set sst_lta long_term_average ds=@sst_mon

# Save the long term average dataset in the current directory
cate res write sst_lta sst_lta.nc

# Perform ENSO index calculation
cate res set enso enso_nino34 ds=@sst_mon var='analysed_sst' file='sst_lta.nc'

# Convert the tabular resource to a dataset
cate res set enso_ds from_dataframe df=@enso

# Select a point of soil moisture in south of India
cate res set cloud_mon_point tseries_point ds=@cloud point='78,12' var='cfc'

# Subset the datasets with a one month lag
cate res set cloud_jannov subset_temporal ds=@cloud_mon_point time_range='2007-01-01,2007-11-01'
cate res set enso_decoct subset_temporal ds=@enso_ds time_range='2006-12-01,2007-10-01'

# TimeSeries Plots
cate res set cld_point_plot plot ds=@cloud_mon_point var="cfc" file="cld_point.jpg"
cate res set cld_jannov_point_plot plot ds=@cloud_jannov var="cfc" file="cld_jannov_point.jpg"
cate res set enso_decoct_plot plot ds=@enso_decoct var="ENSO N3.4 Index" file="enso_decoct_plot.jpg"

# Perform correlation calculation
cate res set corr pearson_correlation_scalar ds_x=@enso_decoct ds_y=@cloud_jannov var_x='ENSO N3.4 Index' var_y='cfc'
cate res print corr

# Delete the workspace
cate ws del --yes
