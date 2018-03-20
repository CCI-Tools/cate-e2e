#!/bin/bash
# Download soil moisture data
cate ds copy esacci.SOILMOISTURE.day.L3S.SSMV.multi-sensor.multi-platform.COMBINED.03-2.r1 --name esacci.SOILMOISTURE.day.L3S.SSMV.multi-sensor.multi-platform.COMBINED.03-2.r1_19980101_19981231_reg_0_30_20_60_var_sm --time '1998-01-01,1998-12-31' --region '0,30,20,60' --vars 'sm,sm_uncertainty'

# Download sea surface temperature data
cate ds copy esacci.SST.day.L4.SSTdepth.multi-sensor.multi-platform.OSTIA.1-1.r1 --name esacci.SST.day.L4.SSTdepth.multi-sensor.multi-platform.OSTIA.1-1.r1_19971201_19981231_reg-175_-10_-115_10_var_analysed_sst --time '1997-12-01,1998-12-31' --region ' -175,-10,-115,10' --vars 'analysed_sst,analysis_error'

# Start interactive session by initialising an empty workspace
cate ws init

# Open the datasets and assign to resource names
cate res open soil local.esacci.SOILMOISTURE.day.L3S.SSMV.multi-sensor.multi-platform.COMBINED.03-2.r1_19980101_19981231_reg_0_30_20_60_var_sm
# Doesn't work see Issue #257
cate res open sst local.esacci.SST.day.L4.SSTdepth.multi-sensor.multi-platform.OSTIA.1-1.r1_19971201_19981231_reg-175_-10_-115_10_var_analysed_sst

# Perform temporal aggregation
cate res set soil_mon temporal_aggregation ds=@soil
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
cate res set soil_mon_point tseries_point ds=@soil_mon point='8,51' var='sm'

# Subset the datasets with a one month lag
cate res set soil_jannov subset_temporal ds=@soil_mon_point time_range='1998-01-01,1998-11-01'
cate res set enso_decoct subset_temporal ds=@enso_ds time_range='1997-12-01,1998-10-01'

# TimeSeries Plots
#cate res set soil_jannov_plot plot ds=@soil_jannov var="sm" file="soil_jannov_point.jpg"
#cate res set enso_decoct_plot plot ds=@enso_decoct var="ENSO N3.4 Index" file="enso_decoct_plot.jpg"

# Perform correlation calculation
cate res set corr pearson_correlation_scalar ds_x=@enso_decoct ds_y=@soil_jannov var_x='ENSO N3.4 Index' var_y='sm'
cate res print corr

# Save and close the workspace
cate ws del --yes
