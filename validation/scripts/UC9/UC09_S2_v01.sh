#!/bin/bash
#Scenario2: UC09 for multiple-years (even full NOAA-17-CLOUD dataset) with monthly timesteps covering the western EU.
#-load data:
cate ds copy esacci.CLOUD.mon.L3C.CLD_PRODUCTS.AVHRR.NOAA-17.AVHRR_NOAA.1-0.r1 -n cld_20070101_20091229 -t 2007-01-01,2009-12-29  #range: 2007-01-01 to 2009-12-29
cate ds copy esacci.AEROSOL.mon.L3.AAI.multi-sensor.multi-platform.ms_uvai.1-5-7.r1 -n aerosol_20070101_20091229  -t 2007-01-01,2009-12-29  #range: 1978-11-01 to 2015-11-29

#-open workspace
cate ws new

#-open ds:
cate res open cloud local.cld_20070101_20091229
cate res open aero local.aerosol2_20070101_20091229

#-plot1:
#cate ws run plot_map ds=@aero var=absorbing_aerosol_index file=./test_aero.png

#-:co-register (does not work atm.)
cate res set aero_coreg coregister ds_master=@cloud ds_slave=@aero

#-select region:
cate res set cloud_sub subset_spatial ds=@cloud region=-13,42,20,60
cate res set aero_sub subset_spatial ds=@aero_coreg region=-13,42,20,60
#-plot2:
#cate ws run plot_map ds=@aero_sub var=absorbing_aerosol_index file=./test_aero_sub.png

#-calculate correlation:
cate res set corr_S2 pearson_correlation ds_x=@aero_sub ds_y=@cloud_sub var_x=absorbing_aerosol_index var_y=cc_total file=corr_S2.txt

cate res print corr_S2

#- finalize ws:
cate ws save
cate ws close
cate ws exit --yes
