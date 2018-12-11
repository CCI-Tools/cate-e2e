#!/bin/bash
#Scenario3: UC09 for globally and daily data of May 2007

#-load data:
cate ds copy esacci.CLOUD.day.L3U.CLD_PRODUCTS.multi-sensor.Envisat.MERISAATSR_ENVISAT.1-0.r1 -n cloud_200705 -t 2007-05-01,2007-05-31
cate ds copy esacci.AEROSOL.day.L3.AAI.multi-sensor.multi-platform.ms_uvai.1-5-7.r1 -n aerosol_200705 -t 2007-05-01,2007-05-31

#-open workspace
cate ws new

#-open ds:
cate res open cloud local.cld_20070101_20091229
cate res open aero local.aerosol2_20070101_20091229

#-plot1:
#cate ws run plot_map ds=@aero var=absorbing_aerosol_index file=./test_aero.png

#-:co-register (does not work atm.)
cate res set aero_coreg coregister ds_master=@cloud ds_replica=@aero

#-select regon:
#cate res set cloud_sub subset_spatial ds=@cloud region=-13,42,20,60
#cate res set aero_sub subset_spatial ds=@aero_coreg region=-13,42,20,60
##-plot2:
##cate ws run plot_map ds=@aero_sub var=absorbing_aerosol_index file=./test_aero_sub.png

#-calculate correlation:
cate res set corr_S3 pearson_correlation ds_x=@aero_coreg ds_y=@cloud var_x=absorbing_aerosol_index var_y=cc_total file=corr_S3.txt

cate res print corr_S3

#- finalize ws:
cate ws save
cate ws close
cate ws exit --yes
