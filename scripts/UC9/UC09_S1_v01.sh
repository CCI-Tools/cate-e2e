#!/bin/bash

#-load data:
cate ds copy esacci.CLOUD.mon.L3C.CLD_PRODUCTS.AVHRR.NOAA-17.AVHRR_NOAA.1-0.r1 -n cld_2007 -t 2007-01-01,2007-12-31
cate ds copy esacci.AEROSOL.mon.L3.AAI.multi-sensor.multi-platform.ms_uvai.1-5-7.r1 -n aerosol2_2007 -t 2007-01-01,2007-12-31

#-open workspace
cate ws new

#-open ds:
cate res open cloud local.cld_2007
cate res open aero local.aerosol2_2007

#-:co-register (does not work atm.)
cate res set aero_coreg coregister ds_master=@cloud ds_slave=@aero

#-select regon:
cate res set cloud_sub subset_spatial ds=@cloud region=-13,42,20,60
cate res set aero_sub subset_spatial ds=@aero_coreg region=-13,42,20,60

#-calculate correlation:
cate res set corr_out pearson_correlation ds_x=@aero_sub ds_y=@cloud_sub var_x=absorbing_aerosol_index var_y=cc_total file=corr.txt

cate res print corr

#- finalize ws:
cate ws save
cate ws close
cate ws exit --yes
