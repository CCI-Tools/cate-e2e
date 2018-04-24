#!/usr/bin/env bash

# Download data
cate ds copy esacci.AEROSOL.mon.L3C.AER_PRODUCTS.AATSR.Envisat.ORAC.03-02.r1 --name uc22_aerosol --time "2010-01-01,2010-12-31" --region " -100,-90,80,90" --vars "AOD550_mean"
cate ds copy esacci.CLOUD.mon.L3C.CLD_PRODUCTS.multi-sensor.multi-platform.ATSR2-AATSR.2-0.r1 --name uc22_cloud --time "2010-01-01,2010-12-31" --region " -100,-90,80,90" --vars "cfc"

rm -rf uc22/
mkdir uc22
cd uc22

# Initialize workspace
cate ws init

# Open datasets
cate res open clouds local.uc22_cloud
cate res open aerosol local.uc22_aerosol

# Subset equatorial region
cate res set clouds_equator subset_spatial ds=@clouds region="-100,-5,80,5"
cate res set aerosol_equator subset_spatial ds=@aerosol region="-100,-5,80,5"

# Calculate mean of the full dataset
cate res set clouds_mean reduce ds=@clouds var="cfc" dim="lat"
cate res set aerosol_mean reduce ds=@aerosol var="AOD550_mean" dim="lat"

# Calculate eqatorial values as a fraction of the mean of the full dataset
cate res set clouds_eq_fraction compute script="equatorial_fraction=clouds_equator.cfc/clouds_mean.cfc"
cate res set aerosol_eq_fraction compute script="equatorial_fraction=aerosol_equator.AOD550_mean/aerosol_mean.AOD550_mean"

# Create the Hovmoeller plots
cate res set plot_1 plot_hovmoeller ds=@clouds_eq_fraction var="equatorial_fraction" x_axis="lon" y_axis="time" contour="False" title="Clouds" file="clouds_hvm.png"
cate res set plot_2 plot_hovmoeller ds=@aerosol_eq_fraction var="equatorial_fraction" x_axis="lon" y_axis="time" contour="False" title="Aerosol" file="aerosol_hvm.png"

# Save and close the workspace
cate ws save
cate ws close

# Exit the interactive session
cate ws exit --yes

cd ..
