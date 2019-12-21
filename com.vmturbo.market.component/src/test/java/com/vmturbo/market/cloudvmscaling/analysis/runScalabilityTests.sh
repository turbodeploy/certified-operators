#!/bin/bash -noprofile
#
# Run scalability tests
#

echo "mvn  -Dtest=Scalability_fixRIs_ISF test"
mvn -Dtest=Scalability_fixRIs_ISF test
echo "mvn  -Dtest=Scalability_fixRIs_regional_windows test"
mvn -Dtest=Scalability_fixRIs_regional_windows test
echo "mvn  -Dtest=Scalability_fixRIs_zonal_windows test"
mvn -Dtest=Scalability_fixRIs_zonal_windows test
echo "mvn  -Dtest=Scalability_fixVMs_ISF test"
mvn -Dtest=Scalability_fixVMs_ISF test
echo "mvn  -Dtest=Scalability_fixVMs_regional_windows test"
mvn -Dtest=Scalability_fixVMs_regional_windows test
echo "mvn  -Dtest=Scalability_fixVMs_zonal_windows test"
mvn -Dtest=Scalability_fixVMs_zonal_windows test
echo "mvn  -Dtest=Scalability_zonal_windows_2timesVMs test"
mvn -Dtest=Scalability_zonal_windows_2timesVMs test
echo "mvn  -Dtest=Scalability_zonal_windows_80percentOfVMs test"
mvn -Dtest=Scalability_zonal_windows_80percentOfVMs test
