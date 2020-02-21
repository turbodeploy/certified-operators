#!/bin/bash -noprofile
#
# Run scalability tests
# Set the logging level for com.vmturbo.market.cloudscaling.sma.analysis.StableMarriagePerContext to debug
# to dump the statistics.
#
echo "mvn Scalability2TimesRIsRegionalLinux test"
mvn -Dtest=Scalability2TimesRIsRegionalLinux
echo "mvn Scalability2TimesRIsRegionalLinux test"
mvn -Dtest=Scalability2TimesRIsRegionalLinux test
echo "mvn Scalability2TimesVMsRegionalLinux test"
mvn -Dtest=Scalability2TimesVMsRegionalLinux test
echo "mvn Scalability2TimesVMsZonalWindows test"
mvn -Dtest=Scalability2TimesVMsZonalWindows test
echo "mvn Scalability80PercentRIsRegionalLinux test"
mvn -Dtest=Scalability80PercentRIsRegionalLinux test
echo "mvn Scalability80PercentRIsZonalWindows test"
mvn -Dtest=Scalability80PercentRIsZonalWindows test
echo "mvn Scalability80PercentVMsRegionalLinux test"
mvn -Dtest=Scalability80PercentVMsRegionalLinux test
echo "mvn Scalability80PercentVMsZonalWindows test"
mvn -Dtest=Scalability80PercentVMsZonalWindows test
echo "mvn ScalabilityFixRIsISF test"
mvn -Dtest=ScalabilityFixRIsISF test
echo "mvn ScalabilityFixRIsRegionalWindows test"
mvn -Dtest=ScalabilityFixRIsRegionalWindows test
echo "mvn ScalabilityFixRIsZonalWindows test"
mvn -Dtest=ScalabilityFixRIsZonalWindows test
echo "mvn ScalabilityFixVMsISF test"
mvn -Dtest=ScalabilityFixVMsISF test
echo "mvn ScalabilityFixVMsRegionalWindows test"
mvn -Dtest=ScalabilityFixVMsRegionalWindows test
echo "mvn ScalabilityFixVMsZonalWindows test"
mvn -Dtest=ScalabilityFixVMsZonalWindows test

