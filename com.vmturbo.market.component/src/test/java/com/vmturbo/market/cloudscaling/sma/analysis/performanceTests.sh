#!/bin/bash --noprofile

#
# run from com.vmturbo.market.component directory
#

#
# Passed in the name of a test, disable @Ingore and run the test
#
subdir="/Users/pfs/VMTurbo/xl-intelliJ/com.vmturbo.market.component/src/test/java/com/vmturbo/market/cloudscaling/sma/analysis"
# all the tests
tests=(Scalability2TimesRIsRegionalLinux    Scalability2TimesRIsZonalWindows     Scalability2TimesVMsRegionalLinux ) \
       Scalability2TimesVMsZonalWindows     Scalability80PercentRIsRegionalLinux Scalability80PercentRIsZonalWindows \
       Scalability80PercentVMsRegionalLinux Scalability80PercentVMsZonalWindows  ScalabilityFixRIsISF \
       ScalabilityFixRIsRegionalWindows     ScalabilityFixRIsZonalWindows        ScalabilityFixVMsISF \
       ScalabilityFixVMsRegionalWindows     ScalabilityFixVMsZonalWindows.java \
      )

runTest () {
  test=$1
  echo "runTest $test"
  cp $subdir/$test.java $subdir/$test.java.orig
  sed 's/@Ignore/\/\/ @Ignore/g' < $subdir/$test.java > $subdir/$test.java.modified
  mv $subdir/$test.java.modified $subdir/$test.java
#  mvn -Dtest=$test test >& $test.1
  cp $subdir/$test.java.orig $subdir/$test.java

  rm $subdir/$test.java.orig
}


echo "tests: ${tests[@]}"
echo
for test in ${tests[@]}; do
   echo "test: $test"
   runTest $test
done
