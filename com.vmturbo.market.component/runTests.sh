#!/bin/bash --noprofile

function disableIgnore {
  target=$1
  echo "disableIgnore() $target"
  cat "$target" | sed 's/@Ignore/\/\/@Ignore/' > ttt.java
  echo "disableIgnore() ls -l ttt.java"
  ls -l ttt.java
  cp ttt.java $target
}

function enableIgnore {
  target=$1
  echo "enableIgnore() uncomment $Ignore"
  cat "$target" | sed 's/\/\/@Ignore/@Ignore/' > ttt.java
  echo "enableIgnore() ls -l ttt.java"
  ls -l ttt.java
  cp ttt.java $target
}

#
# Given the name of the test and where it is located, run the test and generate a CSV file
#
function generate {
  test=$1
  target=$2
  i=$3
  echo "generate() test=$test"
  echo "generate() target=$target"
  echo "generate() i=$i"

  echo "generate() disable $target"
  disableIgnore $target

  echo "generate() mvn -Dtest=$test" 
  # time mvn -Dmaven.compiler.showWarnings=false -Dtest=$test test >& $test.output
  time mvn -o -Dtest=$test test >& $test.output

  # manipulate the output to generate a CSV file
  echo "generate() generate CSV file"
  echo $test > $test.filtered
  echo "time(ms), VMs, RIs, normalizedRIs, templates, families, VM_coupons, natural, desiredState, RI_coupons, discounted_coupons, iterations, engagements, preferences, swaps, costs_natural, currrent, currentWithRIs, SMA, typeOfRIs, OS" > $test.filtered
  grep "\*\*\*" $test.output >> $test.filtered
#  sort -r $test.filtered > $test.sorted
#  uniq $test.sorted > $test.uniq
#  cat $test.uniq | sed 's/\*\*\*//' > $test.$i.csv
  cat $test.filtered | sed 's/\*\*\*//' > $test.$i.csv

  echo "generate() enable $target"
  enableIgnore $target

  # clean up temporary files
  echo "generate() clean up temporary files"
  rm $test.filtered $test.sorted $test.uniq ttt.java #$test.output
}

# where the tests are located
testdir=./src/test/java/com/vmturbo/market/cloudvmscaling/analysis

tests=(
    'Scalability2TimesRIsRegionalLinux.java'
    'Scalability2TimesRIsZonalWindows.java'
    'Scalability2TimesVMsRegionalLinux.java'
    'Scalability2TimesVMsZonalWindows.java'
    'Scalability80PercentRIsRegionalLinux.java'
    'Scalability80PercentRIsZonalWindows.java'
    'Scalability80PercentVMsRegionalLinux.java'
    'Scalability80PercentVMsZonalWindows.java'
    'ScalabilityFixRIsISF.java'
    'ScalabilityFixRIsRegionalWindows.java'
    'ScalabilityFixRIsZonalWindows.java'
    'ScalabilityFixVMsISF.java'
    'ScalabilityFixVMsRegionalWindows.java'
    'ScalabilityFixVMsZonalWindows.java'
     )
echo "tests=${tests[@]}"

for nTest in ${tests[@]}; do
  for i in 1 2 3; do 
    echo "nTest=$nTest i=$i"
    # test's path
    nTarget=$testdir"/"$nTest".java"
    echo "nTarget=$nTarget"

    # call generate
    generate $nTest $nTarget $i
  done 
done

