#!/bin/bash -e
for i in {1..1000}; do echo Line $i; done
sleep 10
for i in {1001..2000}; do echo Line $i; done
sleep 10
for i in {2001..3000}; do echo Line $i; done

