#!/bin/bash

FILES=scripts/*.pig
for f in $FILES
do
  echo "Processing $f file..."
  # take action on each file. $f store current file name
  pig f
  quit
done
echo "Your queries have been run and their results are stored in hadoop fs."