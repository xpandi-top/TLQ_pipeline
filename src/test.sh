#!/bin/zsh


declare -a arr=("1000000SalesRecords.csv" "100000SalesRecords.csv" "10000SalesRecords.csv" "1000SalesRecords.csv" "100SalesRecords.csv" "1500000SalesRecords.csv" "500000SalesRecords.csv" "50000SalesRecords.csv" "5000SalesRecords.csv")
declare -a testarr=("100SalesRecords.csv" "1000SalesRecords.csv")
for i in "${arr[@]}"
do
  echo "processing $i"
  ./callservice.sh $i
done