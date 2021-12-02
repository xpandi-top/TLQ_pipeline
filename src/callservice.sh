#!/bin/bash

# JSON object to pass to Lambda Function
json={"\"bucketname\"":"\"test.bucket.sales.dimo\"","\"filename\"":"\"$1\""}

echo "Invoking Lambda function using AWS CLI"
#time output=`aws lambda invoke --invocation-type RequestResponse --function-name {LAMBDA-FUNCTION-NAME} --region us-east-2 --payload $json /dev/stdout | head -n 1 | head -c -2 ; echo`
{ time output=`aws lambda invoke --invocation-type RequestResponse --function-name service1 --region us-east-2 --payload $json /dev/stdout` ; } 2> "$1.output"

#echo ""
#echo "JSON RESULT:"
#echo $output | jq
echo $output | jq >> "$1.output"
#echo ""
