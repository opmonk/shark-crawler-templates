#!/bin/bash

p_results_dir=../results/processed/small

files=`ls $p_results_dir`
awk -F',' ' { print $3 "," $5 "," $6 } ' ../sql/keywords.csv > tmp_keywords.txt

for file in $files
do
    run_token=`echo $file | awk -F'.' ' { print $1 } '`
    # keyword is last part of the filename itself. Please note, keyword may contain '-' in it. Need to capture everything AFTER the 3rd field. 
    keyword=`echo $run_token | awk -F'-' ' { print substr($0,index($0,$3)) } '`

    ################
    # Upload results to S3 bucket
    ################
    # Need to join between keywords.csv & $p_results_file
    crawlerId=8

    # find assetId & keywordId
    # For dups, need to find the keyword as the end of the run_token
    line=`grep "=$keyword\""'$' ../sql/keywords.csv`
    if [[ $line == "" ]]
    then
        # For dups, need to find the keyword as the end of the run_token (detect if there is "&catalog" in start url)
	line=`grep "$keyword"'&' ../sql/keywords.csv`

	if [[ $line == "" ]]
	then
    		line=`grep "$keyword"'\/' ../sql/keywords.csv`
	fi
    fi
    assetId=`echo $line | awk -F',' ' { print $3 } '`
    keywordId=`echo $line | awk -F',' ' { print $5 } '`

    # echo "$run_token, $assetId, $keywordId"
    echo "curl -XPOST 'https://tt56usy83j.execute-api.us-east-1.amazonaws.com/dev/crawls' -d'{\"crawlerId\": $crawlerId, \"assetId\": $assetId, \"keywordId\": $keywordId, \"runToken\": \"$run_token\"}'"
    # curl -XPOST 'https://tt56usy83j.execute-api.us-east-1.amazonaws.com/dev/crawls' -d'{"crawlerId": 8, "assetId": $assetId, "keywordId": $keywordId, "runToken": "$run_token"}'
done


