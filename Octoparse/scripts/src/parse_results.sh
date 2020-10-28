#!/bin/bash

results_file=../results/raw/DHGateProductionLarge-CB10-22-2020_1603672675.csv
tmp_results_file=${results_file}.tmp
p_results_dir=../results/processed/large
mkdir $p_results_dir
run_token_date=`date +%s`

# Need to remove <U+EEFF>
awk '{ gsub(/\xef\xbb\xbf/,""); print }' $results_file > $tmp_results_file
mv $tmp_results_file $results_file

# extracts the list of keywords to iterate through and create filenames for
# Count how many lines do not contain the seachkey?
cat $results_file | sed -e 's/"[^"]*"//g' | awk -F',' ' { print $8 $9 }' | uniq | grep -v searchkey > keywords.txt


################
# Process results
################
for keyword in `cat keywords.txt`
do 
    ################
    # Setup filename as <timestamp>-DHGate-<keyword> and parse results to individual files based on keyword(s)
    ################
    keyword_trimmed=`echo $keyword | sed 's/&catalog.*//'`
    run_token=$run_token_date-DHGate-$keyword_trimmed
    p_results_file=$run_token.csv

    # Create header for processed results file
    header_line=`head -n 1 $results_file`
    echo $header_line > $p_results_dir/$p_results_file
done

for keyword in `cat keywords.txt`
do
    ################
    # Should be setup as a function name
    ################
    keyword_trimmed=`echo $keyword | sed 's/&catalog.*//'`
    run_token=$run_token_date-DHGate-$keyword_trimmed
    p_results_file=$run_token.csv

    # look for rows containing the keyword in the searchkey or searchkey2 field
    grep ",$keyword,$" $results_file >> $p_results_dir/$p_results_file
    grep ",$keyword$" $results_file >> $p_results_dir/$p_results_file
done
