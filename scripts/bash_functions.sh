#!/bin/bash

function current_ncaa_season()
{
    local myresult=$(Rscript -e 'cat(baseballr::most_recent_ncaa_baseball_season())')
    echo "$myresult"
}
export RESULT=$(current_ncaa_season)
echo $RESULT
while getopts s:e:r: flag
do
    case "${flag}" in
        s) START_YEAR=:"${START_YEAR:=$RESULT}";;
        e) END_YEAR=${OPTARG};;
        r) RESCRAPE=${OPTARG};;
    esac
done

echo $START_YEAR