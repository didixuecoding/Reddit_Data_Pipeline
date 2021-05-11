#!/usr/bin/bash

# list all files under input folder
ls -lth input 1>>assets/datasets_description.txt

# count number of rows in Authors dataset
echo "Number of rows Authors dataset has" >>assets/datasets_description.txt
zstd -cdq input/RA_78M.csv.zst | wc -l>>assets/datasets_description.txt

# count number of rows in Subreddit dataset
echo "Number of rows Subreddit dataset has" >>assets/datasets_description.txt
zstd -cdq input/Reddit_Subreddits.ndjson.zst | wc -l>>assets/datasets_description.txt

# count number of rows in one of the daily Submission dataset
echo "Number of rows Submission dataset on 2018-01-01 has" >>assets/datasets_description.txt
xz -cdq input/RS_2018-01-01.xz | wc -l>>assets/datasets_description.txt

# generate sample dataset for Authors
mkdir -p assets
zstd -cdq input/RA_78M.csv.zst | tail -n +45 | head -n 10 > assets/sample-author.txt

# generate sample dataset for Subreddits
zstd -cdq input/Reddit_Subreddits.ndjson.zst | head -1 > assets/sample-subrredit.json

# generate sample dataset for Submissions
xz -cdq input/RS_2018-01-01.xz | head -1 > assets/sample-submission.json