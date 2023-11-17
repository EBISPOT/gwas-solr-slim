#!/usr/bin/env bash

##
## Wrapper to generate slim solr documents on the farm
## Jobs are executed parallel, but this script keeps running till the last job is finished.
## Once jobs are done, the script checks if it the execution was successful or not.
##


# Get scrit directory:
scriptDir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# List of document types:
docTypes=("variant" "publication" "trait" "gene" "study")
# docTypes=("variant" "gene")
# docTypes=("publication" "trait")

##
## Functions
##

function display_help(){
    if [[ ! -z "${1}" ]]; then echo "${1}"; fi

    echo ""
    echo "Wrapper for the generation of the slim solr documents."
    echo ""
    echo ""
    echo "Usage: $0 -h -l <limit> -b <database> -t -d <dataDirectory>"
    echo ""
    echo -e "\t-d - Data directory where the json files will be saved."
    echo -e "\t-l - limit the number of documents for testing."
    echo -e "\t-b - name of the database used."
    echo -e "\t-h - print help message."
    echo -e "\t-t - call a test run: run only for test cases"
    echo ""
    echo ""

    exit 1
}

##
## Parsing command line options:
## 
OPTIND=1
while getopts "htd:l:b:" opt; do
    case "$opt" in
        "d" ) targetDir="${OPTARG}" ;;
        "l" ) limit="${OPTARG}" ;;
        "b" ) database="${OPTARG}" ;;
        "t" ) testRun=1;; 
        "h" ) display_help ;;
        * ) display_help ;;
    esac
done

# Compiling command:
PythonCommand="generate-solr-docs"

# Checking target directory:
if [[ -z "${targetDir}" ]]; then
    display_help "[Error] Directory for output files needs to be specified. Exiting."
elif [[ ! -d "${targetDir}" ]]; then
    display_help "[Error] A valid directory for output files needs to be specified. Exiting."
else
    # Preparing/cleaning output directory:
    targetDir=$(readlink -f $targetDir)
    mkdir -p "${targetDir}/data"
    mkdir -p "${targetDir}/logs"
    rm -f ${targetDir}/data/*.json
    rm -f ${targetDir}/logs/*

    # Adding output folder to python dir:
    PythonCommand="${PythonCommand} --targetDir ${targetDir}/data"
fi

# Database is optional:
if [[ ! -z "${database}" ]]; then 
    PythonCommand="${PythonCommand} --database  ${database}"
fi

# Limit optional:
if [[ ! -z "${limit}" ]]; then
    PythonCommand="${PythonCommand} --limit $limit"
fi

# Is it a test run?
if [[ ! -z ${testRun} && ${testRun} == 1 ]]; then
    PythonCommand="${PythonCommand} --test"
fi

# Sourcing config files:
source ${scriptDir}/config.sh

# Adding script folder to the path:
#export PYTHONPATH=${PYTHONPATH}:${scriptDir}/scripts

# Activate virtual environment:
#source "${envAct}"

##
## Firing up all documents on farm, while capturing jobIDs
##
declare -A jobIDs
for document in ${docTypes[*]}; do 
    output=$(sbatch --mem=1G \
                    --time=08:00:00 \
                    --job-name=generate_${document} \
                    --output=${targetDir}/logs/generate_${document}.o \
                    --error=${targetDir}/logs/generate_${document}.e \
                    --wrap="${PythonCommand} --document ${document}")

    # Extract the job ID
    jobID=$(echo $output | perl -lane '($id) = $_ =~ /Submitted batch job (\d+)/; print $id' )
    jobIDs[$document]=${jobID}
    echo "[Info] ${document} generation is submitted to farm (job ID: ${jobID})."
done

# Declare an associative array for completed jobs
declare -A completedJobIDs

##
## Every 15 minutes we check all the running jobs to see if they are still running:
##
finishedJob=0
while [[ finishedJob -ne ${#docTypes[@]} ]]; do
    for document in ${!jobIDs[@]}; do 
        if squeue -j ${jobIDs[${document}]} | grep -q ${jobIDs[${document}]}; then
            isRunning=1
        else
            isRunning=0
        fi

        if [[ $isRunning -eq 0 ]]; then 
            finishedJob=$(( $finishedJob + 1 ))
            echo "[Info] Generation of ${document} (job ID: ${jobIDs[${document}]}) is completed."

            # Store the completed job ID in completedJobIDs and unset from jobIDs
            completedJobIDs[$document]=${jobIDs[$document]}
            unset jobIDs[$document]
        fi
    done
    sleep 15m
done
echo "[Info] All jobs finished."

# Testing if the jobs finished with success
failed=0
for document in "${docTypes[@]}"; do 
    if [[ -n ${completedJobIDs[${document}]} ]]; then
        jobState=$(sacct -j "${completedJobIDs[${document}]}" --format=State --noheader | head -n 1 | tr -d '[:space:]')
        if [[ $jobState != "COMPLETED" ]]; then 
            echo "[Warning] Generation of $document failed." 
            failed=1
        fi
    else
        echo "[Info] Job for $document was not completed or already checked."
    fi
done

# Final check for any failures
if [[ $failed -ne 0 ]]; then
    echo "[Error] At least one of the documents failed. Exiting."
    exit 1
else
    echo "[Info] Documents successfully generated. Exiting."
    exit 0
fi
