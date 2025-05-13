#!/bin/bash

# Set default values
START_POLYGON=0
END_POLYGON=689
WAIT_SECONDS=20

# flag to track if we're in the process of terminating
TERMINATING=0

# cleanup function for proper termination
cleanup() {
    if [ $TERMINATING -eq 1 ]; then
        return  # Avoid recursive calls if cleanup is already in progress
    fi
    
    TERMINATING=1
    echo ""
    echo "$(date +'%Y-%m-%d %H:%M:%S') - ⚠️ Caught termination signal. Cleaning up..."
    echo "Polygon job submission was interrupted. Check ${LOG_DIR}/submission_log.csv to see which polygons were processed."
    echo "Exiting..."
    exit 1
}

# set up signal handlers for proper termination
trap cleanup SIGINT SIGTERM SIGHUP


show_help() {
    echo "Usage: $0 [options]"
    echo "Submits jobs for processing polygons within QoS limits"
    echo ""
    echo "Options:"
    echo "  -s, --start NUM      Start polygon ID (default: 0)"
    echo "  -e, --end NUM        End polygon ID (default: 689)"
    echo "  -w, --wait NUM       Seconds to wait between checks (default: 60)"
    echo "  -h, --help           Show this help message"
    echo ""
    echo "Press Ctrl+C at any time to stop the script."
}

# parse command-line options
while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
        -s|--start)
            START_POLYGON="$2"
            shift 2
            ;;
        -e|--end)
            END_POLYGON="$2"
            shift 2
            ;;
        -w|--wait)
            WAIT_SECONDS="$2"
            shift 2
            ;;
        -h|--help)
            show_help
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            show_help
            exit 1
            ;;
    esac
done

# validate input
if ! [[ "$START_POLYGON" =~ ^[0-9]+$ ]] || ! [[ "$END_POLYGON" =~ ^[0-9]+$ ]]; then
    echo "Error: Start and end polygon IDs must be numbers"
    exit 1
fi

if [ "$START_POLYGON" -gt "$END_POLYGON" ]; then
    echo "Error: Start polygon ID must be less than or equal to end polygon ID"
    exit 1
fi

# create a log directory
LOG_DIR="$SCRATCH/polygon-jobs-logs"
mkdir -p $LOG_DIR

echo "===== QoS-Aware Polygon Job Submission ====="
echo "Processing polygons $START_POLYGON to $END_POLYGON"
echo "Waiting $WAIT_SECONDS seconds between submission attempts"
echo "Log directory: $LOG_DIR"
echo "Press Ctrl+C at any time to stop the script"
echo "=========================================="

# get the current QoS job limit
get_max_jobs_allowed() {
    # Try to extract the max jobs from sacctmgr
    # If this doesn't work, we might need to hardcode the value
    local qos_info=$(sacctmgr show qos debug format=MaxSubmitJobsPerUser -n 2>/dev/null)
    local max_jobs=$(echo "$qos_info" | tr -d ' ')
    
    # If we couldn't get the limit, default to 5 (see https://docs.nersc.gov/jobs/policy/#qos-limits-and-charges for more info)
    if [[ -z "$max_jobs" || "$max_jobs" == "-1" ]]; then
        max_jobs=5
    fi
    
    echo $max_jobs
}

# count user's current jobs (both pending and running)
count_user_jobs() {
    squeue -u $USER -h | wc -l
}

# check if a specific polygon job is already queued or running
is_polygon_job_active() {
    local polygon_id=$1
    local job_name="cdr-atlas-polygon-${polygon_id}"
    
    # check if there's any job with this name
    local count=$(squeue -u $USER -n $job_name -h | wc -l)
    
    if [ "$count" -gt 0 ]; then
        return 0  # True - job is active
    else
        return 1  # False - job is not active
    fi
}

# function to submit a job for a specific polygon
submit_polygon_job() {
    polygon_id=$1
    job_name="cdr-atlas-polygon-${polygon_id}"
    
    # skip if this polygon job is already in the queue
    if is_polygon_job_active $polygon_id; then
        echo "$(date +'%Y-%m-%d %H:%M:%S') - Skipping polygon ${polygon_id} - job already in queue"
        return 0
    fi
    
    # create a temporary batch script for this polygon
    temp_script="${LOG_DIR}/polygon_${polygon_id}_job.sh"
    
    # copy the template and replace the polygon parameter
    cp single-case-batch-job.sh $temp_script
    
    # Replace the job name and output file to include the polygon ID
    sed -i "s/--job-name=cdr-atlas-proc/--job-name=${job_name}/" $temp_script
    sed -i "s|--output=cdr-atlas-proc-%j.log|--output=${LOG_DIR}/${job_name}-%j.log|" $temp_script
    
    # Replace the python command to include the correct polygon ID
    sed -i "s/python research_grade_data\.py process-all-cases --polygon 1/python research_grade_data\.py process-all-cases --polygon ${polygon_id}/" $temp_script
    sed -i "s/python dor_cli\.py vis populate-store2 -p 1/python dor_cli\.py vis populate-store2 -p ${polygon_id}/" $temp_script
    sed -i "s/python dor_cli\.py vis populate-store3 -p 1/python dor_cli\.py vis populate-store3 -p ${polygon_id}/" $temp_script
    sed -i "s/python process_fg_co2_excess\.py -p 1/python process_fg_co2_excess\.py -p ${polygon_id}/" $temp_script
    
    # submit the job and capture both job ID and error messages
    submit_output=$(sbatch $temp_script 2>&1)
    submit_status=$?
    
    if [ $submit_status -eq 0 ]; then
   
        job_id=$(echo "$submit_output" | awk '{print $4}')
        echo "$(date +'%Y-%m-%d %H:%M:%S') - ✓ Successfully submitted job for polygon ${polygon_id} - Job ID: ${job_id}"
        echo "${polygon_id},${job_id},$(date +'%Y-%m-%d %H:%M:%S'),submitted" >> "${LOG_DIR}/submission_log.csv"
        return 0
    else
  
        error_msg=$(echo "$submit_output" | head -1)
        echo "$(date +'%Y-%m-%d %H:%M:%S') - ✗ Failed to submit job for polygon ${polygon_id} - Error: ${error_msg}"
        
        if [[ "$error_msg" == *"QOSMaxSubmitJobPerUserLimit"* ]]; then
            echo "Hit QoS job limit. Will retry later."
            return 2  # Special return code for QoS limit
        else
            echo "${polygon_id},failed,$(date +'%Y-%m-%d %H:%M:%S'),${error_msg}" >> "${LOG_DIR}/submission_log.csv"
            return 1  # General error
        fi
    fi
}

# create submission log header
echo "polygon_id,job_id,submission_time,status" > "${LOG_DIR}/submission_log.csv"


MAX_ALLOWED_JOBS=$(get_max_jobs_allowed)
echo "Maximum allowed jobs for your QoS: $MAX_ALLOWED_JOBS"

# Track which polygons still need processing
remaining_polygons=($(seq $START_POLYGON $END_POLYGON))
total_polygons=${#remaining_polygons[@]}
completed=0

# Continue until all polygons are processed
while [ ${#remaining_polygons[@]} -gt 0 ]; do
    # Check if termination was requested
    if [ $TERMINATING -eq 1 ]; then
        break
    fi
    
    current_jobs=$(count_user_jobs)
    jobs_available=$((MAX_ALLOWED_JOBS - current_jobs))
    
    if [ $jobs_available -le 0 ]; then
        echo "$(date +'%Y-%m-%d %H:%M:%S') - Currently at max jobs ($current_jobs). Waiting $WAIT_SECONDS seconds..."
        # Use sleep with small intervals to allow for quick termination
        for ((i=1; i<=WAIT_SECONDS; i+=5)); do
            if [ $TERMINATING -eq 1 ]; then
                break 2  # Break out of both loops
            fi
            sleep 5
        done
        continue
    fi
    
    echo "$(date +'%Y-%m-%d %H:%M:%S') - Can submit $jobs_available more job(s). ${#remaining_polygons[@]} polygons remaining."
    
    # try to submit as many jobs as possible
    submitted=0
    retries=()
    
    # process a chunk of the remaining polygons
    for polygon in "${remaining_polygons[@]:0:$jobs_available}"; do
        if [ $TERMINATING -eq 1 ]; then
            break 2  # Break out of both loops
        fi
        
        submit_polygon_job $polygon
        submit_status=$?
        
        if [ $submit_status -eq 0 ]; then
            # job successfully submitted
            submitted=$((submitted + 1))
            completed=$((completed + 1))
        elif [ $submit_status -eq 2 ]; then
            # Hht QoS limit, save for retry and stop submitting more
            retries+=($polygon)
            break
        else
            # other error, save for retry
            retries+=($polygon)
        fi
    done
    
    # update the remaining list
    remaining_polygons=("${retries[@]}" "${remaining_polygons[@]:$jobs_available}")
    

    progress=$((100 * completed / total_polygons))
    echo "Progress: $completed/$total_polygons polygons ($progress%)"
    
    # if we submitted jobs, wait a moment before checking again
    if [ $submitted -gt 0 ]; then
        echo "Submitted $submitted job(s). Waiting briefly..."
        sleep 5
    else
        echo "No jobs submitted. Waiting $WAIT_SECONDS seconds to try again..."
        # use sleep with small intervals to allow for quick termination
        for ((i=1; i<=WAIT_SECONDS; i+=5)); do
            if [ $TERMINATING -eq 1 ]; then
                break 2  # Break out of both loops
            fi
            sleep 5
        done
    fi
done

if [ $TERMINATING -eq 0 ]; then
    echo "===== Job Submission Complete ====="
    echo "All jobs for polygons $START_POLYGON to $END_POLYGON have been submitted!"
    echo "Check status with: squeue -u $USER"
    echo "Log files are in: $LOG_DIR"
    echo "Submission record: ${LOG_DIR}/submission_log.csv"
fi