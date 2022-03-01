#! /bin/bash -ux

#
# Extract the DRS localization timestamps for a workflow run
# to create a time series graph of the DRS data access rate.
#

# Configure the Terra workflow submission id from which to extract the data.
# Shape 1 - 20k inputs - Oct 2, 2021 11:28 AM PT
# WF_SUBMISSION_ID="f67b144e-5b7c-4361-9c9f-381b4ff7f3e5"

# Shape 2 - 20k inputs scattered by 100 per task - Feb 1, 2022 3:30 PM PT
# WF_SUBMISSION_ID="a98b1b4d-25d5-489a-a955-191334c8ab32"

# Shape 2 - 20k inputs scattered 20 per task - Oct 6, 2021 1:13 PM PT - aborted
# Aborted due to end of test window. At the time, only 143 of 1,000 shards were created.
# Gen3 reported DRS request rate of ~250/RPS
# See: https://nhlbi-biodatacatalyst.slack.com/archives/C01CSE5P7KM/p1633717393028700?thread_ts=1633548575.026200&cid=C01CSE5P7KM
WF_SUBMISSION_ID="698cc797-8235-4585-873a-3d7a68192fa6"

function configure_for_wf_shape1() {
  WF_NAME="ga4ghMd5"
  WF_TASK_NAME='call-md5'
  WF_DRS_LOG_FILENAME='md5.log'
  GSUTIL_DRS_LOG_PATH="${WORKSPACE_BUCKET}/${WF_SUBMISSION_ID}/${WF_NAME}/**/${WF_TASK_NAME}/${WF_DRS_LOG_FILENAME}"
}

function configure_for_wf_shape2() {
  WF_NAME="md5_n_by_m_scatter"
  WF_TASK_NAME='call-md5s'
  WF_DRS_LOG_FILENAME='*.log'
  GSUTIL_DRS_LOG_PATH="${WORKSPACE_BUCKET}/${WF_SUBMISSION_ID}/${WF_NAME}/**/${WF_TASK_NAME}/**/${WF_DRS_LOG_FILENAME}"
}

function convert_timestamps_to_timeseries() {
  timestamp_input_filename=$1
  timeseries_output_filename=$2
  echo -e 'Timestamp\tCount' >"$timeseries_output_filename"
  sed -e 's/$/\t1/' "$timestamp_input_filename" >>"$timeseries_output_filename"
}

# Configure for the workflow shape of the provided WF_SUBMISSION_ID
# configure_for_wf_shape1
configure_for_wf_shape2

WORKING_DIR="submission_${WF_SUBMISSION_ID}"
# rm -rf ${WORKING_DIR}
mkdir ${WORKING_DIR}

DRS_LOG_LIST="${WORKING_DIR}/drs_log_list.txt"
DRS_LOCALIZATION_LOG_LINES="${WORKING_DIR}/drs_localization_log_lines.txt"
DRS_LOCALIZATION_TIMESTAMPS="${WORKING_DIR}/drs_localization_timestamps.txt"
DRS_LOCALIZATION_TIMESERIES="${WORKING_DIR}/drs_localization_timeseries.tsv"

time (gsutil ls -r "${GSUTIL_DRS_LOG_PATH}" >$DRS_LOG_LIST)
wc -l $DRS_LOG_LIST

# This doesn't work because the destination file name is the same - no path is created.
# time (cat $DRS_LOG_LIST | gsutil -m cp -I ${WORKING_DIR})

# Extract the DRS localization log entries from the workflow logs in the workspace bucket
# shellcheck disable=SC2002
time (cat ${DRS_LOG_LIST} | xargs -n 10 -P 3 -I gs_uris gsutil cat gs_uris | grep -F  "Localizing input drs://" >${DRS_LOCALIZATION_LOG_LINES})

# Extract the timestamps from the workflow DRS localization log entries.
cut -c 1-20 ${DRS_LOCALIZATION_LOG_LINES} | sort >${DRS_LOCALIZATION_TIMESTAMPS}

convert_timestamps_to_timeseries ${DRS_LOCALIZATION_TIMESTAMPS} ${DRS_LOCALIZATION_TIMESERIES}

echo Done!
