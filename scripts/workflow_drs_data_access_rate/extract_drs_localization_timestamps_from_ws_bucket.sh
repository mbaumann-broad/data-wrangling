#! /bin/bash -ux

#
# Extract the DRS localization timestamps from workflow logs
# in the workspace bucket to create a time series graph of the DRS data access rate.
#

## Configure the Terra workflow submission id from which to copy the logs.
 #
 ## Shape 1 - 5k inputs - Submitted Feb 17, 2022 6:40 PM ET
 ## NOTE: The BDC Gen3 staging deployment was unknowingly configured
 ## for a lower level of scalability than BDC Gen3 production at the time this test was run.
 ## WF_SUBMISSION_ID="1b21ce93-6c6f-48a6-a82a-615dc02f5fce"
 #
 ## Shape 1 - 5k inputs - Submitted Feb 17, 2022 7:50 PM ET
 ## NOTE: The BDC Gen3 staging deployment was unknowingly configured
 ## for a lower level of scalability than BDC Gen3 production at the time this test was run.
 ## WF_SUBMISSION_ID="6f10eda8-999a-470e-a700-5b5fc3d3cb1e"
 #
 ## Shape 1 - 5k inputs - Submitted Feb 18, 2022, 5:26 PM ET
 ## At the time of this test, the BDC Gen3 staging deployment was configured to
 ## scale similarly to BDC Gen3 production - and it showed in
 ## much improved results compared to the runs above.
 ## All 5,000 workflows completed successfully.
 ## WF_SUBMISSION_ID="b739c1bc-2d10-4863-b7cc-3c0b8910d7b3"
 #
 ## Shape 2 - 10k inputs scattered 100/task - Submitted Feb 18, 2022 6:21 PM ET
 ## At the time of this test, the BDC Gen3 staging deployment
 ## was configured to scale similarly to BDC Gen3 production.
 ## WF_SUBMISSION_ID="0441f747-18e6-4ef1-95b1-51d1d5ed75b1"
 #
 ## Shape 2 - 10k inputs scattered 10/task - Submitted Feb 18, 2022 7:55 PM ET
 ## At the time of this test, the BDC Gen3 staging deployment
 ## was configured to scale similarly to BDC Gen3 production.
 WF_SUBMISSION_ID="1a72b974-00c4-4316-86d5-7a7b1045f9ef"


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
mkdir -p ${WORKING_DIR}

DRS_LOG_LIST="${WORKING_DIR}/drs_log_list.txt"
DRS_LOCALIZATION_LOG_LINES="${WORKING_DIR}/drs_localization_log_lines.txt"
DRS_LOCALIZATION_TIMESTAMPS="${WORKING_DIR}/drs_localization_timestamps.txt"
DRS_LOCALIZATION_TIMESERIES="${WORKING_DIR}/drs_localization_timeseries.tsv"

time (gsutil ls -r "${GSUTIL_DRS_LOG_PATH}" >"$DRS_LOG_LIST")
wc -l "$DRS_LOG_LIST"

# This doesn't work because the destination file name is the same - no path is created.
# time (cat $DRS_LOG_LIST | gsutil -m cp -I ${WORKING_DIR})

# Extract the DRS localization log entries from the workflow logs in the workspace bucket
# shellcheck disable=SC2002
time (cat "${DRS_LOG_LIST}" | xargs -n 10 -P 3 -I gs_uris gsutil cat gs_uris | grep -F  "Localizing input drs://" >"${DRS_LOCALIZATION_LOG_LINES}")

# Extract the timestamps from the workflow DRS localization log entries.
cut -c 1-20 "${DRS_LOCALIZATION_LOG_LINES}" | sort >"${DRS_LOCALIZATION_TIMESTAMPS}"

convert_timestamps_to_timeseries "${DRS_LOCALIZATION_TIMESTAMPS}" "${DRS_LOCALIZATION_TIMESERIES}"

echo Done!
