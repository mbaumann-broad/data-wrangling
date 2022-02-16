#! /bin/bash -ux

#
# Extract the DRS localization timestamps for a workflow run
# to create a time series graph of the DRS data access rate.
#


# Configure the Terra workflow submission id from which to extract the data.
# Shape 1 - 20k inputs
# WF_SUBMISSION_ID="f67b144e-5b7c-4361-9c9f-381b4ff7f3e5"
# Shape 2 - 20k inputs
WF_SUBMISSION_ID="a98b1b4d-25d5-489a-a955-191334c8ab32"


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


# Configure for the workflow shape of the provided WF_SUBMISSION_ID
# configure_for_wf_shape1
configure_for_wf_shape2



WORKING_DIR="submission${WF_SUBMISSION_ID}"
# rm -rf ${WORKING_DIR}
mkdir ${WORKING_DIR}

DRS_LOG_LIST="${WORKING_DIR}/drs_log_list.txt"
DRS_LOCALIZATION_LOG_LINES="${WORKING_DIR}/drs_localization_log_lines.txt"
DRS_LOCALIZATION_TIMESTAMPS="${WORKING_DIR}/drs_localization_timestamps.txt"

time (gsutil ls -r ${GSUTIL_DRS_LOG_PATH} > $DRS_LOG_LIST)
wc -l $DRS_LOG_LIST

# This doesn't work because the destination file name is the same - no path is created.
# time (cat $DRS_LOG_LIST | gsutil -m cp -I ${WORKING_DIR})

# Extract the DRS localization log entries from the workflow logs in the workspace bucket
time (cat ${DRS_LOG_LIST}| xargs -n 10 -P 3 -I gs_uris gsutil cat gs_uris | grep "Localizing input drs://" > ${DRS_LOCALIZATION_LOG_LINES})

# Extract the timestamps from the workflow DRS localization log entries.
cut -c 1-20 ${DRS_LOCALIZATION_LOG_LINES} | sort > ${DRS_LOCALIZATION_TIMESTAMPS}

echo Done!
