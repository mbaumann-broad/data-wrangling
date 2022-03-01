#! /bin/bash -u

#
# Copy Terra workflow selected log files from a Terra workspace bucket
# to the local file system to facilitate exploratory mining of them.
#
# Using gsutil directly for this doesn't work because when copying from
# a bucket path to the local filesystem, the path is discarded and only the
# file name is used for the local file systems. Because all the selected
# log files have the same file name, the same file is overwritten many times.
#

# Configure the Terra workflow submission id from which to copy the logs.

# Shape 1 - 5k inputs - Submitted Feb 17, 2022 6:40 PM ET
# NOTE: The BDC Gen3 staging deployment was unknowingly configured
# for a lower level of scalability than BDC Gen3 production at the time this test was run.
# WF_SUBMISSION_ID="1b21ce93-6c6f-48a6-a82a-615dc02f5fce"

# Shape 1 - 5k inputs - Submitted Feb 17, 2022 7:50 PM ET
# NOTE: The BDC Gen3 staging deployment was unknowingly configured
# for a lower level of scalability than BDC Gen3 production at the time this test was run.
# WF_SUBMISSION_ID="6f10eda8-999a-470e-a700-5b5fc3d3cb1e"

# Shape 1 - 5k inputs - Submitted Feb 18, 2022, 5:26 PM ET
# At the time of this test, the BDC Gen3 staging deployment was configured to
# scale similarly to BDC Gen3 production - and it showed in
# much improved results compared to the runs above.
# All 5,000 workflows completed successfully.
WF_SUBMISSION_ID="b739c1bc-2d10-4863-b7cc-3c0b8910d7b3"

# Shape 2 - 10k inputs scattered 100/task - Submitted Feb 18, 2022 6:21 PM ET
# At the time of this test, the BDC Gen3 staging deployment
# was configured to scale similarly to BDC Gen3 production.
# WF_SUBMISSION_ID="0441f747-18e6-4ef1-95b1-51d1d5ed75b1"

# Shape 2 - 10k inputs scattered 10/task - Submitted Feb 18, 2022 7:55 PM ET
# At the time of this test, the BDC Gen3 staging deployment
# was configured to scale similarly to BDC Gen3 production.
# WF_SUBMISSION_ID="1a72b974-00c4-4316-86d5-7a7b1045f9ef"


function configure_for_wf_shape1 {
  WF_NAME="ga4ghMd5"
  WF_TASK_NAME='call-md5'
  WF_DRS_LOG_FILENAME='md5.log'
  # shellcheck disable=SC2034
  GSUTIL_DRS_LOG_PATH="${WORKSPACE_BUCKET}/${WF_SUBMISSION_ID}/${WF_NAME}/**/${WF_TASK_NAME}/${WF_DRS_LOG_FILENAME}"
}

function configure_for_wf_shape2 {
  WF_NAME="md5_n_by_m_scatter"
  WF_TASK_NAME='call-md5s'
  WF_DRS_LOG_FILENAME='*.log'
  # shellcheck disable=SC2034
  GSUTIL_DRS_LOG_PATH="${WORKSPACE_BUCKET}/${WF_SUBMISSION_ID}/${WF_NAME}/**/${WF_TASK_NAME}/**/${WF_DRS_LOG_FILENAME}"
}

function copy_gcs_uris_to_local_fs {
  gcs_uris_file="$1"
  local_fs_dest_dir="$2"

  MAX_CONCURRENT_GSUTIL_PROCS=20

  # Create a file containing the source and destination parameters for gsutil copy.
  GSUTIL_COPY_ARGS_FILE=$WORKING_DIR/drs_log_gsutil_copy_args.txt
  rm -f "$GSUTIL_COPY_ARGS_FILE"
  while read -r drs_log_gcs_uri; do
      # Construct the local path based on the GCS URI
      uri_path="$(echo "$drs_log_gcs_uri" | cut --delimiter=/ -f 4-)"
      local_path="$local_fs_dest_dir/$uri_path"

      # Write the gsutil copy source URI and destination file
      echo "$drs_log_gcs_uri $local_path" >> "$GSUTIL_COPY_ARGS_FILE"
  done < "$gcs_uris_file"

  # Verify the line count of the in log list file and the args file is the same.
  # TODO Programmatically verify this and if not true exit with an error
  wc -l "$GSUTIL_COPY_ARGS_FILE"

  # Perform concurrent gsutil copies using xargs to provide the process control.
  xargs -P $MAX_CONCURRENT_GSUTIL_PROCS -a "$GSUTIL_COPY_ARGS_FILE" -n 2 gsutil cp
}

# Configure for the workflow shape of the provided WF_SUBMISSION_ID
configure_for_wf_shape1
# configure_for_wf_shape2

WORKING_DIR="./submission_${WF_SUBMISSION_ID}"
# rm -rf ${WORKING_DIR}
mkdir -p ${WORKING_DIR}

WORKFLOW_LOG_DIR="${WORKING_DIR}/workflow-logs"
# rm -rf "${WORKFLOW_LOG_DIR"
mkdir -p "$WORKFLOW_LOG_DIR"

DRS_LOG_LIST="${WORKING_DIR}/drs_log_list.txt"
# rm -f "$DRS_LOG_LIST"

# Create a list of selected log file GCS URIs
time (gsutil ls -r "${GSUTIL_DRS_LOG_PATH}" > "$DRS_LOG_LIST")
wc -l "$DRS_LOG_LIST"

# This doesn't work because the destination file name is the same - no path is created.
# time (cat $DRS_LOG_LIST | gsutil -m cp -I ${WORKING_DIR})

time copy_gcs_uris_to_local_fs "${DRS_LOG_LIST}" "${WORKFLOW_LOG_DIR}"

echo Done!
