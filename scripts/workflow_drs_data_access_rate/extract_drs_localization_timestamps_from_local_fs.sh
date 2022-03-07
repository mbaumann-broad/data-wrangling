#! /bin/bash -ux

#
# Extract all the DRS localization timestamps from workflow logs
# under a directory on the local file system to create time series data
# for graphing the DRS data access rate.
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
# WF_SUBMISSION_ID="b739c1bc-2d10-4863-b7cc-3c0b8910d7b3"

# Shape 2 - 10k inputs scattered 100/task - Submitted Feb 18, 2022 6:21 PM ET
# At the time of this test, the BDC Gen3 staging deployment
# was configured to scale similarly to BDC Gen3 production.
# WF_SUBMISSION_ID="0441f747-18e6-4ef1-95b1-51d1d5ed75b1"

# Shape 2 - 10k inputs scattered 10/task - Submitted Feb 18, 2022 7:55 PM ET
# At the time of this test, the BDC Gen3 staging deployment
# was configured to scale similarly to BDC Gen3 production.
WF_SUBMISSION_ID="1a72b974-00c4-4316-86d5-7a7b1045f9ef"

function convert_timestamps_to_timeseries() {
  timestamp_input_filename=$1
  timeseries_output_filename=$2
  echo -e 'Timestamp\tCount' >"$timeseries_output_filename"
  sed -e 's/$/\t1/' "$timestamp_input_filename" >>"$timeseries_output_filename"
}

WORKING_DIR="submission_${WF_SUBMISSION_ID}"
# rm -rf ${WORKING_DIR}
mkdir -p ${WORKING_DIR}

WORKFLOW_LOG_DIR="${WORKING_DIR}/workflow-logs"

DRS_LOCALIZATION_LOG_LINES="${WORKING_DIR}/drs_localization_log_lines.txt"
DRS_LOCALIZATION_TIMESTAMPS="${WORKING_DIR}/drs_localization_timestamps.txt"
DRS_LOCALIZATION_TIMESERIES="${WORKING_DIR}/drs_localization_timeseries.tsv"

# Extract the DRS localization log entries from the workflow logs on the local file system
# shellcheck disable=SC2038
time (find "$WORKFLOW_LOG_DIR" -type f | xargs grep -F --no-filename "Localizing input drs://" > "${DRS_LOCALIZATION_LOG_LINES}")

# Extract the timestamps from the workflow DRS localization log entries.
cut -c 1-20 ${DRS_LOCALIZATION_LOG_LINES} | sort >"${DRS_LOCALIZATION_TIMESTAMPS}"

convert_timestamps_to_timeseries "${DRS_LOCALIZATION_TIMESTAMPS}" "${DRS_LOCALIZATION_TIMESERIES}"

echo Done!
