#! /bin/bash -ux

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

# Shape 1 - 5k inputs - Started Feb 17, 2022 6:40 PM ET
# WF_SUBMISSION_ID="1b21ce93-6c6f-48a6-a82a-615dc02f5fce"

# Shape 1 - 5k inputs - Started Feb 17, 2022 7:50 PM ET
WF_SUBMISSION_ID="6f10eda8-999a-470e-a700-5b5fc3d3cb1e"


function configure_for_wf_shape1 {
    WF_NAME="ga4ghMd5"
    WF_TASK_NAME='call-md5'
    WF_DRS_LOG_FILENAME='md5.log'
    GSUTIL_DRS_LOG_PATH="${WORKSPACE_BUCKET}/${WF_SUBMISSION_ID}/${WF_NAME}/**/${WF_TASK_NAME}/${WF_DRS_LOG_FILENAME}"
}

function configure_for_wf_shape2 {
    WF_NAME="md5_n_by_m_scatter"
    WF_TASK_NAME='call-md5s'
    WF_DRS_LOG_FILENAME='*.log'
    GSUTIL_DRS_LOG_PATH="${WORKSPACE_BUCKET}/${WF_SUBMISSION_ID}/${WF_NAME}/**/${WF_TASK_NAME}/**/${WF_DRS_LOG_FILENAME}"
}

function copy_gcs_uri_to_local_fs {
	gcs_uri=$1
	local_dir=$2
	# Construct the local full path
	uri_path=`echo $gcs_uri | cut --delimiter=/ -f 4-`
	local_path=$local_dir/$uri_path
	
	# Wait to start another gsutil process until less than 5 are currently running
	current_jobs=`ps | grep -c gsutil`
	while [ $current_jobs -ge 5 ]
	do
		echo waiting for a current gsutil process to end
		sleep 1
		current_jobs=`ps | grep -c gsutil`
	done
	
	gsutil cp $gcs_uri $local_path &
}


# Configure for the workflow shape of the provided WF_SUBMISSION_ID
# configure_for_wf_shape1
configure_for_wf_shape2



WORKING_DIR="submission_${WF_SUBMISSION_ID}"
# rm -rf ${WORKING_DIR}
mkdir ${WORKING_DIR}

DRS_LOG_LIST="${WORKING_DIR}/drs_log_list.txt"

# time (gsutil ls -r ${GSUTIL_DRS_LOG_PATH} > $DRS_LOG_LIST)
wc -l $DRS_LOG_LIST

# This doesn't work because the destination file name is the same - no path is created.
# time (cat $DRS_LOG_LIST | gsutil -m cp -I ${WORKING_DIR})

while read drs_log_gcs_uri
do
	copy_gcs_uri_to_local_fs $drs_log_gcs_uri $WORKING_DIR
done < ./$DRS_LOG_LIST

echo Done!
