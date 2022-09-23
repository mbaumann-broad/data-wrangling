#! /bin/env bash

##############################################################################
# Name
# anvil_samtools.sh
#
# Synopsis
# anvil_samtools.sh [standard samtools command-line options]
#
# Description
# Enables use of samtools for Google "gs://" URIs in requester pays buckets.
# This is useful for accessing AnVIL data for which DRS URIs are not yet
# available. This is designed to be run in a Terra Terminal or as a
# system command from within a Terra Jupyter Notebook.
#
# Requirements/Dependencies
# samtools version 1.13 or later (1.15 or later recommended)
#
# Installation
# 1. Put this file (anvil_samtools.sh) in the Terra Cloud Environment
# 2. In a Terra Terminal, run:
#       chmod +x ./anvil_samtools.sh
# 3. (Optional) Add the directory containing anvil_samtools.sh to the PATH
#    environment variable.
#
# Example Use
# Run "samtools view -H" on an AnVIL 1,000 Genomes CRAM file:
#   ./anvil_samtools.sh view -H gs://fc-56ac46ea-efc4-4683-b6d5-6d95bed41c5e/CCDG_13607/Project_CCDG_13607_B01_GRM_WGS.cram.2019-02-06/Sample_HG02373/analysis/HG02373.final.cram HG02373.final.cram
#
##############################################################################

# Set the Google project to bill for requester pays access.
# When run in a Terra workspace Terminal, this is the workspace Google project.
export GCS_REQUESTER_PAYS_PROJECT=${GOOGLE_PROJECT}

# Obtain a token for the user's current Google credentials
# When run in a Terra Terminal, these are the credentials of the
# Terra user's pet service account.
export GCS_OAUTH_TOKEN=$(gcloud auth print-access-token)

# Run samtools with the given command-line options
samtools "$@"
