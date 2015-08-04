#!/bin/bash
#
# PDFMonitor.sh
# 
# Description	: Poll JDE Print Queue and perform post PDF Processing on certain files
# Author	: Paul Green
# Dated		: 2015-07-30
#
# Synopsis
# --------
# This shell program forms part of a dockerised container application
# It runs when the docker container starts
# It Polls the JDE PrintQueue directory on AIX
# Whenever new PDF files are detected it calls a Javascript program that performs
# post PDF processing actions on selected PDF files (see pdfhandler.js for more info).
# It also calls the Javascript PDFHandler program once when started to immediately deal
# with any PDF files sitting in the JDE PrintQueue since this program was last active.
# It establishes an sshfs mount to the AIX remote system that requires monitoring

# Establish mount to remote JDE enterprise server (AIX) system
DYNCMD="sshfs -o cache=no ${SSHFS_USER}@${SSHFS_HOSTDIR} /home/pdfdata"  
$DYNCMD
if [ $? -ne 0 ]
then 
	echo
	echo "--------- ERROR ------------"
	echo "Problem mounting remote directory to monitor"
	echo "Expecting 3 docker -e arguments for User, Pwd and Target Host:Directory"
	echo "Re-run Docker command and provide remote SSHFS User, Pwd and Host:Directory values"
	echo "Tried this command: '${DYNCMD}' but it failed!"
	exit 1
fi

# The absolute path of the directory containing this script.
DIR="$( cd "$( dirname "$0" )" && pwd)"

# Container application creates /home/pdfdata directory which will be mounted using sshfs 
# to the actual remote AIX directory that holds the JDE PrintQueue pdf files.
# Create a list of directories to monitor - in this case just the one.
REMOTE_DIR="/home/pdfdata"


 
# STARTUP / RECOVERY
#
# If this container app crashed or the server was taken offline for a time then on startup
# might need to recover/process any JDE PDF's generated since last time this program was active.
# For example if the container is down no logos will be stamped on Invoice Prints so when it comes 
# back up and this script runs for first time then pass control over to the javascrip PDF handler
# to deal with any un-processed PDF files in the Print Queue  
NODEARGS=" 'S' '${HOSTNAME}' "
node ./src/pdfhandler ${NODEARGS} 

# Establish unique file names for this container to hold the before and after 
# directory snapshots
BEFORE='/tmp/' + &HOSTNAME + '_before.txt'
AFTER='/tmp/' + &HOSTNAME + '_after.txt'

# Take a snapshot of the remote monitored directory before starting to monitor
ls $REMOTE_DIR > $BEFORE 

# Ensure Startup flag is not 'S' for all subsequent calls to pdfhandler set to 'M' for Monitor Loop
NODEARGS=" 'M' '${HOSTNAME}' "



# POLLING 
#
# Loop indefinitely - to stop this program stop the container. 
# This container application exists to monitor this directory and pass control over to the 
# PDF Handler progam to deal with new PDF's
while [[ true ]] ; do
  
  # Take another snapshot of the remote monitored directory for comparison
  ls $REMOTE_DIR > $AFTER
 
  # Compare the Before and After snapshots 
  if diff -q $BEFORE $AFTER > /dev/null; then
     echo 'No changes'
  else
     echo 'Changes detected.....'
	#### node ./src/pdfhandler ${NODEARGS}
  fi

  sleep ${INTERVAL_SECONDS}
done
