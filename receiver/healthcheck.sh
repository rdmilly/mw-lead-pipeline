#!/bin/bash
# Lead Pipeline Healthcheck - runs every 5 min
LOG=/tmp/pipeline-healthcheck.log
TS=$(date '+%Y-%m-%d %H:%M:%S')

# Test: Is pipeline screen running?
PIPELINE=$(screen -ls 2>/dev/null | grep pipeline-auto)
if [ -z "$PIPELINE" ]; then
  # Check if receiver is even running
  RECV=$(docker ps --format '{{.Names}}' | grep millyext-receiver)
  if [ -z "$RECV" ]; then
    echo "$TS [ALERT] Receiver not running. Starting via compose..." >> $LOG
    cd /opt/projects/millyext-receiver && docker compose up -d 2>&1 >> $LOG
    sleep 10
  fi
  echo "$TS [ALERT] Pipeline screen not running. Restarting..." >> $LOG
  screen -dmS pipeline-auto bash -c 'docker exec millyext-receiver python3 /app/pipeline_auto.py > /tmp/pipeline-continuous.log 2>&1'
  echo "$TS [RESULT] Pipeline restarted" >> $LOG
fi
