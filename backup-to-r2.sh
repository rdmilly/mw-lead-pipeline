#!/bin/bash
# Lead Pipeline R2 Backup — runs from Contabo
set -e
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
BACKUP_DIR=/tmp/leads-backup-$TIMESTAMP
mkdir -p $BACKUP_DIR
LOG=/var/log/leads-r2-backup.log
log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a $LOG; }

log "=== Leads R2 backup started ==="

# 1. Dump leads-postgres
log "Dumping mw-postgres..."
docker exec mw-postgres pg_dump -U leads leads | gzip > $BACKUP_DIR/leads-postgres-$TIMESTAMP.sql.gz
log "Dump size: $(du -h $BACKUP_DIR/leads-postgres-$TIMESTAMP.sql.gz | cut -f1)"

# 2. Upload to R2
log "Uploading to R2..."
rclone copy $BACKUP_DIR/leads-postgres-$TIMESTAMP.sql.gz r2:millyweb-backups/daily/leads-postgres/ --s3-upload-cutoff 100M 2>/dev/null
log "Uploaded to R2"

# 3. Cleanup old dailies (keep 7 days)
rclone delete r2:millyweb-backups/daily/leads-postgres --min-age 7d 2>/dev/null || true

# 4. Git sync
cd /opt/projects/mw-lead-pipeline
if [ -d .git ]; then
  git add -A 2>/dev/null || true
  git commit -m "backup: $TIMESTAMP" 2>/dev/null || true
  git push origin main 2>/dev/null || true
  log "Git synced"
fi

# Cleanup
rm -rf $BACKUP_DIR
log "=== Leads R2 backup complete ==="
