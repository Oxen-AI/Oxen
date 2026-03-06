#!/usr/bin/env bash
set -euo pipefail

REPO_PATH=${1:-}
FILEPATH=${2:-}
MIGRATION_NAME=${3:-}

BUCKET_NAME="test-repo-backups" #TODONOW CHANGE
TIMESTAMP=$(date "+%Y%m%d-%H%M%S")

# Check params
if [ -z "$REPO_PATH" ] || [ -z "$MIGRATION_NAME" ] || [ -z "$FILEPATH" ]; then
  echo "Usage: $0 <repo_path> <dest_path_prefix> <migration_name>"
  exit 1
fi

echo "Received Arguments: $1, $2, $3"

if [[ "$REPO_PATH" == /* ]]; then
    ABSOLUTE_REPO_PATH="$REPO_PATH"
else
    ABSOLUTE_REPO_PATH="$(pwd)/$REPO_PATH"
fi

# 1. Save the repo to a tarball
if ! oxen save "$REPO_PATH" -o "$ABSOLUTE_REPO_PATH.tar.gz"; then
  echo "Error saving repo"
  exit 1
fi

# 2. Upload the tarball to S3
if ! aws s3 cp "$REPO_PATH.tar.gz" "s3://$BUCKET_NAME/$FILEPATH/$TIMESTAMP.tar.gz"; then
  echo "aws s3 cp failed"
  exit 1
fi

# Step 3: Verify that the tarball has been uploaded to s3
if ! aws s3 ls "s3://$BUCKET_NAME/$FILEPATH.tar.gz"; then
  echo "Verification failed, tarball not found in S3"
  exit 1
fi

# Step 4: Run migration
if ! (cd "$ABSOLUTE_REPO_PATH" && oxen migrate up "$MIGRATION_NAME" ./); then
  echo "Migration failed"
  exit 1
fi

# Step 5: Delete the local tarball
echo "Attempting to delete $ABSOLUTE_REPO_PATH.tar.gz"
if [ -e "$ABSOLUTE_REPO_PATH.tar.gz" ]; then
  echo "$ABSOLUTE_REPO_PATH.tar.gz exists"
else
  echo "$ABSOLUTE_REPO_PATH.tar.gz does not exist"
fi

if ! rm -f "$ABSOLUTE_REPO_PATH.tar.gz"; then
  echo "Tarball deletion failed"
  exit 1
fi

echo "Backup and Migration completed successfully"
