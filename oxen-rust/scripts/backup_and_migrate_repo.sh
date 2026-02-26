#!/bin/bash

REPO_PATH=$1
FILEPATH=$2
MIGRATION_NAME=$3


echo "Received Arguments: $1, $2, $3"

BUCKET_NAME="test-repo-backups" #TODONOW CHANGE
TIMESTAMP=$(date "+%Y%m%d-%H%M%S")

# Check params
if [ -z "$REPO_PATH" ] || [ -z "$MIGRATION_NAME" ] || [ -z "$FILEPATH" ]; then
  echo "Usage: $0 <repo_path> <dest_path_prefix> <migration_name>"
  exit 1
fi

if [[ "$REPO_PATH" == /* ]]; then
    ABSOLUTE_REPO_PATH="$REPO_PATH"
else
    ABSOLUTE_REPO_PATH="$(pwd)/$REPO_PATH"
fi

# 1. Save the repo to a tarball
oxen save "$REPO_PATH" -o $ABSOLUTE_REPO_PATH.tar.gz

# Exit if save issues
if [ $? -ne 0 ]; then
  echo "Error saving repo"
  exit 1
fi

# 2. Upload the tarball to S3
aws s3 cp "$REPO_PATH.tar.gz" "s3://$BUCKET_NAME/$FILEPATH/$TIMESTAMP.tar.gz"

# Check if aws s3 cp was successful
if [ $? -ne 0 ]; then
  echo "aws s3 cp failed"
  exit 1
fi

# Step 3: Verify that the tarball has been uploaded to s3
aws s3 ls "s3://$BUCKET_NAME/$FILEPATH.tar.gz"
if [ $? -ne 0 ]; then
  echo "Verification failed, tarball not found in S3"
  exit 1
fi

# Step 4: Run migration
cd "$ABSOLUTE_REPO_PATH" && oxen migrate up "$MIGRATION_NAME" ./

# Check if migration was successful
if [ $? -ne 0 ]; then
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

rm -f "$ABSOLUTE_REPO_PATH.tar.gz"

# Check if tarball deletion was successful
if [ $? -ne 0 ]; then
  echo "Tarball deletion failed"
  exit 1
fi

echo "Backup and Migration completed successfully"