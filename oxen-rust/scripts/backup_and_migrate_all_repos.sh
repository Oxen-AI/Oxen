#!/bin/bash

ROOT_PATH=$1
MIGRATION_NAME=$2


if [[ "$ROOT_PATH" == /* ]]; then
    ABSOLUTE_ROOT_PATH="$ROOT_PATH"
    ABSOLUTE_ROOT_PATH="$(realpath $ROOT_PATH)"
else
    ABSOLUTE_ROOT_PATH="$(pwd)/$ROOT_PATH"
    ABSOLUTE_ROOT_PATH="$(realpath $ROOT_PATH)"
fi

# Dir where this script is running - to reference ./backup_and_migrate_repo.sh
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

for namespace in "$ABSOLUTE_ROOT_PATH"/*; do

  if [ -d "$namespace" ]; then
    namespace_name=$(basename "$namespace")

    for repository in "$namespace"/*; do
      if [ -d "$repository" ]; then
        repository_name=$(basename "$repository")

        # Check if the .oxen directory exists in the repository
        if [ -d "$repository/.oxen" ]; then

          # Make the script exectuable
          chmod +x "$DIR/backup_and_migrate_repo.sh"
          "$DIR/backup_and_migrate_repo.sh" "$repository" "$namespace_name/$repository_name" "$MIGRATION_NAME"

          if [ $? -ne 0 ]; then
            echo "Backup and migration failed for $repository"
            # exit 1
          fi
        fi
      fi
    done
  fi
done






