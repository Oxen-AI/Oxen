#!/usr/bin/env bash
set -euo pipefail

ROOT_PATH=${1:-}
MIGRATION_NAME=${2:-}

if [[ -z "${ROOT_PATH}" ]] ||  [[ -z "${MIGRATION_NAME}" ]]; then
	echo "Error: must provide root path as the 1st argument and the migration name as the 2nd"
	exit 1
fi

if [[ "$ROOT_PATH" == /* ]]; then
    ABSOLUTE_ROOT_PATH="$(realpath "$ROOT_PATH")"
else
    ABSOLUTE_ROOT_PATH="$(realpath "$ROOT_PATH")"
fi

# Dir where this script is running - to reference ./backup_and_migrate_repo.sh
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

N_FAILS=0

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
          if ! "$DIR/backup_and_migrate_repo.sh" "$repository" "$namespace_name/$repository_name" "$MIGRATION_NAME"; then
            echo "Backup and migration failed for $repository"
						N_FAILS=$((N_FAILS + 1))
          fi
        fi
      fi
    done
  fi
done

if [[ "${N_FAILS}" -ne 0 ]]; then
  echo "Failed to backup and migrate ${N_FAILS} repositories"
	exit 1
else
	echo "Successfully backed up and migrated all repositories"
fi

