ulimit -n 10240

IN_FILE=$1
SYNC_SERVER=$2

if [ -z "$IN_FILE" ]
then
  echo "No input file specified."
  exit 1
fi

if [ -z "$SYNC_SERVER" ]
then
  echo "No sync server specified."
  exit 1
fi

while read repo; do
  if [ -d $repo ]
  then
    echo "Repo $repo already exists."
  else
    echo "Cloning $repo...."

    oxen clone https://staging.hub.oxen.ai/ox/$repo --all
    cd $repo
    oxen config --set-remote origin https://$SYNC_SERVER/ox/$repo
    oxen push origin main
    cd ..
  fi
done < $IN_FILE
