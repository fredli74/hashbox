#
# Setup hashback client on linux
#

platform=linux-amd64

# 1. Create upgrade script
mkdir -p ~/.hashback
cat >~/.hashback/update-hashback.sh <<EOL
#!/bin/sh
set -x
base=https://github.com/fredli74/hashbox/releases
latest=\$(curl -Ls -o /dev/null -w %{url_effective} \$base/latest)
version=\${latest##*/}
url=\$base/download/\$version/hashback-$platform-\$version.zip
echo Updating hashback-$platform to $version
curl -k -sS -f -L -o temp.zip "\$url" || { echo ERROR downloading latest hashback version 1>&2 ; exit 1 ; }
sudo unzip -o -d  /usr/local/bin/ ./temp.zip
rm ./temp.zip
chmod +x /usr/local/bin/hashback
/usr/local/bin/hashback -version
EOL
chmod +x ~/.hashback/update-hashback.sh

# 2. Download client
~/.hashback/update-hashback.sh

# 3. Create a cron job
#  - On row "hashback store `hostname -f` /" 
#  - Change `hostname -f` if you want a different backupset name
#  - Change / to all the paths you need to backup (space delimited)
cat >~/.hashback/cronjob.sh <<EOL
#!/bin/bash
set -eu

OUT=${TMPDIR:-/tmp/}hashback.out.\$\$
ERR=${TMPDIR:-/tmp/}hashback.err.\$\$
LOG=~/.hashback/hashback.log
LOCK=${TMPDIR:-/tmp/}hashback.lock

[ -f \$LOCK ] && { echo "Already started, remove "\$LOCK" to release lock."; exit 1; }

set +e
echo \$\$ > "\$LOCK"

### ADD BACKUP JOBS HERE
( /usr/local/bin/hashback store `hostname -f` /   > >(tee \$OUT) 2> >(tee \$ERR) ) >\$LOG; RESULT=\$?

rm -f "\$LOCK"
set -e

if [ \$RESULT -ne 0 ]
    then
    echo "Cron failure or error output for the command:"
    echo "\$@"
    echo
    echo "RESULT CODE: \$RESULT"
    echo
    echo "ERROR OUTPUT:"
    cat "\$ERR"
    echo
    echo "STANDARD OUTPUT:"
    cat "\$OUT"
fi

rm -f "\$OUT"
rm -f "\$ERR"
EOL
chmod +x ~/.hashback/cronjob.sh

# 4. Schedule the cron job
# 
# crontab -e 
# 42 * * * * ~/.hashback/cronjob.sh
#
# or 
#
# sudo ln --symbolic ~/.hashback/cronjob.sh /etc/cron.hourly/hashback
