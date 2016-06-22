# 1. Create upgrade script
platform=mac-amd64
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

# 3. Save local settings in an options file
#  - Set user, password and server ip:port
# hashback -user=user -password=password -server=ip:port -ignore=".dropbox.cache/" -saveoptions

# 4. Remove old plist schedule (if upgrading)
launchctl stop se.elysian.hashback.plist
launchctl unload ~/Library/LaunchAgents/se.elysian.hashback.plist
launchctl remove se.elysian.hashback.plist

# 5. Create a plist schedule
#  - Set RunAtLoad to true if you do not care what time of day the backup is started
#  - Set StartCalendarInterval to an hour or minute per day the backup should run
#  - Set or remove -interval, default is once every 24 hours
#  - Change or add additional paths at the end of ProgramArguments, default is only your home folder
#
cat >~/Library/LaunchAgents/se.elysian.hashback.plist <<EOL
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>se.elysian.hashback.plist</string>

  <key>ProgramArguments</key>
  <array>
    <string>/usr/local/bin/hashback</string>
    <string>-verbose</string>
    <string>-progress</string>
    <string>-retaindays=7</string>
    <string>-retainweeks=24</string>
    <string>-interval=1440</string>
    <string>store</string>
    <string>$(hostname -s)</string>
    <string>$HOME/</string>
  </array>

  <key>Nice</key>
  <integer>-20</integer>

  <key>RunAtLoad</key>
  <false/>

  <key>KeepAlive</key>
  <dict>
    <key>SuccessfulExit</key>
    <false/>
  </dict>

  <key>StartCalendarInterval</key>
  <dict>
    <key>Minute</key>
    <integer>01</integer>
  </dict>

  <key>LowPriorityIO</key>
  <true/>

  <key>StandardErrorPath</key>
  <string>$HOME/Library/Logs/hashback.log</string>

  <key>StandardOutPath</key>
  <string>$HOME/Library/Logs/hashback.log</string>
</dict>
</plist>
EOL


# 6. Load the schedule (or reboot)
#
launchctl load ~/Library/LaunchAgents/se.elysian.hashback.plist


# 7. Manual start
#
launchctl start se.elysian.hashback.plist


# 8. Monitor the backup
#
tail -F ~/Library/Logs/hashback.log
