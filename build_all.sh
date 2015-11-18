set -x
if [ $# -eq 0 ]
  then
    echo "No version supplied, try ./build_all.sh 0.2"
    exit
fi
echo 

cd server
GOOS=darwin go build -o "$GOPATH/bin/hashbox/hashbox-mac-amd64"
GOOS=windows go build -o "$GOPATH/bin/hashbox/hashbox-windows-amd64.exe"
GOOS=freebsd go build -o "$GOPATH/bin/hashbox/hashbox-freebsd-amd64"
GOARM=7 GOOS=linux GOARCH=arm go build -o "$GOPATH/bin/hashbox/hashbox-linux-armv7l"
echo $GOARM
cd ../hashback
GOOS=darwin go build -o "$GOPATH/bin/hashbox/hashback"
GOOS=windows go build -o "$GOPATH/bin/hashbox/hashback.exe"

#-ldflags "-s"  for "release" version

zip "$GOPATH/bin/hashbox/hashbox-mac-amd64-$1.zip"     "$GOPATH/bin/hashbox/hashbox-mac-amd64"
zip "$GOPATH/bin/hashbox/hashbox-windows-amd64-$1.zip" "$GOPATH/bin/hashbox/hashbox-windows-amd64.exe"
zip "$GOPATH/bin/hashbox/hashbox-freebsd-amd64-$1.zip" "$GOPATH/bin/hashbox/hashbox-freebsd-amd64"
zip "$GOPATH/bin/hashbox/hashbox-linux-armv7l-$1.zip"  "$GOPATH/bin/hashbox/hashbox-linux-armv7l"
zip "$GOPATH/bin/hashbox/hashback-mac-$1.zip"          "$GOPATH/bin/hashbox/hashback"
zip "$GOPATH/bin/hashbox/hashback-win-$1.zip"          "$GOPATH/bin/hashbox/hashback.exe"
