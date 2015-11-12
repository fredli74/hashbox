set -x
cd server
GOOS=darwin go build -o "$GOPATH/bin/hashbox/hashbox-mac-amd64"
GOOS=windows go build -o "$GOPATH/bin/hashbox/hashbox-windows-amd64.exe"
GOOS=freebsd go build -o "$GOPATH/bin/hashbox/hashbox-freebsd-amd64"
GOARM=7 GOOS=linux GOARCH=arm go build -o "$GOPATH/bin/hashbox/hashbox-linux-armv7l"
echo $GOARM
cd ../hashback
GOOS=darwin go build -o "$GOPATH/bin/hashbox/hashback"
GOOS=windows go build -o "$GOPATH/bin/hashbox/hashback.exe"
