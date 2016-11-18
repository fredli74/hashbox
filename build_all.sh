#!/bin/sh
set -x
version=`git describe HEAD --tags --always --abbrev=0`
revision=`git describe HEAD --tags --always`

rm "$GOPATH/bin/hashbox/"*.zip
rm "$GOPATH/bin/hashbox/"*

BuildAndZip () {
	rm "$GOPATH/bin/hashbox/$2"
	go build -ldflags "-X main.Version=$revision" -o "$GOPATH/bin/hashbox/$2"
	rm "$GOPATH/bin/hashbox/$1-$version.zip"
	zip -jD "$GOPATH/bin/hashbox/$1-$version.zip" "$GOPATH/bin/hashbox/$2"
}

cd server
go fmt
GOOS=freebsd GOARCH=amd64       BuildAndZip "hashbox-freebsd-amd64" "hashbox-freebsd-amd64"
GOOS=linux   GOARCH=arm GOARM=7 BuildAndZip "hashbox-linux-armv7l"  "hashbox-linux-armv7l"
GOOS=darwin  GOARCH=amd64       BuildAndZip "hashbox-mac-amd64"     "hashbox-mac"
GOOS=windows GOARCH=amd64       BuildAndZip "hashbox-windows-amd64" "hashbox-windows.exe"
cd ../hashback
go fmt
GOOS=linux   GOARCH=amd64 BuildAndZip "hashback-linux-amd64" "hashback"
GOOS=darwin  GOARCH=amd64 BuildAndZip "hashback-mac-amd64"   "hashback"
GOOS=windows GOARCH=amd64 BuildAndZip "hashback-win-amd64"   "hashback.exe"
GOOS=windows GOARCH=386   BuildAndZip "hashback-win-x86"     "hashback-x86.exe"

exit 0
