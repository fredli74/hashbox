set -x
version=`git describe HEAD --always --abbrev=0`
revision=`git describe HEAD --always`

cd server
rm "$GOPATH/bin/hashbox/"*
GOOS=darwin go build -ldflags "-X main.Version=$revision" -o "$GOPATH/bin/hashbox/hashbox-mac"
GOOS=windows go build -ldflags "-X main.Version=$revision" -o "$GOPATH/bin/hashbox/hashbox-windows.exe"
GOOS=freebsd go build -ldflags "-X main.Version=$revision" -o "$GOPATH/bin/hashbox/hashbox-freebsd-amd64"
GOARM=7 GOOS=linux GOARCH=arm go build -ldflags "-X main.Version=$revision" -o "$GOPATH/bin/hashbox/hashbox-linux-armv7l"
echo $GOARM
cd ../hashback
GOOS=darwin go build -ldflags "-X main.Version=$revision" -o "$GOPATH/bin/hashbox/hashback"
GOOS=windows go build -ldflags "-X main.Version=$revision" -o "$GOPATH/bin/hashbox/hashback.exe"
GOOS=windows GOARCH=386 go build -ldflags "-X main.Version=$revision" -o "$GOPATH/bin/hashbox/hashback-x86.exe"

#-ldflags "-s"  for "release" version

cd $GOPATH/bin/hashbox/
rm *.zip
zip -D "hashbox-mac-amd64-$version.zip"     "hashbox-mac"
zip -D "hashbox-windows-amd64-$version.zip" "hashbox-windows.exe"
zip -D "hashbox-freebsd-amd64-$version.zip" "hashbox-freebsd-amd64"
zip -D "hashbox-linux-armv7l-$version.zip"  "hashbox-linux-armv7l"
zip -D "hashback-mac-amd64-$version.zip"    "hashback"
zip -D "hashback-win-amd64-$version.zip"    "hashback.exe"
zip -D "hashback-win-x86-$version.zip"      "hashback-x86.exe"
