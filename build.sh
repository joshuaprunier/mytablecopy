#!/bin/bash

major_rev="1"
minor_rev="0"
commit_num=`git shortlog -s | awk '{ sum += $1 } END { print sum }'`
gitsha=`git log -n1 --pretty="%h"`
date=`date +"%Y%m%d"`
time=`date +"%H%M%S"`

version=${major_rev}.${minor_rev}.${commit_num}-${gitsha}-${date}.${time}

echo
echo "Building mytablecopy version"
echo ${version}
echo


# Linux
echo
echo "Building Linux"
mkdir -p bin/linux
go build -ldflags "-X main.versionInformation=$version" -o bin/linux/mytablecopy mytablecopy.go
if [[ $? -eq 0 ]]; then
        echo "  mytablecopy - OK"
else
        echo "  mytablecopy - FAILED"
fi

# Windows
echo
echo "Building Windows"
mkdir -p bin/windows
GOOS=windows GOARCH=amd64 go build -ldflags "-X main.versionInformation=$version" -o bin/windows/mytablecopy.exe mytablecopy.go
if [[ $? -eq 0 ]]; then
        echo "  mytablecopy.exe - OK"
else
        echo "  mytablecopy.exe - FAILED"
fi

# Darwin
echo
echo "Building Darwin"
mkdir -p bin/darwin
GOOS=darwin GOARCH=amd64 go build -ldflags "-X main.versionInformation=$version" -o bin/darwin/mytablecopy mytablecopy.go
if [[ $? -eq 0 ]]; then
        echo "  mytablecopy - OK"
else
        echo "  mytablecopy - FAILED"
fi

echo
echo "Done!"
