#!/bin/bash
set -e

: ${CD:="../graphtoolsCD"}
echo "Exporting GraphTools to $CD"

if ! git branch | grep '*' | grep "${1:-master}"; then
	echo "Not in ${1:-master} branch!"
	exit 13
fi

if git status -s | grep "^.M"; then
	echo "Changes exists!  They cannot be exported to '$CD'.  Aborting"
	exit 14
fi

[ -e "$CD" ] && echo "Directory already exists: $CD" && exit 12
mkdir "$CD"
git archive ${1:-master} | tar -x -C $CD

cd $CD
echo "Build code so that it is ready to run on delivery"
mvn clean install

pushd dev
mvn clean
popd

{
cd ..
echo "Creating graphtools.zip"
zip -r graphtools-src.zip graphtools/ && rm -rf graphtools/

} >> makeDeliverable-graphtools.log

