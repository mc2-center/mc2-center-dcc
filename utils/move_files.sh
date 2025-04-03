#/usr/bin/bash

files=""
target=""

for f in $files;
do
	synapse mv --id "$f" --parentid "$target"
done