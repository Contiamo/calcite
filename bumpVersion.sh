#!/bin/bash

pom='pom.xml'

if [ ! -f ./$pom ] 
  then         
    echo "Cannot find $pom in current dir"
    exit 1;    
fi

currVerParts=$(cat $pom | sed -n 's/<version>\(1\.[0-9]\+\.[0-9]\+\)-c\([0-9]\+\)-SNAPSHOT<\/version>.*/\1-\2/p' | head -n 1)
currInternalVer=$(echo $currVerParts | cut -d '-' -f 2)
currCalciteVer=$(echo $currVerParts | cut -d '-' -f 1)
nextInternalVer=$(($currInternalVer+1))

currVer=$currCalciteVer-c$currInternalVer
nextVer=$currCalciteVer-c$nextInternalVer
echo current version = $currVer
echo next version = $nextVer

echo start replacing versions in all $pom files 

for f in `find . -name 'pom.xml'`
do
  echo processing $f
  sed -i "s/$currVer/$nextVer/g" $f
done

echo done
