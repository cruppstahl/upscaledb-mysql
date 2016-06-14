#!/bin/bash

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

mysql="/usr/local/mysql/bin/mysql --force --batch -v --user=root --database=test"

function run {
  file=$1
  base=`basename $file`
  echo $file
  cat $file | $mysql > /tmp/$base
  diff $file.golden /tmp/$base
  if [ "$?" -ne "0" ]
  then
    echo -e "${RED}[fail]${NC} $file"
  else
    echo -e "${GREEN}[ok]${NC}   $file"
  fi
}

function run_all {
  for dir in `find . -mindepth 1 -type d`
  do
    for file in `ls $dir/*.txt`
    do
      run $file
    done
  done
}

if [ "$1" = "" ]
then
  run_all
else
  for file in `ls $1`
  do
    run $file
  done
fi

