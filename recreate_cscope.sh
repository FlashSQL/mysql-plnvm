#!/bin/bash
#This script build cscope files and build ctags Library
MYSQL_HOME=/home/vldb/mysql-plnvm

cd $MYSQL_HOME
find -name "*.h" -o -name "*.hpp" -o -name "*.i" -o -name "*.ic" -o -name "*.in" -o -name "*.c" -o -name "*.cc" -o -name "*.cpp" > cscope.files
cscope -b -q -k
ctags -L cscope.files
