#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
rm -rf $DIR/../../eva_datasets

user=${1:-root}

mysql -u $user -e 'Drop DATABASE eva_catalog;'
mysql -u $user -e 'CREATE DATABASE IF NOT EXISTS eva_catalog;'
