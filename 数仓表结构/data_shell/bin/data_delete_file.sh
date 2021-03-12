#!/bin/bash -e

. ${data_delete_dir:=$(cd `dirname "${BASH_SOURCE[0]}"`;pwd)}/../conf_env/env.sh

find ${1:-$log} -name '*' -type f -mtime +1 -delete
