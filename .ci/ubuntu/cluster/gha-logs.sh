#!/bin/bash

set -o errexit
set -o pipefail
set -o xtrace

readonly arg1="${1:-undefined}"

function now
{
    date '+%Y%m%d-%H%M%S'
}

script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
readonly script_dir
readonly log_dir="$script_dir/log"
echo "$(now) [INFO] log_dir: '$log_dir'"

function run_docker_compose
{
    # shellcheck disable=SC2068
    docker compose --file "$script_dir/docker-compose.yml" $@
}

set -o nounset

run_docker_compose logs rmq0 > "$log_dir/rmq0.log"
run_docker_compose logs rmq1 > "$log_dir/rmq1.log"
run_docker_compose logs rmq2 > "$log_dir/rmq2.log"

if [[ $arg1 == 'check' ]]
then
    if grep -iF inet_error "$log_dir"/*
    then
        echo '[ERROR] found inet_error in RabbitMQ logs' 1>&2
        exit 1
    fi
fi
