#!/usr/bin/env bash

set -o errexit
set -o pipefail
set -o xtrace

function now
{
    date '+%Y%m%d-%H%M%S'
}

script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
readonly script_dir
echo "$(now) [INFO] script_dir: '$script_dir'"

function run_docker_compose
{
    # shellcheck disable=SC2068
    docker compose --file "$script_dir/docker-compose.yml" $@
}

if [[ $2 == 'arm' ]]
then
    readonly rabbitmq_image="${RABBITMQ_IMAGE:-pivotalrabbitmq/rabbitmq-arm64:main}"
else
    readonly rabbitmq_image="${RABBITMQ_IMAGE:-pivotalrabbitmq/rabbitmq:main}"
fi

if [[ ! -v GITHUB_ACTIONS ]]
then
    GITHUB_ACTIONS='false'
fi

if [[ -d $GITHUB_WORKSPACE ]]
then
    echo "$(now) [INFO] GITHUB_WORKSPACE is set: '$GITHUB_WORKSPACE'"
else
    GITHUB_WORKSPACE="$(cd "$script_dir/../../.." && pwd)"
    echo "$(now) [INFO] set GITHUB_WORKSPACE to: '$GITHUB_WORKSPACE'"
fi

if [[ $1 == 'pull' ]]
then
    readonly docker_fresh='true'
else
    readonly docker_fresh='false'
fi

if [[ $1 == 'stop' ]]
then
    run_docker_compose down
    exit 0
fi

set -o nounset

function start_rabbitmq
{
    echo "$(now) [INFO] starting RabbitMQ cluster via docker compose"
    if [[ $docker_fresh == 'true' ]]
    then
        run_docker_compose build --no-cache --pull --build-arg "RABBITMQ_DOCKER_TAG=$rabbitmq_image"
        run_docker_compose up --pull always --detach
    else
        run_docker_compose build --build-arg "RABBITMQ_DOCKER_TAG=$rabbitmq_image"
        run_docker_compose up --detach
    fi
}

function wait_rabbitmq
{
    set +o errexit
    declare -i count=60
    echo "$(now) [INFO] waiting for rmq0 container to come up..."
    while (( count > 0)) && ! run_docker_compose exec rmq0 rabbitmqctl await_startup >/dev/null 2>&1
    do
        sleep 1
        echo "$(now) [INFO] still waiting for rmq0 container to come up..."
        (( count-- ))
    done
    set -o errexit

    echo "$(now) [INFO] rmq0 container and RabbitMQ are up"

    run_docker_compose exec rmq1 rabbitmqctl await_startup
    echo "$(now) [INFO] rmq1 container and RabbitMQ are up"

    run_docker_compose exec rmq2 rabbitmqctl await_startup
    echo "$(now) [INFO] rmq2 container and RabbitMQ are up"

    run_docker_compose exec rmq0 rabbitmqctl enable_feature_flag all
    run_docker_compose exec rmq0 rabbitmq-diagnostics erlang_version
    run_docker_compose exec rmq0 rabbitmqctl version
}

function install_ca_certificate
{
    set +o errexit
    hostname
    hostname -s
    hostname -f
    openssl version
    openssl version -d
    set -o errexit

    if [[ $GITHUB_ACTIONS == 'true' ]]
    then
        readonly openssl_store_dir='/usr/lib/ssl/certs'
        sudo cp -vf "$GITHUB_WORKSPACE/.ci/certs/ca_certificate.pem" "$openssl_store_dir"
        sudo ln -vsf "$openssl_store_dir/ca_certificate.pem" "$openssl_store_dir/$(openssl x509 -hash -noout -in $openssl_store_dir/ca_certificate.pem).0"
    else
        echo "[WARNING] you must install '$GITHUB_WORKSPACE/.ci/certs/ca_certificate.pem' manually into your trusted root store"
    fi

    for PORT in 5671 5681 5691
    do
        openssl s_client -connect "localhost:$PORT" \
            -CAfile "$GITHUB_WORKSPACE/.ci/certs/ca_certificate.pem" \
            -cert "$GITHUB_WORKSPACE/.ci/certs/client_localhost_certificate.pem" \
            -key "$GITHUB_WORKSPACE/.ci/certs/client_localhost_key.pem" \
            -pass pass:grapefruit < /dev/null
    done
}

start_rabbitmq

wait_rabbitmq

install_ca_certificate
