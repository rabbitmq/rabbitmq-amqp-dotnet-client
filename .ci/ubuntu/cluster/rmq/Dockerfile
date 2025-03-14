ARG RABBITMQ_DOCKER_TAG=rabbitmq:4.1.0-beta.4-management-alpine

FROM ${RABBITMQ_DOCKER_TAG}

ENV RABBITMQ_FEATURE_FLAGS=khepri_db

COPY --chown=rabbitmq:rabbitmq --chmod=0400 erlang.cookie /var/lib/rabbitmq/.erlang.cookie
COPY --chown=rabbitmq:rabbitmq --chmod=0400 ca_certificate.pem /etc/rabbitmq/ca_certificate.pem
COPY --chown=rabbitmq:rabbitmq --chmod=0400 server_localhost_certificate.pem /etc/rabbitmq/server_localhost_certificate.pem
COPY --chown=rabbitmq:rabbitmq --chmod=0400 server_localhost_key.pem /etc/rabbitmq/server_localhost_key.pem
COPY --chown=rabbitmq:rabbitmq --chmod=0644 enabled_plugins /etc/rabbitmq/enabled_plugins
COPY --chown=rabbitmq:rabbitmq rabbitmq-env.conf /etc/rabbitmq/rabbitmq-env.conf
COPY --chown=rabbitmq:rabbitmq rabbitmq.conf /etc/rabbitmq/rabbitmq.conf
COPY --chown=rabbitmq:rabbitmq advanced.config /etc/rabbitmq/advanced.config

EXPOSE 4369 5671 5672 15672 15692 25672 35672-35682
