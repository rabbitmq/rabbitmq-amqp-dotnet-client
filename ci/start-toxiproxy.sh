#!/usr/bin/env bash

docker rm -f toxiproxy 2>/dev/null || echo "toxiproxy was not running"
docker run -d --name toxiproxy \
    --network host \
    ghcr.io/shopify/toxiproxy