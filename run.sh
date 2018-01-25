#!/usr/bin/env bash

sudo nvidia-docker run -dit --restart unless-stopped -p 50053:50053 bluelens/bl-index:latest