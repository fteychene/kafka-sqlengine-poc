#!/usr/bin/env bash

set -eu -o pipefail

kafka-topics --bootstrap-server localhost:19092 --topic test --create

kcat -b localhost:19092 -t test -T -K: -P -l tests_data