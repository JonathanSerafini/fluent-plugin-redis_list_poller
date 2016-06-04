#!/bin/bash

for x in `seq 1 10000`; do
  echo "RPUSH test \"{ \\\"message\\\": \\\"${x}\\\" }\""
done | redis-cli --pipe

