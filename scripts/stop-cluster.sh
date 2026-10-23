#!/usr/bin/env bash

if [ -f .cluster-pids.json ]; then
  pkill -f fluxgate-allocator || true
  pkill -f fluxgate-node || true
  rm -f .cluster-pids.json
  echo "Cluster stopped."
else
  pkill -f fluxgate-allocator || true
  pkill -f fluxgate-node || true
  echo "Processes stopped."
fi
