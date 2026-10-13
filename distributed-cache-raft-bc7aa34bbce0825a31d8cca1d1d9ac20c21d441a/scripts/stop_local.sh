#!/usr/bin/env bash
for f in logs/node*.pid; do
  [ -f "$f" ] && kill "$(cat "$f")" 2>/dev/null || true
done
echo "Stopped nodes."
