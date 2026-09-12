#!/bin/sh
# watchdog.sh — restarts server.js if it ever exits/crashes.
# Resolves its own folder, so it works regardless of where this repo is
# checked out.
#
# Run in the foreground: ./watchdog.sh
# Run in the background: nohup ./watchdog.sh >/dev/null 2>&1 &
# Auto-start at login (systemd user service, Linux):
#   systemd-run --user --unit=gemini-proxy --same-dir -- sh "$(pwd)/watchdog.sh"
# Auto-start at login (macOS): wrap this script in a launchd .plist, or
# just run the "background" command above from a login item / shell profile.

DIR="$(cd "$(dirname "$0")" && pwd)"
while true; do
  node "$DIR/server.js"
  sleep 3
done
