#!/bin/sh
set -e

if [ -n "$UMASK" ]; then
    umask "$UMASK"
fi

if [ -n "$PUID" ] || [ -n "$PGID" ]; then
    UID_VAL="${PUID:-0}"
    GID_VAL="${PGID:-0}"
    chown -R "$UID_VAL:$GID_VAL" /config 2>/dev/null || true
    chown "$UID_VAL:$GID_VAL" /downloads 2>/dev/null || true
    exec su-exec "$UID_VAL:$GID_VAL" /usr/local/bin/rustnzbd "$@"
fi

exec /usr/local/bin/rustnzbd "$@"
