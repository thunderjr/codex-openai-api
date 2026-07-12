#!/bin/sh
set -eu

while IFS= read -r line; do
    method=$(printf '%s' "$line" | sed -n 's/.*"method":"\([^"]*\)".*/\1/p')
    id=$(printf '%s' "$line" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')
    case "$method" in
        initialize)
            printf '{"id":%s,"result":{"userAgent":"fake","codexHome":"/tmp","platformFamily":"unix","platformOs":"linux"}}\n' "$id"
            ;;
        thread/start)
            printf '{"id":%s,"result":{"thread":{"id":"thread-test","ephemeral":true}}}\n' "$id"
            ;;
        turn/start)
            printf '{"id":%s,"result":{"turn":{"id":"turn-test","status":"inProgress","items":[],"error":null}}}\n' "$id"
            printf '%s\n' '{"method":"item/agentMessage/delta","params":{"threadId":"thread-test","turnId":"turn-test","itemId":"item-test","delta":"fake response"}}'
            printf '%s\n' '{"method":"turn/completed","params":{"threadId":"thread-test","turnId":"turn-test","turn":{"status":"completed"},"usage":null}}'
            ;;
        turn/interrupt)
            printf '{"id":%s,"result":{}}\n' "$id"
            ;;
    esac
done
