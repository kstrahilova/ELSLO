set -e

if [ -n "${HOST_HEADER:-}" ]; then
  /usr/local/bin/wrk -D exp -t "$NUM_THREADS" -c "$NUM_CONNECTIONS" -d "$DURATION" -L -s ./hotel-reservation.lua -H "Host: $HOST_HEADER" "$SERVICE_URL" -R "$RPS"
else
  /usr/local/bin/wrk -D exp -t "$NUM_THREADS" -c "$NUM_CONNECTIONS" -d "$DURATION" -L -s ./hotel-reservation.lua "$SERVICE_URL" -R "$RPS"
fi