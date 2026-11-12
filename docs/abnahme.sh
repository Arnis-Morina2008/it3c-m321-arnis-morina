#!/usr/bin/env bash
#
# Abnahme der Live-Zustellung: S1 bis S8 aus Bewertung 2, in der Reihenfolge
# des Auftrags, auf EINEM Stack (Spezifikation Kapitel 6).
#
# Aufruf im Wurzelverzeichnis des Repos:   bash docs/abnahme.sh
# Voraussetzungen: Docker mit compose, Maven, .env (Kopie von .env.example).
#
# Am Schluss steht pro Szenario eine Zeile "S<n>: erwartet ... gemessen ...".
# Alle Hilfscontainer heissen abnahme-..., sie werden am Ende entfernt.

set -u

# --- Vorbereitung ------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR/.."

# Unter Git Bash (Windows) würde ein Pfad wie /stream sonst umgeschrieben.
export MSYS_NO_PATHCONV=1

if [ ! -f .env ]; then
  echo "Keine .env gefunden. Zuerst: cp .env.example .env"
  exit 1
fi
# Passwörter und Benutzer nur aus .env, nie fest im Skript.
set -a
. ./.env
set +a

CURL_IMAGE="curlimages/curl:8.10.1"
NETWORK="chat-net"
WORK_DIR="$(mktemp -d)"
RESULTS=()

# --- Hilfsfunktionen ---------------------------------------------------------

# Hält ein Ergebnis fest und gibt es sofort aus.
record() {
  local line="$1: erwartet $2, gemessen $3 -> $4"
  RESULTS+=("$line")
  echo ">>> $line"
}

# Namen der laufenden chat-service-Container, sortiert (…-1 vor …-2).
# Die Namen hängen vom Ordnernamen ab, darum werden sie nie fest eingetragen.
instance_names() {
  docker compose ps --format '{{.Name}}' chat-service | sort
}

# Anzahl Bindungen am Fanout-Exchange chat.delivery.
count_delivery_bindings() {
  docker compose exec -T rabbitmq rabbitmqctl -q list_bindings source_name destination_name \
    | grep -c '^chat\.delivery[[:space:]]'
}

# Wartet, bis genau so viele Instanz-Queues an chat.delivery gebunden sind.
wait_for_bindings() {
  local expected="$1"
  for i in $(seq 1 60); do
    if [ "$(count_delivery_bindings)" = "$expected" ]; then
      return 0
    fi
    sleep 1
  done
  return 1
}

# Startet einen Stream-Client im Netz chat-net. Seine Ausgabe liest man
# später mit "docker logs". Optionaler dritter Parameter: Last-Event-ID.
start_client() {
  local name="$1" url="$2" last_event_id="${3:-}"
  docker rm -f "$name" >/dev/null 2>&1
  if [ -n "$last_event_id" ]; then
    docker run -d --name "$name" --network "$NETWORK" "$CURL_IMAGE" \
      -sN -H "Last-Event-ID: $last_event_id" "$url" >/dev/null
  else
    docker run -d --name "$name" --network "$NETWORK" "$CURL_IMAGE" -sN "$url" >/dev/null
  fi
  # Warten, bis der Server ":connected" geschickt hat, der Client also angemeldet ist.
  for i in $(seq 1 20); do
    if docker logs "$name" 2>&1 | grep -q '^:connected'; then
      return 0
    fi
    sleep 0.5
  done
  echo "Client $name hat keine Verbindung bekommen"
}

# Anzahl Ereignisse (id:-Zeilen), die ein Client bisher bekommen hat.
count_events() {
  docker logs "$1" 2>&1 | grep -c '^id:'
}

# Wartet, bis ein Client mindestens so viele Ereignisse hat, höchstens $3 Sekunden.
wait_for_events() {
  local name="$1" expected="$2" max_seconds="$3"
  for i in $(seq 1 "$max_seconds"); do
    if [ "$(count_events "$name")" -ge "$expected" ]; then
      return 0
    fi
    sleep 1
  done
}

# Inhalte (content) aller Ereignisse eines Absenders, eine Zahl pro Zeile.
contents_of() {
  local name="$1" sender="$2"
  docker logs "$name" 2>&1 | grep "^data:.*\"senderId\":\"$sender\"" \
    | sed 's/.*"content":"\([0-9]*\)".*/\1/'
}

# Sendet Nachrichten aus EINEM Hilfscontainer mit einer Schleife.
# Parameter: Liste von "instanz raum absender anzahl erste_nummer", durch ";"
# getrennt. Mehrere Absender laufen im selben Container nebeneinander.
send_messages() {
  local jobs="$1"
  local script='
    send() {
      i=$5
      last=$(($5 + $4 - 1))
      while [ $i -le $last ]; do
        curl -s -o /dev/null -X POST "http://$1:8080/messages" \
          -H "Content-Type: application/json" \
          -d "{\"roomId\":\"$2\",\"senderId\":\"$3\",\"senderName\":\"$3\",\"content\":\"$i\"}"
        i=$((i + 1))
      done
    }
    IFS=";"
    for job in $JOBS; do
      IFS=" "
      send $job &
      IFS=";"
    done
    wait'
  docker run --rm --network "$NETWORK" -e JOBS="$jobs" --entrypoint sh "$CURL_IMAGE" -c "$script"
}

# Zeilen in der Tabelle message.
count_rows() {
  docker compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tA \
    -c "SELECT count(*) FROM message;" 2>/dev/null | tr -d '[:space:]'
}

# Wartet, bis chat.persist leer ist, damit vorherige Szenarien fertig geschrieben sind.
wait_for_empty_persist_queue() {
  for i in $(seq 1 60); do
    local waiting
    waiting=$(docker compose exec -T rabbitmq rabbitmqctl -q list_queues name messages \
      | grep '^chat\.persist[[:space:]]' | awk '{print $2}')
    if [ "$waiting" = "0" ]; then
      return 0
    fi
    sleep 1
  done
}

# Räumt Hilfscontainer auf und setzt eine angehaltene Instanz fort, auch bei Abbruch.
cleanup() {
  for name in $(docker ps -aq --filter "name=abnahme-"); do
    docker rm -f "$name" >/dev/null 2>&1
  done
  for name in $(docker compose ps -q --status paused chat-service); do
    docker unpause "$name" >/dev/null 2>&1
  done
  rm -rf "$WORK_DIR"
}
trap cleanup EXIT

# --- S1: mvn clean test ------------------------------------------------------

echo "=== S1: mvn clean test"
# Für Maven die Variable MSYS_NO_PATHCONV weglassen: unter Git Bash findet
# mvn sonst seine eigenen Bibliotheken nicht. Unter Linux ändert das nichts.
if env -u MSYS_NO_PATHCONV mvn -q clean test > "$WORK_DIR/s1.log" 2>&1; then
  record S1 "BUILD SUCCESS" "Exit 0" OK
else
  record S1 "BUILD SUCCESS" "Exit $? (Log: tail $WORK_DIR/s1.log)" FEHLER
  tail -30 "$WORK_DIR/s1.log"
fi

# --- S2: Stack starten, kein Port nach aussen --------------------------------

echo "=== S2: docker compose up -d --build --wait"
docker compose up -d --build --wait
s2_exit=$?
published=$(docker compose ps --format "{{.Service}} {{.Ports}}" | grep -c -- '->')
running=$(docker compose ps --services --status running | wc -l | tr -d ' ')
configured=$(docker compose config --services | wc -l | tr -d ' ')
if [ "$s2_exit" = "0" ] && [ "$published" = "0" ] && [ "$running" = "$configured" ]; then
  s2_status=OK
else
  s2_status=FEHLER
fi
record S2 "Exit 0, $configured Dienste laufen, 0 Ports" \
  "Exit $s2_exit, $running Dienste laufen, $published Ports" "$s2_status"

# --- S3: zwei Instanzen, Raum A und B ---------------------------------------

echo "=== S3: zwei Instanzen, je 200 Nachrichten"
docker compose up -d --scale chat-service=2 --wait
wait_for_bindings 2
CS1=$(instance_names | sed -n 1p)
CS2=$(instance_names | sed -n 2p)
# Räume mit der Uhrzeit im Namen, damit ein zweiter Lauf nicht alte Nachrichten sieht.
ROOM_A="11111111-1111-1111-1111-$(printf '%012d' "$(date +%s)")"
ROOM_B="22222222-2222-2222-2222-$(printf '%012d' "$(date +%s)")"

start_client abnahme-s3-a1 "http://$CS1:8080/stream?roomId=$ROOM_A"
start_client abnahme-s3-a2 "http://$CS2:8080/stream?roomId=$ROOM_A"
start_client abnahme-s3-b "http://$CS1:8080/stream?roomId=$ROOM_B"
send_messages "$CS1 $ROOM_A s1 200 1;$CS2 $ROOM_A s2 200 1"
wait_for_events abnahme-s3-a1 400 60
wait_for_events abnahme-s3-a2 400 60

s3_status=OK
s3_measured=""
for client in abnahme-s3-a1 abnahme-s3-a2; do
  events=$(count_events "$client")
  distinct=$(docker logs "$client" 2>&1 | grep '^id:' | sort -u | wc -l | tr -d ' ')
  order=aufsteigend
  for sender in s1 s2; do
    contents_of "$client" "$sender" > "$WORK_DIR/$client-$sender.txt"
    if ! sort -n -c -u "$WORK_DIR/$client-$sender.txt" 2>/dev/null; then
      order=UNGEORDNET
    fi
  done
  s3_measured="$s3_measured ${client#abnahme-s3-}=$events/$distinct verschieden/$order;"
  if [ "$events" != "400" ] || [ "$distinct" != "400" ] || [ "$order" != "aufsteigend" ]; then
    s3_status=FEHLER
  fi
done
events_b=$(count_events abnahme-s3-b)
if [ "$events_b" != "0" ]; then
  s3_status=FEHLER
fi
record S3 "a1 und a2 je 400/400 verschieden/aufsteigend, b=0" "$s3_measured b=$events_b" "$s3_status"
docker rm -f abnahme-s3-a1 abnahme-s3-a2 abnahme-s3-b >/dev/null

# --- S4: zurück auf eine Instanz ---------------------------------------------

echo "=== S4: --scale chat-service=1"
bindings_before=$(count_delivery_bindings)
consumers_before=$(docker compose exec -T rabbitmq rabbitmqctl -q list_queues name consumers \
  | grep '^chat\.delivery\.' | awk '{print $2}' | tr '\n' ' ')
docker compose up -d --scale chat-service=1
bindings_after=""
for i in 1 2 3 4 5 6; do
  sleep 5
  bindings_after=$(count_delivery_bindings)
  echo "nach $((i * 5)) s: $bindings_after"
done
persist_and_dlq=$(docker compose exec -T rabbitmq rabbitmqctl -q list_queues name \
  | grep -cE '^chat\.(persist|dlq)[[:space:]]*$')
if [ "$bindings_before" = "2" ] && [ "$consumers_before" = "1 1 " ] \
   && [ "$bindings_after" = "1" ] && [ "$persist_and_dlq" = "2" ]; then
  s4_status=OK
else
  s4_status=FEHLER
fi
record S4 "vorher 2 Queues (Consumer 1 1), nach 30 s 1, persist+dlq 2" \
  "vorher $bindings_before (Consumer $consumers_before), nach 30 s $bindings_after, persist+dlq $persist_and_dlq" \
  "$s4_status"

# --- S5: ohne batch-writer und postgres --------------------------------------

echo "=== S5: batch-writer und postgres gestoppt, 100 Nachrichten"
CS1=$(instance_names | sed -n 1p)
ROOM_S5="55555555-5555-5555-5555-$(printf '%012d' "$(date +%s)")"
wait_for_empty_persist_queue
rows_before=$(count_rows)
docker compose stop batch-writer postgres
start_client abnahme-s5 "http://$CS1:8080/stream?roomId=$ROOM_S5"
send_messages "$CS1 $ROOM_S5 s5 100 1"
sleep 5
events_s5=$(count_events abnahme-s5)
docker compose start postgres batch-writer
docker compose up -d --wait postgres batch-writer >/dev/null 2>&1
rows_after="$rows_before"
for i in $(seq 1 45); do
  rows_after=$(count_rows)
  if [ -n "$rows_after" ] && [ $((rows_after - rows_before)) -ge 100 ]; then
    break
  fi
  sleep 2
done
rows_new=$((rows_after - rows_before))
if [ "$events_s5" = "100" ] && [ "$rows_new" = "100" ]; then
  s5_status=OK
else
  s5_status=FEHLER
fi
record S5 "100 Ereignisse nach 5 s, +100 Zeilen" "$events_s5 Ereignisse, +$rows_new Zeilen" "$s5_status"
docker rm -f abnahme-s5 >/dev/null

# --- S6: eine von zwei Instanzen angehalten ----------------------------------

echo "=== S6: Instanz 2 angehalten, 5000 Nachrichten"
docker compose up -d --scale chat-service=2 --wait
wait_for_bindings 2
CS1=$(instance_names | sed -n 1p)
CS2=$(instance_names | sed -n 2p)
ROOM_S6="66666666-6666-6666-6666-$(printf '%012d' "$(date +%s)")"
start_client abnahme-s6 "http://$CS1:8080/stream?roomId=$ROOM_S6"

# docker pause hält genau EINE Instanz an. docker compose pause chat-service
# würde beide anhalten (Spezifikation 6, S6).
docker pause "$CS2" >/dev/null

# Misst jede Sekunde die längste Instanz-Queue. Die Queue der laufenden
# Instanz bleibt nahe 0, das Maximum kommt also von der angehaltenen.
(
  max=0
  while [ ! -f "$WORK_DIR/s6-stop" ]; do
    n=$(docker compose exec -T rabbitmq rabbitmqctl -q list_queues name messages \
      | grep '^chat\.delivery\.' | awk '{print $2}' | sort -n | tail -1)
    n=${n:-0}
    if [ "$n" -gt "$max" ]; then
      max=$n
    fi
    echo "$max" > "$WORK_DIR/s6-max"
    sleep 1
  done
) &
monitor_pid=$!

send_messages "$CS1 $ROOM_S6 s6 5000 1"
wait_for_events abnahme-s6 5000 120
events_s6=$(count_events abnahme-s6)
sleep 3
touch "$WORK_DIR/s6-stop"
wait "$monitor_pid"
max_queue=$(cat "$WORK_DIR/s6-max")

docker unpause "$CS2" >/dev/null
wait_for_bindings 2
start_client abnahme-s6-cs2 "http://$CS2:8080/stream?roomId=$ROOM_S6"
send_messages "$CS1 $ROOM_S6 s6 1 5001"
wait_for_events abnahme-s6-cs2 1 30
events_cs2=$(count_events abnahme-s6-cs2)
if [ "$max_queue" -le 1000 ] && [ "$events_s6" = "5000" ] && [ "$events_cs2" -ge 1 ]; then
  s6_status=OK
else
  s6_status=FEHLER
fi
record S6 "Queue max <= 1000, Client CS1 5000, CS2 danach >= 1" \
  "Queue max $max_queue, Client CS1 $events_s6, CS2 danach $events_cs2" "$s6_status"
docker rm -f abnahme-s6 abnahme-s6-cs2 >/dev/null

# --- S7: Neuverbindung mit Last-Event-ID -------------------------------------

echo "=== S7: Trennung, 50 Nachrichten, Neuverbindung nach 10 s"
CS1=$(instance_names | sed -n 1p)
ROOM_S7="77777777-7777-7777-7777-$(printf '%012d' "$(date +%s)")"
start_client abnahme-s7-first "http://$CS1:8080/stream?roomId=$ROOM_S7"
send_messages "$CS1 $ROOM_S7 s7 1 0"
wait_for_events abnahme-s7-first 1 10
last_id=$(docker logs abnahme-s7-first 2>&1 | grep '^id:' | tail -1 | cut -d: -f2 | tr -d ' ')
docker rm -f abnahme-s7-first >/dev/null

send_messages "$CS1 $ROOM_S7 s7 50 1"
sleep 10
start_client abnahme-s7-again "http://$CS1:8080/stream?roomId=$ROOM_S7" "$last_id"
wait_for_events abnahme-s7-again 50 30
events_replayed=$(count_events abnahme-s7-again)
contents_of abnahme-s7-again s7 > "$WORK_DIR/s7.txt"
seq 1 50 > "$WORK_DIR/s7-expected.txt"
if diff -q "$WORK_DIR/s7-expected.txt" "$WORK_DIR/s7.txt" >/dev/null; then
  replay_order="genau 1..50"
else
  replay_order="NICHT 1..50"
fi
send_messages "$CS1 $ROOM_S7 s7 1 51"
wait_for_events abnahme-s7-again 51 10
events_total=$(count_events abnahme-s7-again)
last_content=$(contents_of abnahme-s7-again s7 | tail -1)
if [ "$events_replayed" = "50" ] && [ "$replay_order" = "genau 1..50" ] \
   && [ "$events_total" = "51" ] && [ "$last_content" = "51" ]; then
  s7_status=OK
else
  s7_status=FEHLER
fi
record S7 "50 nachgeliefert (1..50), danach live 51" \
  "$events_replayed nachgeliefert ($replay_order), danach $events_total, letztes $last_content" "$s7_status"
docker rm -f abnahme-s7-again >/dev/null

# --- S8: Regeln aus CLAUDE.md ------------------------------------------------

echo "=== S8: Quelltext von chat-service/"
stream_hits=$(grep -rnE "\.stream\(|Stream\.|Collectors" chat-service/src | wc -l | tr -d ' ')
env_in_repo=$(git ls-files .env | wc -l | tr -d ' ')
if [ "$stream_hits" = "0" ] && [ "$env_in_repo" = "0" ]; then
  s8_status=OK
else
  s8_status=FEHLER
  grep -rnE "\.stream\(|Stream\.|Collectors" chat-service/src
fi
record S8 "0 Streams, .env nicht im Repo" "$stream_hits Streams, .env $env_in_repo mal im Repo" "$s8_status"

# --- Zusammenfassung ---------------------------------------------------------

echo
echo "================ Ergebnis ================"
for line in "${RESULTS[@]}"; do
  echo "$line"
done
