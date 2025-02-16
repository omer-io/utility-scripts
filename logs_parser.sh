#!/bin/bash

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <log_file> <tab_title>"
    exit 1
fi

LOG_FILE="$1"
TAB_TITLE="$2"
OUTPUT_FILE="solana_logs.csv"

# Extract headers
HEADERS=$(grep banking_stage_scheduler_reception_counts "$LOG_FILE" | \
    awk -F "banking_stage_scheduler_reception_counts" '{print $2}' | \
    awk -F"=" '{for (i=1; i<=NF; i++) {gsub(/[0-9]+/, "", $i); printf "%s,", $i} printf "\n"}' | \
    sed 's/\<i\>//g' | sed 's/  *, */,/g' | sed 's/,$//' | head -n1)

HEADERS=",$HEADERS"

# Extract values and replace spaces with commas
VALUES=$(paste -d',' <(
    grep banking_stage_scheduler_reception_counts "$LOG_FILE" | awk '{print $1}' | \
    awk -F'T' '{split($2, arr, ":"); print arr[2] ":" arr[3]}' | sed 's/Z//g'
) <(
    grep banking_stage_scheduler_reception_counts "$LOG_FILE" | \
    awk -F banking_stage_scheduler_reception_counts '{print $2}' | \
    awk -F"=" '{for (i=2; i<=NF; i++) {gsub(/[^0-9]+/, "", $i); printf "%s,", $i} printf "\n"}' | \
    sed 's/,$//g'
))

# Compute column sums, replace spaces with commas, and remove trailing comma
SUMS=$(echo "$VALUES" | awk -F',' '{for (i=2; i<=NF; i++) sum[i]+=$i} END {printf "0,"; for (i=2; i<=NF; i++) printf "%s,", sum[i]; printf "\n"}' | sed 's/,$//')

# Write sums, headers, and values to CSV
echo "banking_stage_scheduler_reception_counts" > "$OUTPUT_FILE"
echo "$SUMS" >> "$OUTPUT_FILE"
echo "$HEADERS" >> "$OUTPUT_FILE"
echo "$VALUES" >> "$OUTPUT_FILE"

echo -e "\n" >> "$OUTPUT_FILE"

HEADERS=$(grep banking_stage_scheduler_reception_slot_counts "$LOG_FILE" | \
    awk -F "banking_stage_scheduler_reception_slot_counts" '{print $2}' | \
    awk -F"=" '{for (i=1; i<=NF; i++) {gsub(/[0-9]+/, "", $i); printf "%s,", $i} printf "\n"}' | \
    sed 's/\<i\>//g' | sed 's/  *, */,/g' | sed 's/,$//' | head -n1)

HEADERS=",$HEADERS"

# Extract and append banking_stage_scheduler_slot_counts values
SLOT_VALUES=$(paste -d',' <(
    grep banking_stage_scheduler_reception_slot_counts "$LOG_FILE" | awk '{print $1}' | \
    awk -F'T' '{split($2, arr, ":"); print arr[2] ":" arr[3]}' | sed 's/Z//g'
) <(
    grep banking_stage_scheduler_reception_slot_counts "$LOG_FILE" | \
    awk -F "banking_stage_scheduler_reception_slot_counts" '{print $2}' | \
    awk -F"=" '{for (i=2; i<=NF; i++) {gsub(/[^0-9]+/, "", $i); printf "%s,", $i} printf "\n"}' | sed 's/,$//'
))

# Compute column sums for slot values
SLOT_SUMS=$(echo "$SLOT_VALUES" | awk -F',' '{for (i=2; i<NF; i++) sum[i]+=$i} END {printf "0,"; for (i=2; i<=NF; i++) printf "%s,", sum[i]; printf "\n"}' | sed 's/,$//')

echo "banking_stage_scheduler_reception_slot_counts" >> "$OUTPUT_FILE"
echo "$SLOT_SUMS" >> "$OUTPUT_FILE"
echo "$HEADERS" >> "$OUTPUT_FILE"
echo "$SLOT_VALUES" >> "$OUTPUT_FILE"

echo -e "\n" >> "$OUTPUT_FILE"

#### Extract banking_stage_scheduler_counts ####
HEADERS_SCHEDULER=$(grep banking_stage_scheduler_counts "$LOG_FILE" | \
    awk -F "banking_stage_scheduler_counts" '{print $2}' | \
    awk -F"=" '{for (i=1; i<=NF; i++) {gsub(/[0-9]+/, "", $i); printf "%s,", $i} printf "\n"}' | \
    sed 's/\<i\>//g' | sed 's/  *, */,/g' | sed 's/,$//' | head -n1)

HEADERS_SCHEDULER=",$HEADERS_SCHEDULER"

VALUES_SCHEDULER=$(paste -d',' <(
    grep banking_stage_scheduler_counts "$LOG_FILE" | awk '{print $1}' | \
    awk -F'T' '{split($2, arr, ":"); print arr[2] ":" arr[3]}' | sed 's/Z//g'
) <(
    grep banking_stage_scheduler_counts "$LOG_FILE" | \
    awk -F banking_stage_scheduler_counts '{print $2}' | \
    awk -F"=" '{for (i=2; i<=NF; i++) {gsub(/[^0-9]+/, "", $i); printf "%s,", $i} printf "\n"}' | \
    sed 's/,$//g'
))

SUMS_SCHEDULER=$(echo "$VALUES_SCHEDULER" | awk -F',' '{for (i=2; i<=NF; i++) sum[i]+=$i} END {printf "0,"; for (i=2; i<=NF; i++) printf "%s,", sum[i]; printf "\n"}' | sed 's/,$//')

echo "banking_stage_scheduler_counts" >> "$OUTPUT_FILE"
echo "$SUMS_SCHEDULER" >> "$OUTPUT_FILE"
echo "$HEADERS_SCHEDULER" >> "$OUTPUT_FILE"
echo "$VALUES_SCHEDULER" >> "$OUTPUT_FILE"

echo -e "\n" >> "$OUTPUT_FILE"

#### Extract banking_stage_scheduler_slot_counts ####
HEADERS_SCHEDULER_SLOT=$(grep banking_stage_scheduler_slot_counts "$LOG_FILE" | \
    awk -F "banking_stage_scheduler_slot_counts" '{print $2}' | \
    awk -F"=" '{for (i=1; i<=NF; i++) {gsub(/[0-9]+/, "", $i); printf "%s,", $i} printf "\n"}' | \
    sed 's/\<i\>//g' | sed 's/  *, */,/g' | sed 's/,$//' | head -n1)

HEADERS_SCHEDULER_SLOT=",$HEADERS_SCHEDULER_SLOT"

SLOT_VALUES_SCHEDULER=$(paste -d',' <(
    grep banking_stage_scheduler_slot_counts "$LOG_FILE" | awk '{print $1}' | \
    awk -F'T' '{split($2, arr, ":"); print arr[2] ":" arr[3]}' | sed 's/Z//g'
) <(
    grep banking_stage_scheduler_slot_counts "$LOG_FILE" | \
    awk -F "banking_stage_scheduler_slot_counts" '{print $2}' | \
    awk -F"=" '{for (i=2; i<=NF; i++) {gsub(/[^0-9]+/, "", $i); printf "%s,", $i} printf "\n"}' | sed 's/,$//'
))

SLOT_SUMS_SCHEDULER=$(echo "$SLOT_VALUES_SCHEDULER" | awk -F',' '{for (i=2; i<NF; i++) sum[i]+=$i} END {printf "0,"; for (i=2; i<NF; i++) printf "%s,", sum[i]; printf "\n"}' | sed 's/,$//')

echo "banking_stage_scheduler_slot_counts" >> "$OUTPUT_FILE"
echo "$SLOT_SUMS_SCHEDULER" >> "$OUTPUT_FILE"
echo "$HEADERS_SCHEDULER_SLOT" >> "$OUTPUT_FILE"
echo "$SLOT_VALUES_SCHEDULER" >> "$OUTPUT_FILE"

echo -e "\n" >> "$OUTPUT_FILE"

HEADERS_WORKER_ERROR=$(grep banking_stage_worker_error_metrics "$LOG_FILE" | \
    awk -F "banking_stage_worker_error_metrics" '{print $2}' | \
    awk -F"=" '{for (i=1; i<=NF; i++) {gsub(/[0-9]+/, "", $i); printf "%s,", $i} printf "\n"}' | \
    sed 's/\<i\>//g' | sed 's/  *, */,/g' | sed 's/,$//' | head -n1)

VALUES_WORKER_ERROR=$(paste -d',' <(
    grep banking_stage_worker_error_metrics "$LOG_FILE" | awk '{print $1}' | \
    awk -F'T' '{split($2, arr, ":"); print arr[2] ":" arr[3]}' | sed 's/Z//g'
) <(
    grep banking_stage_worker_error_metrics "$LOG_FILE" | \
    awk -F banking_stage_worker_error_metrics '{print $2}' | \
    awk -F"=" '{for (i=2; i<=NF; i++) {gsub(/[^0-9]+/, "", $i); printf "%s,", $i} printf "\n"}' | \
    sed 's/,$//g'
))

SUMS_WORKER_ERROR=$(echo "$VALUES_WORKER_ERROR" | awk -F',' '{for (i=2; i<NF; i++) sum[i]+=$i} END {printf "0,"; for (i=2; i<NF; i++) printf "%s,", sum[i]; printf "\n"}' | sed 's/,$//')

echo "banking_stage_worker_error_metrics" >> "$OUTPUT_FILE"
echo "$SUMS_WORKER_ERROR" >> "$OUTPUT_FILE"
echo "$HEADERS_WORKER_ERROR" >> "$OUTPUT_FILE"
echo "$VALUES_WORKER_ERROR" >> "$OUTPUT_FILE"

echo -e "\n" >> "$OUTPUT_FILE"

HEADERS_WORKER=$(grep banking_stage_worker_counts "$LOG_FILE" | \
    awk -F "banking_stage_worker_counts" '{print $2}' | \
    awk -F"=" '{for (i=1; i<=NF; i++) {gsub(/[0-9]+/, "", $i); printf "%s,", $i} printf "\n"}' | \
    sed 's/\<i\>//g' | sed 's/  *, */,/g' | sed 's/,$//' | head -n1)

VALUES_WORKER=$(paste -d',' <(
    grep banking_stage_worker_counts "$LOG_FILE" | awk '{print $1}' | \
    awk -F'T' '{split($2, arr, ":"); print arr[2] ":" arr[3]}' | sed 's/Z//g'
) <(
    grep banking_stage_worker_counts "$LOG_FILE" | \
    awk -F banking_stage_worker_counts '{print $2}' | \
    awk -F"=" '{for (i=2; i<=NF; i++) {gsub(/[^0-9]+/, "", $i); printf "%s,", $i} printf "\n"}' | \
    sed 's/,$//g'
))

SUMS_WORKER=$(echo "$VALUES_WORKER" | awk -F',' '{for (i=2; i<NF; i++) sum[i]+=$i} END {printf "0,"; for (i=2; i<NF; i++) printf "%s,", sum[i]; printf "\n"}' | sed 's/,$//')

echo "banking_stage_worker_counts" >> "$OUTPUT_FILE"
echo "$SUMS_WORKER" >> "$OUTPUT_FILE"
echo "$HEADERS_WORKER" >> "$OUTPUT_FILE"
echo "$VALUES_WORKER" >> "$OUTPUT_FILE"

echo -e "\n" >> "$OUTPUT_FILE"

HEADERS_LEADER_DETECTION=$(grep banking_stage_scheduler_leader_detection "$LOG_FILE" | \
    awk -F "banking_stage_scheduler_leader_detection" '{print $2}' | \
    awk -F"=" '{for (i=1; i<=NF; i++) {gsub(/[0-9]+/, "", $i); printf "%s,", $i} printf "\n"}' | \
    sed 's/\<i\>//g' | sed 's/  *, */,/g' | sed 's/,$//' | head -n1)
HEADERS_LEADER_DETECTION=",$HEADERS_LEADER_DETECTION"
LEADER_DETECTION=$(paste -d',' <(
    grep banking_stage_scheduler_leader_detection "$LOG_FILE" | awk '{print $1}' | \
    awk -F'T' '{split($2, arr, ":"); print arr[2] ":" arr[3]}' | sed 's/Z//g'
) <(
    grep banking_stage_scheduler_leader_detection "$LOG_FILE" | \
    awk -F "banking_stage_scheduler_leader_detection" '{print $2}' | \
    awk -F"=" '{for (i=2; i<=NF; i++) {gsub(/[^0-9]+/, "", $i); printf "%s,", $i} printf "\n"}' | \
    sed 's/,$//g'
))

echo "banking_stage_scheduler_leader_detection" >> "$OUTPUT_FILE"
echo "$HEADERS_LEADER_DETECTION" >> "$OUTPUT_FILE"
echo "$LEADER_DETECTION" >> "$OUTPUT_FILE"

echo -e "\n" >> "$OUTPUT_FILE"

HEADERS_SCHEDULER_TIMING=$(grep banking_stage_scheduler_timing "$LOG_FILE" | \
    awk -F "banking_stage_scheduler_timing" '{print $2}' | \
    awk -F"=" '{for (i=1; i<=NF; i++) {gsub(/[0-9]+/, "", $i); printf "%s,", $i} printf "\n"}' | \
    sed 's/\<i\>//g' | sed 's/  *, */,/g' | sed 's/,$//' | head -n1)
HEADERS_SCHEDULER_TIMING=",$HEADERS_SCHEDULER_TIMING"
SCHEDULER_TIMING=$(paste -d',' <(
    grep banking_stage_scheduler_timing "$LOG_FILE" | awk '{print $1}' | \
    awk -F'T' '{split($2, arr, ":"); print arr[2] ":" arr[3]}' | sed 's/Z//g'
) <(
    grep banking_stage_scheduler_timing "$LOG_FILE" | \
    awk -F "banking_stage_scheduler_timing" '{print $2}' | \
    awk -F"=" '{for (i=2; i<=NF; i++) {gsub(/[^0-9]+/, "", $i); printf "%s,", $i} printf "\n"}' | \
    sed 's/,$//g'
))

echo "banking_stage_scheduler_timing" >> "$OUTPUT_FILE"
echo "$HEADERS_SCHEDULER_TIMING" >> "$OUTPUT_FILE"
echo "$SCHEDULER_TIMING" >> "$OUTPUT_FILE"

echo -e "\n" >> "$OUTPUT_FILE"

HEADERS_WORKER_TIMING=$(grep banking_stage_worker_timing "$LOG_FILE" | \
    awk -F "banking_stage_worker_timing" '{print $2}' | \
    awk -F"=" '{for (i=1; i<=NF; i++) {gsub(/[0-9]+/, "", $i); printf "%s,", $i} printf "\n"}' | \
    sed 's/\<i\>//g' | sed 's/  *, */,/g' | sed 's/,$//' | head -n1)

WORKER_TIMING=$(paste -d',' <(
    grep banking_stage_worker_timing "$LOG_FILE" | awk '{print $1}' | \
    awk -F'T' '{split($2, arr, ":"); print arr[2] ":" arr[3]}' | sed 's/Z//g'
) <(
    grep banking_stage_worker_timing "$LOG_FILE" | \
    awk -F "banking_stage_worker_timing" '{print $2}' | \
    awk -F"=" '{for (i=2; i<=NF; i++) {gsub(/[^0-9]+/, "", $i); printf "%s,", $i} printf "\n"}'| \
    sed 's/,$//g'
))

echo "banking_stage_worker_timing" >> "$OUTPUT_FILE"
echo "$HEADERS_WORKER_TIMING" >> "$OUTPUT_FILE"
echo "$WORKER_TIMING" >> "$OUTPUT_FILE"

echo -e "\n" >> "$OUTPUT_FILE"

HEADERS_LEADER_SLOT_PACKET_COUNT=$(grep banking_stage-leader_slot_packet_counts "$LOG_FILE" | \
    awk -F "banking_stage-leader_slot_packet_counts" '{print $2}' | \
    awk -F"=" '{for (i=1; i<=NF; i++) {gsub(/[0-9]+/, "", $i); printf "%s,", $i} printf "\n"}' | \
    sed 's/\<i\>//g' | sed 's/  *, */,/g' | sed 's/,$//' | head -n1)

LEADER_SLOT_PACKET_COUNT=$(paste -d',' <(
    grep banking_stage-leader_slot_packet_counts "$LOG_FILE" | awk '{print $1}' | \
    awk -F'T' '{split($2, arr, ":"); print arr[2] ":" arr[3]}' | sed 's/Z//g'
) <(
    grep banking_stage-leader_slot_packet_counts "$LOG_FILE" | \
    awk -F "banking_stage-leader_slot_packet_counts" '{print $2}' | \
    awk -F"=" '{for (i=2; i<=NF; i++) {gsub(/[^0-9]+/, "", $i); printf "%s,", $i} printf "\n"}' | \
    sed 's/,$//g'
))

SUMS_LEADER_SLOT_PACKET_COUNT=$(echo "$LEADER_SLOT_PACKET_COUNT" | awk -F',' '{for (i=2; i<=NF; i++) sum[i]+=$i} END {printf "0,"; for (i=2; i<=NF; i++) printf "%s,", sum[i]; printf "\n"}' | sed 's/,$//')

echo "banking_stage-leader_slot_packet_counts" >> "$OUTPUT_FILE"
echo "$SUMS_LEADER_SLOT_PACKET_COUNT" >> "$OUTPUT_FILE"
echo "$HEADERS_LEADER_SLOT_PACKET_COUNT" >> "$OUTPUT_FILE"
echo "$LEADER_SLOT_PACKET_COUNT" >> "$OUTPUT_FILE"

echo -e "\n" >> "$OUTPUT_FILE"

HEADERS_LEADER_SLOT_TRANSACTION_ERROR=$(grep banking_stage-leader_slot_transaction_errors "$LOG_FILE" | \
    awk -F "banking_stage-leader_slot_transaction_errors" '{print $2}' | \
    awk -F"=" '{for (i=1; i<=NF; i++) {gsub(/[0-9]+/, "", $i); printf "%s,", $i} printf "\n"}' | \
    sed 's/\<i\>//g' | sed 's/  *, */,/g' | sed 's/,$//' | head -n1)

LEADER_SLOT_TRANSACTION_ERROR=$(paste -d',' <(
    grep banking_stage-leader_slot_transaction_errors "$LOG_FILE" | awk '{print $1}' | \
    awk -F'T' '{split($2, arr, ":"); print arr[2] ":" arr[3]}' | sed 's/Z//g'
) <(
    grep banking_stage-leader_slot_transaction_errors "$LOG_FILE" | \
    awk -F "banking_stage-leader_slot_transaction_errors" '{print $2}' | \
    awk -F"=" '{for (i=2; i<=NF; i++) {gsub(/[^0-9]+/, "", $i); printf "%s,", $i} printf "\n"}' | \
    sed 's/,$//g'
))

SUMS_LEADER_SLOT_TRANSACTION_ERROR=$(echo "$LEADER_SLOT_TRANSACTION_ERROR" | awk -F',' '{for (i=2; i<=NF; i++) sum[i]+=$i} END {printf "0,"; for (i=2; i<=NF; i++) printf "%s,", sum[i]; printf "\n"}' | sed 's/,$//')

echo "banking_stage-leader_slot_transaction_errors" >> "$OUTPUT_FILE"
echo "$SUMS_LEADER_SLOT_TRANSACTION_ERROR" >> "$OUTPUT_FILE"
echo "$HEADERS_LEADER_SLOT_TRANSACTION_ERROR" >> "$OUTPUT_FILE"
echo "$LEADER_SLOT_TRANSACTION_ERROR" >> "$OUTPUT_FILE"

echo -e "\n" >> "$OUTPUT_FILE"

HEADERS_POH_SERVICE=$(grep poh-service "$LOG_FILE" | \
    awk -F "poh-service" '{print $2}' | \
    awk -F"=" '{for (i=1; i<=NF; i++) {gsub(/[0-9]+/, "", $i); printf "%s,", $i} printf "\n"}' | \
    sed 's/\<i\>//g' | sed 's/  *, */,/g' | sed 's/,$//' | head -n1)
HEADERS_POH_SERVICE=",$HEADERS_POH_SERVICE"
VALUES_POH_SERVICE=$(paste -d',' <(
    grep poh-service "$LOG_FILE" | awk '{print $1}' | \
    awk -F'T' '{split($2, arr, ":"); print arr[2] ":" arr[3]}' | sed 's/Z//g'
) <(
    grep poh-service "$LOG_FILE" | \
    awk -F "poh-service" '{print $2}' | \
    awk -F"=" '{for (i=2; i<=NF; i++) {gsub(/[^0-9]+/, "", $i); printf "%s,", $i} printf "\n"}' | \
    sed 's/,$//g'
))

echo "poh-service" >> "$OUTPUT_FILE"
echo "$HEADERS_POH_SERVICE" >> "$OUTPUT_FILE"
echo "$VALUES_POH_SERVICE" >> "$OUTPUT_FILE"

echo -e "\n" >> "$OUTPUT_FILE"

HEADERS_POH_RECORDER=$(grep tick_lock_contention "$LOG_FILE" | \
    awk -F "poh_recorder" '{print $2}' | \
    awk -F"=" '{for (i=1; i<=NF; i++) {gsub(/[0-9]+/, "", $i); printf "%s,", $i} printf "\n"}' | \
    sed 's/\<i\>//g' | sed 's/  *, */,/g' | sed 's/,$//' | head -n1)
HEADERS_POH_RECORDER=",$HEADERS_POH_RECORDER"
VALUES_POH_RECORDER=$(paste -d',' <(
    grep tick_lock_contention "$LOG_FILE" | awk '{print $1}' | \
    awk -F'T' '{split($2, arr, ":"); print arr[2] ":" arr[3]}' | sed 's/Z//g'
) <(
    grep tick_lock_contention "$LOG_FILE" | \
    awk -F "poh_recorder" '{print $2}' | \
    awk -F"=" '{for (i=2; i<=NF; i++) {gsub(/[^0-9]+/, "", $i); printf "%s,", $i} printf "\n"}' | \
    sed 's/,$//g'
))

echo "poh-recorder" >> "$OUTPUT_FILE"
echo "$HEADERS_POH_RECORDER" >> "$OUTPUT_FILE"
echo "$VALUES_POH_RECORDER" >> "$OUTPUT_FILE"

echo -e "\n" >> "$OUTPUT_FILE"

# HEADERS_REPLAY_SLOTS_STATS=$(grep replay-slot-stats "$LOG_FILE" | \
#     awk -F "replay-slot-stats" '{print $2}' | \
#     awk -F"=" '{for (i=1; i<=NF; i++) {gsub(/[0-9]+/, "", $i); printf "%s,", $i} printf "\n"}' | \
#     sed 's/\<i\>//g' | sed 's/  *, */,/g' | sed 's/,$//' | head -n1)
# HEADERS_REPLAY_SLOTS_STATS=",$HEADERS_REPLAY_SLOTS_STATS"
# VALUES_REPLAY_SLOTS_STATS=$(paste -d',' <(
#     grep replay-slot-stats "$LOG_FILE" | awk '{print $1}' | \
#     awk -F'T' '{split($2, arr, ":"); print arr[2] ":" arr[3]}' | sed 's/Z//g'
# ) <(
#     grep replay-slot-stats "$LOG_FILE" | \
#     awk -F "replay-slot-stats" '{print $2}' | \
#     awk -F"=" '{for (i=2; i<=NF; i++) {gsub(/[^0-9]+/, "", $i); printf "%s,", $i} printf "\n"}' | \
#     sed 's/,$//g'
# ))

# echo "replay-slot-stats" >> "$OUTPUT_FILE"
# echo "$HEADERS_REPLAY_SLOTS_STATS" >> "$OUTPUT_FILE"
# echo "$VALUES_REPLAY_SLOTS_STATS" >> "$OUTPUT_FILE"

# echo -e "\n" >> "$OUTPUT_FILE"

# Notify user
echo "CSV file generated: $OUTPUT_FILE"

# Upload CSV to Google Sheets
python3 upload_logs.py "$OUTPUT_FILE" "$TAB_TITLE"