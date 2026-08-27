#!/usr/bin/env bash
set -euo pipefail

# C08 transaction-event topics. This script creates metadata only.
KAFKA_TOPICS_BIN="${KAFKA_TOPICS_BIN:-${KAFKA_HOME:-此处自定义}/bin/kafka-topics.sh}"
KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-此处自定义}"
KAFKA_REPLICATION_FACTOR="${KAFKA_REPLICATION_FACTOR:-2}"

if [[ ! -x "${KAFKA_TOPICS_BIN}" ]]; then
  echo "kafka-topics.sh is not executable: ${KAFKA_TOPICS_BIN}" >&2
  exit 1
fi

create_topic() {
  local topic="$1"

  "${KAFKA_TOPICS_BIN}" \
    --bootstrap-server "${KAFKA_BOOTSTRAP_SERVERS}" \
    --create \
    --if-not-exists \
    --topic "${topic}" \
    --partitions 3 \
    --replication-factor "${KAFKA_REPLICATION_FACTOR}" \
    --config "retention.ms=604800000" \
    --config "cleanup.policy=delete"
}

# The daily-facts producer publishes only these three transaction event types.
create_topic "ods_order_info"
create_topic "ods_order_detail"
create_topic "ods_payment_info"

echo "C08 Kafka topics are present on ${KAFKA_BOOTSTRAP_SERVERS}."
