#!/usr/bin/env bash

# Kafka
export KAFKA_BOOTSTRAP_SERVERS="此处自定义"

# MySQL realtime 结果库
export MYSQL_HOST="此处自定义"
export MYSQL_PORT="此处自定义"
export MYSQL_USER="此处自定义"
export MYSQL_PASSWORD="此处自定义"
export REALTIME_MYSQL_DATABASE="realtime"
export MYSQL_CHARSET="utf8"
export MYSQL_TIMEZONE="Asia/Shanghai"

# 必须是 Flink 集群可访问的持久化文件系统目录
export FLINK_CHECKPOINT_ROOT="此处自定义"
