#!/bin/bash
set -e

CONTAINER_NAME="my-postgis-container"

DB_USER="postgres"
DB_NAME="postgres"
TARGET_TABLE="performance_wa"  # 写入缓冲区 - GeoMesa架构入口点
DB_PASSWD="ds123456"

TBL_DIR_IN_LOCAL="/data6/zhangdw/datasets/beijingshi_tbl_100k"

FIL_NAME="merged_0.tbl"

gzip -k "${TBL_DIR_IN_LOCAL}/${FIL_NAME}"

time (gunzip -c "${TBL_DIR_IN_LOCAL}/${FIL_NAME}.gz" | docker exec -i "${CONTAINER_NAME}" \
  psql -U postgres -e \
  "COPY ${TARGET_TABLE}(fid,geom,dtg,taxi_id) FROM STDIN WITH (FORMAT text, DELIMITER '|', NULL '');")