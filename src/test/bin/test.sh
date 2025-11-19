#!/bin/bash
set -e

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)

CONTAINER_NAME="my-postgis-container"

DB_USER="postgres"
DB_NAME="postgres"
TARGET_TABLE="performance_wa"  # 写入缓冲区 - GeoMesa架构入口点
DB_PASSWD="ds123456"

TBL_DIR_IN_LOCAL="/data6/zhangdw/datasets/beijingshi_tbl_100k"

FIL_NAME="merged_0.tbl"

bash "${SCRIPT_DIR}/disable_geomesa_features.sh"

docker exec -i "${CONTAINER_NAME}" psql -U "${DB_USER}" -d "${DB_NAME}" -c "DELETE FROM performance_wa;"

time (cat "${TBL_DIR_IN_LOCAL}/${FIL_NAME}" | docker exec -i "${CONTAINER_NAME}" \
  psql -U "${DB_USER}" -d "${DB_NAME}" \
  -c "COPY ${TARGET_TABLE}(fid,geom,dtg,taxi_id) FROM STDIN WITH (FORMAT text, DELIMITER '|', NULL '');")

docker exec -i "${CONTAINER_NAME}" psql -U "${DB_USER}" -d "${DB_NAME}" -c "SELECT count(1) FROM performance;"

bash "${SCRIPT_DIR}/enable_geomesa_features.sh"