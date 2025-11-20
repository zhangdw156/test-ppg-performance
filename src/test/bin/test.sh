#!/bin/bash
set -e  # 任何命令失败立即退出
set -o pipefail # 管道中的任何命令失败也视为失败

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)

# --- 配置 ---
CONTAINER_NAME="my-postgis-container"
DB_USER="postgres"
DB_NAME="postgres"
# DB_PASSWD="ds123456" # 如果需要密码，需要在psql命令中使用PGPASSWORD环境变量
TARGET_TABLE_BASE="performance" # 使用基础名称
TBL_DIR_IN_LOCAL="/data6/zhangdw/datasets/beijingshi_tbl_100k"
FIL_NAME="merged_0.tbl"
FILE_PATH="${TBL_DIR_IN_LOCAL}/${FIL_NAME}"

# --- 检查文件是否存在 ---
if [[ ! -f "$FILE_PATH" ]]; then
    echo "错误: 数据文件未找到: $FILE_PATH"
    exit 1
fi

# ==============================================================================
# 步骤 1: 连接数据库，获取当前活动的、格式化好的分区表名
# ==============================================================================
echo ">>> 步骤 1: 正在获取当前活动的分区表名..."
# SQL 查询直接让数据库拼接好表名返回
GET_PARTITION_SQL="SELECT '\"${TARGET_TABLE_BASE}_wa_' || lpad(value::text, 3, '0') || '\"' FROM \"public\".\"geomesa_wa_seq\" WHERE type_name = '${TARGET_TABLE_BASE}'"

# 使用 command substitution `$(...)` 来捕获psql的输出
# -tA 参数让输出非常干净，没有表头和空格，便于程序读取
PARTITION_NAME=$(docker exec "${CONTAINER_NAME}" \
  psql -U "${DB_USER}" -d "${DB_NAME}" -tA -c "${GET_PARTITION_SQL}")

# 健壮性检查：如果未能获取到分区名，则报错退出
if [[ -z "$PARTITION_NAME" ]]; then
    echo "错误: 未能从 geomesa_wa_seq 获取到分区名 (type_name='${TARGET_TABLE_BASE}')"
    exit 1
fi

echo "成功获取分区表名: ${PARTITION_NAME}"

# ==============================================================================
# 步骤 2: 构建并执行包含完整事务的最终加载命令
# ==============================================================================
echo ">>> 步骤 2: 开始执行数据导入事务..."

# 定义要锁定的主分区表名
LOCK_TABLE_NAME="\"public\".\"${TARGET_TABLE_BASE}_wa\""

# 注意：我们不再使用 -c 参数。
# 我们将通过管道把一个完整的指令流（SQL命令 + 数据）传递给 psql。
time ( \
    ( \
        echo "BEGIN;"; \
        echo "LOCK TABLE ${LOCK_TABLE_NAME} IN SHARE UPDATE EXCLUSIVE MODE;"; \
        # 在这里使用我们上一步获取到的 shell 变量 PARTITION_NAME
        echo "COPY public.${PARTITION_NAME}(fid,geom,dtg,taxi_id) FROM STDIN WITH (FORMAT text, DELIMITER '|', NULL '');"; \
        # cat 命令将文件内容注入到数据流中
        cat "${FILE_PATH}"; \
        # \. 是 COPY FROM STDIN 的结束标记
        echo "\."; \
        echo "COMMIT;"; \
    ) | docker exec -i "${CONTAINER_NAME}" \
        psql -U "${DB_USER}" -d "${DB_NAME}" -q -v ON_ERROR_STOP=1 \
)

echo ">>> 数据导入成功！"