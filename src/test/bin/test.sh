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
# 步骤 0: (预清理) 在导入前清理 performance 视图/表中的数据
# ==============================================================================
echo ">>> 步骤 0: [预清理] 正在清理 ${TARGET_TABLE_BASE} 中的历史数据..."
docker exec "${CONTAINER_NAME}" \
  psql -U "${DB_USER}" -d "${DB_NAME}" \
  -c "DELETE FROM \"public\".\"${TARGET_TABLE_BASE}\";"

echo "预清理完成。"

# ==============================================================================
# 步骤 1: 连接数据库，获取当前活动的、格式化好的分区表名
# ==============================================================================
echo ">>> 步骤 1: 正在获取当前活动的分区表名..."
GET_PARTITION_SQL="SELECT '\"${TARGET_TABLE_BASE}_wa_' || lpad(value::text, 3, '0') || '\"' FROM \"public\".\"geomesa_wa_seq\" WHERE type_name = '${TARGET_TABLE_BASE}'"

PARTITION_NAME=$(docker exec "${CONTAINER_NAME}" \
  psql -U "${DB_USER}" -d "${DB_NAME}" -tA -c "${GET_PARTITION_SQL}")

if [[ -z "$PARTITION_NAME" ]]; then
    echo "错误: 未能从 geomesa_wa_seq 获取到分区名 (type_name='${TARGET_TABLE_BASE}')"
    exit 1
fi

echo "成功获取分区表名: ${PARTITION_NAME}"

# ==============================================================================
# 步骤 1.5 (新增): 删除分区表上除主键外的所有索引
# ==============================================================================
echo ">>> 步骤 1.5: [优化] 正在删除 ${PARTITION_NAME} 上除主键外的索引以加速导入..."

# 1. 去除 PARTITION_NAME 中的双引号，用于查询系统表 (例如 "performance_wa_000" -> performance_wa_000)
PARTITION_NAME_PURE=$(echo "${PARTITION_NAME}" | tr -d '"')

# 2. 查询该表下的非主键索引名称
# 逻辑：查询 pg_indexes，过滤掉 indexdef 包含 "PRIMARY KEY" 的记录
GET_INDEXES_SQL="SELECT indexname FROM pg_indexes WHERE schemaname = 'public' AND tablename = '${PARTITION_NAME_PURE}' AND indexdef NOT LIKE '%PRIMARY KEY%';"

INDEXES_TO_DROP=$(docker exec "${CONTAINER_NAME}" \
  psql -U "${DB_USER}" -d "${DB_NAME}" -tA -c "${GET_INDEXES_SQL}")

# 3. 循环删除索引
if [[ -n "$INDEXES_TO_DROP" ]]; then
    echo "发现以下辅助索引，准备删除:"
    echo "${INDEXES_TO_DROP}"

    # 设置 IFS 为换行符，防止索引名中有空格导致解析错误（虽然这里通常没有）
    IFS=$'\n'
    for IDX in $INDEXES_TO_DROP; do
        echo " -> Dropping index: ${IDX} ..."
        docker exec "${CONTAINER_NAME}" \
          psql -U "${DB_USER}" -d "${DB_NAME}" \
          -c "DROP INDEX IF EXISTS \"public\".\"${IDX}\";"
    done
    unset IFS
    echo "辅助索引删除完毕。"
else
    echo "未发现需要删除的辅助索引 (可能已经被删除或仅剩主键)。"
fi

# ==============================================================================
# 步骤 2: 构建并执行包含完整事务的最终加载命令
# ==============================================================================
echo ">>> 步骤 2: 开始执行数据导入事务..."

LOCK_TABLE_NAME="\"public\".\"${TARGET_TABLE_BASE}_wa\""

time ( \
    ( \
        echo "BEGIN;"; \
        echo "LOCK TABLE ${LOCK_TABLE_NAME} IN SHARE UPDATE EXCLUSIVE MODE;"; \
        echo "COPY public.${PARTITION_NAME}(fid,geom,dtg,taxi_id) FROM STDIN WITH (FORMAT text, DELIMITER '|', NULL '');"; \

        # 1. 输出文件内容
        cat "${FILE_PATH}"; \

        # 2. 【关键修改】智能补全换行符
        # tail -c 1 读取最后一个字节。
        # 如果最后是换行符，Shell的命令替换 $(...) 会自动把结尾的换行符去掉，导致结果为空，条件为假 -> 不 echo。
        # 如果最后不是换行符（是数据），结果不为空，条件为真 -> echo 一个换行符。
        if [ -n "$(tail -c 1 "${FILE_PATH}")" ]; then
            echo ""
        fi

        # 3. 输出结束标记
        echo "\."; \
        echo "COMMIT;"; \
    ) | docker exec -i "${CONTAINER_NAME}" \
        psql -U "${DB_USER}" -d "${DB_NAME}" -q -v ON_ERROR_STOP=1 \
)

echo ">>> 数据导入成功！"

# ==============================================================================
# 步骤 3: 验证 performance 视图中的最终数据量
# ==============================================================================
echo ">>> 步骤 3: [验证] 正在统计 ${TARGET_TABLE_BASE} 中的数据总量..."

FINAL_COUNT=$(docker exec "${CONTAINER_NAME}" \
  psql -U "${DB_USER}" -d "${DB_NAME}" -tA \
  -c "SELECT count(1) FROM \"public\".\"${TARGET_TABLE_BASE}\";")

echo "=============================================="
echo "   ${TARGET_TABLE_BASE} 当前记录数: ${FINAL_COUNT}"
echo "=============================================="

# ==============================================================================
# 步骤 4: (后清理) 再次清除 performance 里的数据
# ==============================================================================
echo ">>> 步骤 4: [后清理] 正在清除 ${TARGET_TABLE_BASE} 中的数据以释放空间/重置环境..."

time docker exec "${CONTAINER_NAME}" \
  psql -U "${DB_USER}" -d "${DB_NAME}" \
  -c "DELETE FROM \"public\".\"${TARGET_TABLE_BASE}\";"

echo "=============================================="
echo "   数据已全部清除。"
echo "=============================================="