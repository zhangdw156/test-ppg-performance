#!/bin/bash
# ==============================================================================
# Shell脚本：全量数据导入与性能测试 (v4 - 精确计时版)
#
# v4 更新:
# - 将计时器精确地包裹在执行 COPY 的 `docker exec` 命令两端，
#   确保性能指标只反映纯粹的数据库写入时间，排除文件复制和清理的开销。
# ==============================================================================

set -e

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)

# --- 配置区 ---
CONTAINER_NAME="my-postgis-container"
DB_USER="postgres"
DB_NAME="postgres"
TARGET_TABLE="performance_wa"
TBL_DIR_IN_LOCAL="/data6/zhangdw/datasets/beijingshi_tbl_100k"
ROWS_PER_FILE=100000

# --- 辅助脚本路径 ---
DISABLE_SCRIPT="${SCRIPT_DIR}/disable_geomesa_features.sh"
ENABLE_SCRIPT="${SCRIPT_DIR}/enable_geomesa_features.sh"


echo "=================================================="
echo "$(date '+%Y-%m-%d %H:%M:%S') - 开始全量数据导入流程..."
echo "=================================================="

# 1. 调用禁用脚本
echo -e "\n>>> 阶段 1: 禁用 GeoMesa 特性..."
bash "${DISABLE_SCRIPT}"

# 2. 清空目标表
echo -e "\n>>> 阶段 2: 清空写入缓冲区表 '${TARGET_TABLE}'..."
docker exec -i "${CONTAINER_NAME}" psql -U "${DB_USER}" -d "${DB_NAME}" -c "DELETE FROM ${TARGET_TABLE};"
echo "表已清空。"

# 3. 查找要导入的文件列表
file_list=$(find "${TBL_DIR_IN_LOCAL}" -maxdepth 1 -type f -name "*.tbl" | sort)
files=($file_list)
total_files=${#files[@]}
if [ "$total_files" -eq 0 ]; then
    echo "错误: 在目录 '${TBL_DIR_IN_LOCAL}' 中没有找到任何 .tbl 文件。"
    bash "${ENABLE_SCRIPT}" # 恢复环境
    exit 1
fi
echo "共找到 ${total_files} 个文件需要导入。"


# 4. 循环导入并计时
echo -e "\n>>> 阶段 4: 开始循环导入文件..."
success_count=0
fail_count=0
total_copy_duration=0 # [MODIFIED] 变量名修改为更精确的 total_copy_duration
start_total_time=$(date +%s.%N)

for i in "${!files[@]}"; do
    local_full_path="${files[$i]}"
    filename=$(basename "$local_full_path")
    container_temp_path="/tmp/${filename}"
    current_file_num=$((i + 1))

    echo -ne "  -> 正在导入文件 ${current_file_num}/${total_files}: ${filename} ... "

    # [MODIFIED] 开始新的计时逻辑

    # 步骤 A: 预处理 - 复制文件到容器 (不计时)
    docker cp "${local_full_path}" "${CONTAINER_NAME}:${container_temp_path}" > /dev/null 2>&1

    # 步骤 B: 核心操作 - 执行 COPY (精确计时)
    copy_start_time=$(date +%s.%N)

    IMPORT_SQL="COPY ${TARGET_TABLE}(fid,geom,dtg,taxi_id) FROM '${container_temp_path}' WITH (FORMAT text, DELIMITER '|', NULL '');"

    # 执行导入，并将返回值保存
    docker exec -i "${CONTAINER_NAME}" psql -U "${DB_USER}" -d "${DB_NAME}" -v ON_ERROR_STOP=1 -c "${IMPORT_SQL}"
    exit_code=$?

    copy_end_time=$(date +%s.%N)

    # 步骤 C: 后处理 - 从容器中删除临时文件 (不计时)
    docker exec -i "${CONTAINER_NAME}" rm "${container_temp_path}" > /dev/null 2>&1

    if [ "$exit_code" -eq 0 ]; then
        copy_duration=$(echo "scale=3; $copy_end_time - $copy_start_time" | bc)
        total_copy_duration=$(echo "scale=3; $total_copy_duration + $copy_duration" | bc)
        ((success_count++))
        echo "完成 (COPY耗时: ${copy_duration}s)"
    else
        echo "失败！"
        ((fail_count++))
    fi
done

end_total_time=$(date +%s.%N)
total_script_duration=$(echo "scale=3; $end_total_time - $start_total_time" | bc)
echo "所有文件导入尝试完毕。"


# 5. 调用恢复脚本
echo -e "\n>>> 阶段 5: 恢复 GeoMesa 特性..."
bash "${ENABLE_SCRIPT}"

# 6. 最终验证和性能报告
echo -e "\n>>> 阶段 6: 生成最终报告..."
echo "=================================================="
echo " 全量数据导入完成 - 性能报告"
echo "=================================================="
printf "脚本总执行时间: %.3f 秒\n" "$total_script_duration"
echo "--------------------------------------------------"
echo "文件处理统计:"
echo "  - 成功导入文件数: ${success_count}"
echo "  - 失败导入文件数: ${fail_count}"
echo "  - 文件总数: ${total_files}"
echo "--------------------------------------------------"

if [ "$success_count" -gt 0 ]; then
    total_rows_imported=$((success_count * ROWS_PER_FILE))
    avg_time_per_file=$(echo "scale=3; $total_copy_duration / $success_count" | bc)
    # [MODIFIED] 所有性能计算都基于精确的 total_copy_duration
    overall_throughput=$(echo "scale=0; $total_rows_imported / $total_copy_duration" | bc 2>/dev/null || echo "0")
    avg_ms_per_row=$(echo "scale=3; ($total_copy_duration * 1000) / $total_rows_imported" | bc 2>/dev/null || echo "0")

    echo "性能指标 (仅计算 COPY 命令耗时):"
    printf "  - 纯数据 COPY 总耗时: %.3f 秒\n" "$total_copy_duration"
    echo "  - 成功导入总行数 (估算): ${total_rows_imported}"
    printf "  - 平均每个文件的 COPY 时间: %.3f 秒\n" "$avg_time_per_file"
    printf "  - 平均每行 COPY 时间: %.3f 毫秒\n" "$avg_ms_per_row"
    printf "  - 纯 COPY 吞吐量: %d 条/秒\n" "$overall_throughput"
fi
echo "--------------------------------------------------"

echo "最终数据量验证..."
final_count=$(docker exec -i "${CONTAINER_NAME}" psql -U "${DB_USER}" -d "${DB_NAME}" -t -c "SELECT count(1) FROM performance;" 2>/dev/null | tr -d '[:space:]')

if [ $? -eq 0 ] && [ -n "$final_count" ]; then
    echo "  -> 'performance' 视图中的总记录数: $final_count"
    if [ "$final_count" -ge "$((success_count * ROWS_PER_FILE * 98 / 100))" ]; then
        echo "  -> 【成功】数据量符合预期！"
    else
        echo "  -> 【警告】最终数据量与成功导入文件数不符，请检查分区维护任务是否已执行。"
    fi
else
    echo "  -> 【警告】无法获取最终数据量统计。"
fi
echo "=================================================="