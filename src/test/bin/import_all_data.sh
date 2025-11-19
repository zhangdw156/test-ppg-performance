#!/bin/bash
# ==============================================================================
# Shell脚本：全量数据导入与性能测试 (v6 - 最终优化版)
#
# v6 更新:
# - 恢复使用最高效的 `cat | docker exec` 管道模式进行数据导入。
# - 确保计时器精确地包裹管道命令，以测量纯粹的导入性能。
# - 在脚本开头增加对 `bc` 命令的依赖检查，防止因缺少计算器导致静默失败。
# ==============================================================================

set -e

# --- 依赖检查 ---
if ! command -v bc &> /dev/null; then
    echo "错误: 本脚本需要 'bc' 命令来进行浮点数运算，但系统中未找到。"
    echo "请先安装它。在 CentOS/RHEL 上，请运行: sudo yum install -y bc"
    exit 1
fi

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
total_import_duration=0 # [MODIFIED] 变量名统一为 total_import_duration
start_total_time=$(date +%s.%N)

for i in "${!files[@]}"; do
    local_full_path="${files[$i]}"
    filename=$(basename "$local_full_path")
    current_file_num=$((i + 1))

    echo -ne "  -> 正在导入文件 ${current_file_num}/${total_files}: ${filename} ... "

    # [MODIFIED] 重新加入 -q 标志，并将 stderr 重定向到一个临时文件以捕获错误
    psql_error_log=$(mktemp)

    $import_start_time=$(date +%s.%N)

    cat "${local_full_path}" | docker exec -i "${CONTAINER_NAME}" \
      psql -U "${DB_USER}" -d "${DB_NAME}" -q -v ON_ERROR_STOP=1 \
      -c "COPY ${TARGET_TABLE}(fid,geom,dtg,taxi_id) FROM STDIN WITH (FORMAT text, DELIMITER '|', NULL '');" 2> "${psql_error_log}"
    exit_code=$?

    import_end_time=$(date +%s.%N)

    echo "${import_end_time} --- ${$import_start_time}"

    if [ "$exit_code" -eq 0 ]; then
        import_duration=$(echo "scale=3; $import_end_time - $import_start_time" | bc)
        total_import_duration=$(echo "scale=3; $total_import_duration + $import_duration" | bc)
        ((success_count++))
        echo "完成 (耗时: ${import_duration}s)"
    else
        # [MODIFIED] 改进错误输出
        echo -e "\n\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
        echo "!!!!!!!!!!!!!!!   导入失败！  !!!!!!!!!!!!!!!"
        echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
        echo "文件: ${filename}"
        echo "退出码: $exit_code"
        echo "psql 错误信息:"
        cat "${psql_error_log}"
        echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
        ((fail_count++))
        # 因为 set -e，脚本在这里就会停止
    fi

    # 清理临时日志文件
    rm -f "${psql_error_log}"
done

end_total_time=$(date +%s.%N)
total_script_duration=$(echo "scale=3; $end_total_time - $start_total_time" | bc)
echo "所有文件导入尝试完毕。"


# 5. 调用恢复脚本
echo -e "\n>>> 阶段 5: 恢复 GeoMesa 特性..."
bash "${ENABLE_SCRIPT}"

# 6. 最终验证和性能报告 [MODIFIED]
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

# [MODIFIED] 增加安全检查
if [ "$success_count" -gt 0 ]; then
    total_rows_imported=$((success_count * ROWS_PER_FILE))
    avg_time_per_file=$(echo "scale=3; $total_import_duration / $success_count" | bc)

    # 安全地计算吞吐量
    overall_throughput=0
    # 使用 awk 来比较浮点数，检查总耗时是否大于0
    if (( $(echo "$total_import_duration > 0" | bc -l) )); then
        overall_throughput=$(echo "scale=0; $total_rows_imported / $total_import_duration" | bc)
        avg_ms_per_row=$(echo "scale=3; ($total_import_duration * 1000) / $total_rows_imported" | bc)
    else
        # 如果总耗时为0或接近0，无法计算有意义的吞吐量
        avg_ms_per_row="0.000"
    fi

    echo "性能指标 (仅计算导入命令耗时):"
    printf "  - 纯数据导入总耗时: %.3f 秒\n" "$total_import_duration"
    echo "  - 成功导入总行数 (估算): ${total_rows_imported}"
    printf "  - 平均每个文件的导入时间: %.3f 秒\n" "$avg_time_per_file"
    printf "  - 平均每行处理时间: %.3f 毫秒\n" "$avg_ms_per_row"
    printf "  - 纯导入吞吐量: %d 条/秒\n" "$overall_throughput"
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