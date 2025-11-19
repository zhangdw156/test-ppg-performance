#!/bin/bash
# ==============================================================================
# Shell脚本：全量数据导入与性能测试
#
# 流程:
# 1. 调用 `disable_geomesa_features.sh` 准备数据库环境。
# 2. 清空目标表 `performance_wa`。
# 3. 循环导入指定目录下的所有 `.tbl` 文件。
# 4. 记录每个文件的导入时间，并计算总体性能指标。
# 5. 调用 `enable_geomesa_features.sh` 恢复数据库功能。
# 6. 最终验证导入的数据总量。
#
# 使用方法:
# 1. 确保此脚本与 disable_geomesa_features.sh 和 enable_geomesa_features.sh 在同一目录。
# 2. 确保 .tbl 文件目录已准备好。
# 3. 运行脚本: ./import_all_data.sh
# ==============================================================================

set -e # 任何命令失败则立即退出

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)

# --- 配置区 (请根据你的环境修改) ---
CONTAINER_NAME="my-postgis-container"
DB_USER="postgres"
DB_NAME="postgres"
TARGET_TABLE="performance_wa"
TBL_DIR_IN_LOCAL="/data6/zhangdw/datasets/beijingshi_tbl_100k"
ROWS_PER_FILE=100000 # 估算值，用于计算总行数和吞吐量

# --- 辅助脚本路径 ---
DISABLE_SCRIPT="${SCRIPT_DIR}/disable_geomesa_features.sh"
ENABLE_SCRIPT="${SCRIPT_DIR}/enable_geomesa_features.sh"

# --- 脚本开始 ---
# 检查辅助脚本是否存在
if [ ! -f "$DISABLE_SCRIPT" ] || [ ! -f "$ENABLE_SCRIPT" ]; then
    echo "错误: 辅助脚本 ${DISABLE_SCRIPT} 或 ${ENABLE_SCRIPT} 未找到。"
    exit 1
fi

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
echo -e "\n>>> 阶段 3: 查找并准备导入文件..."
# 使用 find 和 sort 来获取一个稳定排序的文件列表
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
# 初始化计数器
success_count=0
fail_count=0
total_import_duration=0
start_total_time=$(date +%s.%N)

for i in "${!files[@]}"; do
    full_path="${files[$i]}"
    filename=$(basename "$full_path")
    current_file_num=$((i + 1))

    echo -ne "  -> 正在导入文件 ${current_file_num}/${total_files}: ${filename} ... "

    # 记录单个文件开始时间
    file_start_time=$(date +%s.%N)

    # 执行导入命令
    cat "${full_path}" | docker exec -i "${CONTAINER_NAME}" \
      psql -U "${DB_USER}" -d "${DB_NAME}" -q -v ON_ERROR_STOP=1 \
      -c "COPY ${TARGET_TABLE}(fid,geom,dtg,taxi_id) FROM STDIN WITH (FORMAT text, DELIMITER '|', NULL '');" > /dev/null 2>&1

    # 检查退出状态码 (-q 选项让 psql 在成功时不输出，> /dev/null 2>&1 屏蔽所有输出)
    if [ $? -eq 0 ]; then
        file_end_time=$(date +%s.%N)
        file_duration=$(echo "scale=3; $file_end_time - $file_start_time" | bc)
        total_import_duration=$(echo "scale=3; $total_import_duration + $file_duration" | bc)
        ((success_count++))
        echo "完成 (耗时: ${file_duration}s)"
    else
        echo "失败！"
        # 你可以在这里添加错误处理逻辑，比如记录失败文件名
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
echo "脚本总执行时间: ${total_script_duration} 秒"
echo "--------------------------------------------------"
echo "文件处理统计:"
echo "  - 成功导入文件数: ${success_count}"
echo "  - 失败导入文件数: ${fail_count}"
echo "  - 文件总数: ${total_files}"
echo "--------------------------------------------------"

if [ "$success_count" -gt 0 ]; then
    total_rows_imported=$((success_count * ROWS_PER_FILE))
    avg_time_per_file=$(echo "scale=3; $total_import_duration / $success_count" | bc)
    overall_throughput=$(echo "scale=0; $total_rows_imported / $total_import_duration" | bc 2>/dev/null || echo "0")
    avg_ms_per_row=$(echo "scale=3; ($total_import_duration * 1000) / $total_rows_imported" | bc 2>/dev/null || echo "0")

    echo "性能指标 (仅计算成功导入的文件):"
    echo "  - 纯数据导入总耗时: ${total_import_duration} 秒"
    echo "  - 成功导入总行数 (估算): ${total_rows_imported}"
    printf "  - 平均每个文件的导入时间: %.3f 秒\n" "$avg_time_per_file"
    printf "  - 平均每行处理时间: %.3f 毫秒\n" "$avg_ms_per_row"
    printf "  - 总吞吐量: %d 条/秒\n" "$overall_throughput"
fi
echo "--------------------------------------------------"

echo "最终数据量验证..."
# 使用 docker exec 的返回值来判断命令是否成功
final_count=$(docker exec -i "${CONTAINER_NAME}" psql -U "${DB_USER}" -d "${DB_NAME}" -t -c "SELECT count(1) FROM performance;" 2>/dev/null | tr -d '[:space:]')

if [ $? -eq 0 ] && [ -n "$final_count" ]; then
    echo "  -> 'performance' 视图中的总记录数: $final_count"
    # 进行一个简单的验证
    if [ "$final_count" -ge "$((success_count * ROWS_PER_FILE * 98 / 100))" ]; then
        echo "  -> 【成功】数据量符合预期！"
    else
        echo "  -> 【警告】最终数据量与成功导入文件数不符，请检查分区维护任务是否已执行。"
    fi
else
    echo "  -> 【警告】无法获取最终数据量统计。"
fi
echo "=================================================="