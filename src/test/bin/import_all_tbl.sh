#!/bin/bash

# ==============================================================================
# Shell脚本：批量导入 .tbl 文件到 PostgreSQL Docker 容器并计时（精确到毫秒）
#
# 使用方法:
# 1. 修改下面的 "--- 配置区 ---" 中的容器名、数据库信息和文件目录。
# 2. 在终端中给予此脚本执行权限: chmod +x import_all_tbl.sh
# 3. 运行脚本: ./import_all_tbl.sh
# ==============================================================================

# --- 配置区 ---

# 1. Docker 容器名
CONTAINER_NAME="stag-gstria-postgis_postgis_1"

# 2. 数据库连接详细信息
DB_USER="postgres"
DB_NAME="gstria"
TABLE_NAME="performance"
DB_PASSWD="ds123456"

# 3. 存放 .tbl 文件的目录
TBL_DIR="/home/gstria/datasets/beijingshi_tbl"


# 记录脚本总开始时间 (纳秒级时间戳，精确到小数点后9位)
start_total_time=$(date +%s.%N)

# 检查 docker 命令是否存在
if ! command -v docker &> /dev/null
then
    echo "错误: docker 命令未找到。请确保 Docker 已安装并正在运行。"
    exit 1
fi

# 检查指定的 Docker 容器是否正在运行
if ! docker ps --filter "name=${CONTAINER_NAME}" --format "{{.Names}}" | grep -q "^${CONTAINER_NAME}$"; then
    echo "错误: Docker 容器 '${CONTAINER_NAME}' 不存在或未在运行。"
    echo "请使用 'docker ps' 检查您正在运行的容器。"
    exit 1
fi

# 检查 TBL 文件目录是否存在
if [ ! -d "$TBL_DIR" ]; then
    echo "错误: 本地目录 '$TBL_DIR' 不存在。"
    exit 1
fi

echo "开始批量导入过程..."
echo "目标 Docker 容器: $CONTAINER_NAME"
echo "目标数据库: $DB_NAME, 目标表: $TABLE_NAME"
echo "本地 TBL 文件目录: $TBL_DIR"
echo ""

# 查找目录下的所有 .tbl 文件
shopt -s nullglob
files=("$TBL_DIR"/*.tbl)

# 检查是否找到了任何 .tbl 文件
if [ ${#files[@]} -eq 0 ]; then
    echo "在目录 '$TBL_DIR' 中没有找到任何 .tbl 文件。"
    exit 0
fi

echo "共找到 ${#files[@]} 个 .tbl 文件需要导入。"

# 计数器
success_count=0
fail_count=0
total_import_time=0  # 累计导入时间（浮点数）

# 遍历找到的每一个 .tbl 文件
for tbl_file in "${files[@]}"; do
    filename=$(basename "$tbl_file")
    echo "--------------------------------------------------"
    echo "准备导入文件: $filename"

    # 构建 \copy 命令
    COMMAND="copy ${TABLE_NAME}(fid,geom,dtg,taxi_id) FROM STDIN WITH (FORMAT text, DELIMITER '|', NULL '');"

    # 记录单个文件开始时间（纳秒级）
    start_file_time=$(date +%s.%N)

    # 执行导入
    cat "${tbl_file}" | docker exec \
      -i \
      -e PGPASSWORD="${DB_PASSWD}" \
      "${CONTAINER_NAME}" \
      psql -U "${DB_USER}" -d "${DB_NAME}" -v ON_ERROR_STOP=1 -c "${COMMAND}"

    # 检查退出状态码
    if [ $? -eq 0 ]; then
        # 记录单个文件结束时间（纳秒级）
        end_file_time=$(date +%s.%N)
        # 计算单个文件导入耗时（保留3位小数）
        file_duration=$(echo "scale=3; $end_file_time - $start_file_time" | bc)

        # 格式化显示（确保3位小数）
        printf "成功: 文件 '%s' 已成功导入。耗时: %.3f 秒。\n" "$filename" "$file_duration"

        ((success_count++))
        # 累加总导入时间（保留3位小数）
        total_import_time=$(echo "scale=3; $total_import_time + $file_duration" | bc)
    else
        echo "失败: 导入文件 '$filename' 时发生错误。"
        ((fail_count++))
    fi
done

# 记录脚本总结束时间
end_total_time=$(date +%s.%N)
# 计算脚本总耗时（保留3位小数）
total_duration=$(echo "scale=3; $end_total_time - $start_total_time" | bc)

echo "=================================================="
echo "所有文件处理完毕。"
echo "成功导入: $success_count 个文件"
echo "导入失败: $fail_count 个文件"
echo "--------------------------------------------------"
# 格式化总时间显示
printf "脚本总执行时间: %.3f 秒。\n" "$total_duration"

# 计算并显示平均导入时间（保留3位小数）
if [ $success_count -gt 0 ]; then
    average_time=$(echo "scale=3; $total_import_time / $success_count" | bc)
    printf "平均每个文件的导入时间: %.3f 秒。\n" "$average_time"
fi
echo "=================================================="

# 恢复 shell 选项
shopt -u nullglob