#!/bin/bash

# ==============================================================================
# Shell脚本：批量导入 .tbl 文件到 PostgreSQL Docker 容器
#
# 使用方法:
# 1. 修改下面的 "--- 配置区 ---" 中的容器名、数据库信息和文件目录。
# 2. 在终端中给予此脚本执行权限: chmod +x import_tbl_via_docker.sh
# 3. 运行脚本: ./import_tbl_via_docker.sh
# ==============================================================================

# --- 配置区 ---

# 1. Docker 容器名
#    请确保将此名称替换为您正在运行的 PostgreSQL 容器的实际名称或ID
CONTAINER_NAME="stag-gstria-postgis_postgis_1" # <--- 修改这里！

# 2. 数据库连接详细信息
#    当 psql 在容器内部运行时，主机名通常是 'localhost'
DB_USER="postgres"
DB_NAME="postgres"
TABLE_NAME="performance"
DB_PASSWD="ds123456"

# 3. 存放 .tbl 文件的目录
#    这仍然是主机上的路径，脚本会从这里读取文件
TBL_DIR="/home/gstria/datasets/beijingshi_tbl" # <--- 修改这里！

# --- 脚本主逻辑 (通常无需修改以下内容) ---

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

# 遍历找到的每一个 .tbl 文件
for tbl_file in "${files[@]}"; do
    filename=$(basename "$tbl_file")
    echo "--------------------------------------------------"
    echo "准备导入文件: $filename"

    # 构建 \copy 命令，从标准输入(STDIN)读取数据
    # 这是关键的改动，psql 将从管道接收数据，而不是直接读取文件
    COMMAND="\copy ${TABLE_NAME}(geom,dtg,taxi_id) FROM STDIN WITH (FORMAT text, DELIMITER '|', NULL '')"

    # 执行导入
    # 1. `cat "${tbl_file}"`: 在主机上读取文件内容并输出到标准输出。
    # 2. `|`: 将 cat 的输出通过管道传给下一个命令。
    # 3. `docker exec -i ...`: -i 标志让 docker exec 从标准输入读取数据。
    # 4. `-e PGPASSWORD=...`: 将密码作为环境变量安全地传递给容器内的 psql。
    cat "${tbl_file}" | docker exec \
      -i \
      -e PGPASSWORD="${DB_PASSWD}" \
      "${CONTAINER_NAME}" \
      psql -U "${DB_USER}" -d "${DB_NAME}" -v ON_ERROR_STOP=1 -c "${COMMAND}"

    # 检查上一个命令（整个管道）的退出状态码
    if [ $? -eq 0 ]; then
        echo "成功: 文件 '$filename' 已成功导入。"
        ((success_count++))
    else
        echo "失败: 导入文件 '$filename' 时发生错误。"
        ((fail_count++))
        # 如果你希望在遇到第一个错误时就立即停止整个脚本，取消下面一行的注释
        # exit 1
    fi
done

echo "=================================================="
echo "所有文件处理完毕。"
echo "成功导入: $success_count 个文件"
echo "导入失败: $fail_count 个文件"
echo "=================================================="

# 恢复 shell 选项
shopt -u nullglob