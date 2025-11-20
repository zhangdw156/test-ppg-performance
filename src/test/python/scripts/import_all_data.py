#!/usr/bin/env python3
import os
import sys
import time
import logging
import subprocess
import shlex  # 引入 shlex 模块用于安全地引用shell参数
from pathlib import Path

# ==================== 日志配置 ====================
# 创建日志目录（如不存在）
SCRIPT_DIR = Path(__file__).parent.resolve()
LOG_DIR = SCRIPT_DIR.parent / "logs"
LOG_DIR.mkdir(exist_ok=True)  # 不存在则创建，存在则忽略

# 日志文件名（包含当前日期）
LOG_FILE = LOG_DIR / f"import_log_{time.strftime('%Y%m%d')}.log"

# 配置日志：同时输出到控制台和文件，格式包含时间、级别、消息
logging.basicConfig(
    level=logging.INFO,  # 日志级别：INFO及以上会被记录
    format="%(asctime)s - %(levelname)s - %(message)s",  # 日志格式
    datefmt="%Y-%m-%d %H:%M:%S",  # 时间格式
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),  # 写入文件（支持中文）
        logging.StreamHandler(sys.stdout)  # 输出到控制台
    ]
)

# ==================== 配置参数 ====================
CONTAINER_NAME = "my-postgis-container"
DB_USER = "postgres"
DB_NAME = "postgres"
# --- 已修改 ---
# TARGET_TABLE 现在作为基础名称，用于查询和锁定
TARGET_TABLE_BASE = "performance"
TBL_DIR_IN_LOCAL = Path("/data6/zhangdw/datasets/beijingshi_tbl_100k")
ROWS_PER_FILE = 100000

# 辅助脚本路径
DISABLE_SCRIPT = SCRIPT_DIR.parent.parent / "bin" / "disable_geomesa_features.sh"
ENABLE_SCRIPT = SCRIPT_DIR.parent.parent / "bin" / "enable_geomesa_features.sh"

# ==================== 工具函数 ====================
def run_command(cmd, check=True, capture_output=False):
    """执行 shell 命令，返回结果或记录错误日志并退出"""
    try P
    result = subprocess.run(
        cmd,
        shell=True,
        check=check,
        capture_output=capture_output,
        text=True
    )
    return result
except subprocess.CalledProcessError as e:
# 在主逻辑中捕获错误，这里只记录通用错误
logging.error(f"命令执行失败: {e.cmd}")
if capture_output:
    logging.error(f"错误输出: {e.stderr.strip()}")
# 让调用者决定是否退出
raise e

# ==================== 新增的导入核心函数 ====================
def import_single_file_with_lock(file_path):
    """
    使用包含锁和动态分区名的事务来导入单个文件。
    该函数包含两个主要步骤：
    1. 查询数据库获取当前活动的、格式化好的分区表名。
    2. 构建并执行一个包含 BEGIN, LOCK, COPY, COMMIT 的 shell 命令。
    """
    # --- 步骤 1: 查询数据库，获取当前活动的、格式化好的分区表名 ---
    get_partition_sql = (
        f"SELECT '\"{TARGET_TABLE_BASE}_wa_' || lpad(value::text, 3, '0') || '\"' "
        f"FROM \"public\".\"geomesa_wa_seq\" WHERE type_name = '{TARGET_TABLE_BASE}'"
    )
    get_partition_cmd = (
        f"docker exec {CONTAINER_NAME} "
        f"psql -U {DB_USER} -d {DB_NAME} -tA -c {shlex.quote(get_partition_sql)}"
    )

    try:
        result = run_command(get_partition_cmd, capture_output=True)
        partition_name = result.stdout.strip()
        if not partition_name:
            # 如果查询结果为空，这是一个严重错误，直接抛出异常
            raise ValueError(f"未能从 geomesa_wa_seq 获取到分区名 (type_name='{TARGET_TABLE_BASE}')")

        logging.info(f"      -> 动态获取分区表名: {partition_name}")

    except (subprocess.CalledProcessError, ValueError) as e:
        logging.error(f"      -> 获取动态分区表名失败！")
        if isinstance(e, subprocess.CalledProcessError):
            logging.error(f"      -> 错误输出: {e.stderr.strip()}")
        else:
            logging.error(f"      -> 错误原因: {e}")
        # 返回一个模拟的失败结果，让主循环处理
        return subprocess.CompletedProcess(args=get_partition_cmd, returncode=1, stderr=str(e))

    # --- 步骤 2: 构建并执行包含完整事务的最终加载命令 ---
    lock_table_name = f'"{TARGET_TABLE_BASE}_wa"'
    copy_options = "WITH (FORMAT text, DELIMITER E'|', NULL E'')"
    copy_sql = f"COPY public.{partition_name}(fid,geom,dtg,taxi_id) FROM STDIN {copy_options};"

    # 结构: ( {一系列echo输出SQL脚本; cat文件内容; echo结束符和COMMIT;} ) | psql
    final_cmd = (
        f'({{ '
        f'echo "BEGIN;"; '
        f'echo \'LOCK TABLE public.{lock_table_name} IN SHARE UPDATE EXCLUSIVE MODE;\'; '
        f'echo {shlex.quote(copy_sql)}; '
        f'cat {file_path}; '
        f'echo; echo "\\."; '  # COPY FROM STDIN 的结束标记
        f'echo "COMMIT;"; '
        f'}} ) | docker exec -i {CONTAINER_NAME} '
        f'psql -U {DB_USER} -d {DB_NAME} -q -v ON_ERROR_STOP=1'
    )

    # 执行最终的导入命令
    return run_command(final_cmd, check=False, capture_output=True)


# ==================== 主逻辑 ====================
def main():
    logging.info("=" * 50)
    start_total_time = time.time()
    logging.info(f"开始全量数据导入流程...")
    logging.info("=" * 50)

    # 1. 禁用 GeoMesa 特性
    logging.info("\n>>> 阶段 1: 禁用 GeoMesa 特性...")
    run_command(f"bash {DISABLE_SCRIPT}")

    # 2. 清空目标表的 *所有分区*
    # --- 已修改 ---
    # 注意：这里我们清空的是主分区表，它会自动级联清空所有子分区
    logging.info(f"\n>>> 阶段 2: 清空主分区表 '{TARGET_TABLE_BASE}_wa' (将级联清空所有子分区)...")
    run_command(
        f"docker exec -i {CONTAINER_NAME} psql -U {DB_USER} -d {DB_NAME} -c 'TRUNCATE {TARGET_TABLE_BASE}_wa;'"
    )
    logging.info("所有分区表已清空。")

    # 3. 查找要导入的文件列表
    logging.info("\n>>> 阶段 3: 查找数据文件...")
    tbl_files = sorted(TBL_DIR_IN_LOCAL.glob("*.tbl"))
    total_files = len(tbl_files)
    if total_files == 0:
        logging.error(f"在目录 '{TBL_DIR_IN_LOCAL}' 中未找到任何 .tbl 文件。")
        run_command(f"bash {ENABLE_SCRIPT}")
        sys.exit(1)
    logging.info(f"共找到 {total_files} 个文件需要导入。")

    # 4. 循环导入文件并计时
    logging.info("\n>>> 阶段 4: 开始循环导入文件...")
    success_count = 0
    fail_count = 0
    total_import_duration = 0.0  # 总导入耗时（秒）

    for i, file_path in enumerate(tbl_files, 1):
        filename = file_path.name
        logging.info(f"  -> 正在导入文件 {i}/{total_files}: {filename} ... ")
        import_start = time.time()

        # --- 已修改: 调用新的导入函数 ---
        result = import_single_file_with_lock(file_path)

        import_end = time.time()
        import_duration = import_end - import_start

        if result.returncode == 0:
            success_count += 1
            total_import_duration += import_duration
            logging.info(f"  -> 导入文件 {i}/{total_files}: {filename} ... ✅ (耗时: {import_duration:.3f}s)")
        else:
            fail_count += 1
            logging.error(f"  -> 导入文件 {i}/{total_files}: {filename} ... ❌")
            # 错误信息已经在 import_single_file_with_lock 函数中记录，这里可以只记录简要信息
            logging.error(f"      -> 导入失败，请检查上方日志获取详细错误。")

    logging.info("\n所有文件导入尝试完毕。")

    # 5. 恢复 GeoMesa 特性
    logging.info(f"\n>>> 阶段 5: 恢复 GeoMesa 特性...")
    run_command(f"bash {ENABLE_SCRIPT}")

    # 6. 生成最终报告
    # ... (这部分无需修改，逻辑完全兼容) ...
    logging.info(f"\n>>> 阶段 6: 生成最终报告...")
    logging.info("=" * 50)
    logging.info(" 全量数据导入完成 - 性能报告")
    logging.info("=" * 50)

    end_total_time = time.time()
    total_script_duration = end_total_time - start_total_time
    logging.info(f"脚本总执行时间: {total_script_duration:.3f} 秒")
    logging.info("-" * 50)
    logging.info("文件处理统计:")
    logging.info(f"  - 成功导入文件数: {success_count}")
    logging.info(f"  - 失败导入文件数: {fail_count}")
    logging.info(f"  - 文件总数: {total_files}")
    logging.info("-" * 50)

    if success_count > 0:
        total_rows_imported = success_count * ROWS_PER_FILE
        avg_time_per_file = total_import_duration / success_count
        if total_import_duration > 0:
            overall_throughput = int(total_rows_imported / total_import_duration)
            avg_ms_per_row = (total_import_duration * 1000) / total_rows_imported
        else:
            overall_throughput = 0
            avg_ms_per_row = 0.0

        logging.info("性能指标 (仅计算导入命令耗时):")
        logging.info(f"  - 纯数据导入总耗时: {total_import_duration:.3f} 秒")
        logging.info(f"  - 成功导入总行数 (估算): {total_rows_imported}")
        logging.info(f"  - 平均每个文件的导入时间: {avg_time_per_file:.3f} 秒")
        logging.info(f"  - 平均每行处理时间: {avg_ms_per_row:.3f} 毫秒")
        logging.info(f"  - 纯导入吞吐量: {overall_throughput} 条/秒")

    logging.info("-" * 50)

    logging.info("最终数据量验证...")
    # --- 已修改 ---
    # 验证时，从主分区表查询，它会包含所有子分区的数据
    final_count_table = f"{TARGET_TABLE_BASE}_wa"
    cmd = (
        f"docker exec -i {CONTAINER_NAME} psql -U {DB_USER} -d {DB_NAME} -t -c 'SELECT count(1) FROM {final_count_table};'"
        " 2>/dev/null | tr -d '[:space:]'"
    )
    result = run_command(cmd, check=False, capture_output=True)
    final_count = result.stdout.strip()

    if result.returncode == 0 and final_count and final_count.isdigit():
        logging.info(f"  -> '{final_count_table}' 表中的总记录数: {final_count}")
        expected_rows = success_count * ROWS_PER_FILE
        if int(final_count) == expected_rows:
            logging.info("  -> 【成功】数据量与预期完全相符！")
        else:
            logging.warning("  -> 【警告】最终数据量与成功导入文件数不符，请检查。")
    else:
        logging.warning("  -> 【警告】无法获取最终数据量统计。")

    logging.info("=" * 50)

if __name__ == "__main__":
    main()