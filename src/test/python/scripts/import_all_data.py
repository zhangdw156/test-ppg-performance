#!/usr/bin/env python3
import os
import sys
import time
import logging
import subprocess
import shlex
from pathlib import Path

# ==================== 日志配置 ====================
SCRIPT_DIR = Path(__file__).parent.resolve()
LOG_DIR = SCRIPT_DIR.parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

LOG_FILE = LOG_DIR / f"import_log_{time.strftime('%Y%m%d')}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)

# ==================== 配置参数 ====================
CONTAINER_NAME = "my-postgis-container"
DB_USER = "postgres"
DB_NAME = "postgres"

TARGET_TABLE_BASE = "performance"

TBL_DIR_IN_LOCAL = Path("/data6/zhangdw/datasets/beijingshi_tbl_100k")
ROWS_PER_FILE = 100000

# ==================== 工具函数 ====================
def run_command(cmd, check=True, capture_output=False):
    """
    执行 shell 命令。
    """
    try:
        result = subprocess.run(
            cmd,
            shell=True,
            check=check,
            capture_output=capture_output,
            text=True
        )
        return result
    except subprocess.CalledProcessError as e:
        logging.error(f"命令执行失败: {e.cmd}")
        if capture_output and e.stderr:
            logging.error(f"错误输出: {e.stderr.strip()}")
        raise e

# ==================== 核心导入函数 ====================
def import_single_file_with_lock(file_path):
    """
    使用包含锁和动态分区名的事务来导入单个文件。
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
            raise ValueError(f"未能从 geomesa_wa_seq 获取到分区名 (type_name='{TARGET_TABLE_BASE}')")

        logging.info(f"      -> 动态获取分区表名: {partition_name}")

    except (subprocess.CalledProcessError, ValueError) as e:
        logging.error(f"      -> 获取动态分区表名失败！")
        if isinstance(e, subprocess.CalledProcessError):
            if e.stderr: logging.error(f"      -> 错误输出: {e.stderr.strip()}")
        else:
            logging.error(f"      -> 错误原因: {e}")
        return subprocess.CompletedProcess(args=get_partition_cmd, returncode=1, stderr=str(e))

    # --- 步骤 2: 构建并执行包含完整事务的最终加载命令 ---
    lock_table_name = f'"{TARGET_TABLE_BASE}_wa"'
    copy_options = "WITH (FORMAT text, DELIMITER E'|', NULL E'')"
    copy_sql = f"COPY public.{partition_name}(fid,geom,dtg,taxi_id) FROM STDIN {copy_options};"

    # 使用 shlex.quote 确保文件路径安全，防止路径中有空格导致 cat 失败
    safe_file_path = shlex.quote(str(file_path))

    final_cmd = (
        f'({{ '
        f'echo "BEGIN;"; '
        f'echo \'LOCK TABLE public.{lock_table_name} IN SHARE UPDATE EXCLUSIVE MODE;\'; '
        f'echo {shlex.quote(copy_sql)}; '
        f'cat {safe_file_path}; '           # <--- 这里使用了 quote 后的路径
        f'echo; echo "\\."; '
        f'echo "COMMIT;"; '
        f'}} ) | docker exec -i {CONTAINER_NAME} '
        f'psql -U {DB_USER} -d {DB_NAME} -q -v ON_ERROR_STOP=1'
    )

    return run_command(final_cmd, check=False, capture_output=True)


# ==================== 主逻辑 ====================
def main():
    logging.info("=" * 50)
    start_total_time = time.time()
    logging.info(f"开始全量数据导入流程 (带锁机制)...")
    logging.info("=" * 50)

    # 1. 清空目标表
    logging.info(f"\n>>> 阶段 1: 清空主分区表 '{TARGET_TABLE_BASE}_wa' (将级联清空所有子分区)...")
    try:
        run_command(
            f"docker exec -i {CONTAINER_NAME} psql -U {DB_USER} -d {DB_NAME} -c 'TRUNCATE {TARGET_TABLE_BASE}_wa;'"
        )
        logging.info("所有分区表已清空。")
    except subprocess.CalledProcessError:
        logging.error("清空表失败，脚本终止。")
        sys.exit(1)

    # 2. 查找文件
    logging.info(f"\n>>> 阶段 2: 查找数据文件...")
    tbl_files = sorted(TBL_DIR_IN_LOCAL.glob("*.tbl"))
    total_files = len(tbl_files)
    if total_files == 0:
        logging.error(f"在目录 '{TBL_DIR_IN_LOCAL}' 中未找到任何 .tbl 文件。")
        sys.exit(1)
    logging.info(f"共找到 {total_files} 个文件需要导入。")

    # 3. 循环导入
    logging.info(f"\n>>> 阶段 3: 开始循环导入文件...")
    success_count = 0
    fail_count = 0
    total_import_duration = 0.0

    for i, file_path in enumerate(tbl_files, 1):
        filename = file_path.name
        logging.info(f"  -> 正在导入文件 {i}/{total_files}: {filename} ... ")

        import_start = time.time()
        result = import_single_file_with_lock(file_path)
        import_end = time.time()
        import_duration = import_end - import_start

        if result.returncode == 0:
            success_count += 1
            total_import_duration += import_duration
            logging.info(f"  -> 导入文件 {i}/{total_files}: {filename} ... ✅ (耗时: {import_duration:.3f}s)")
        else:
            # ============================================================
            # 关键修正：在这里显式打印 SQL 错误信息
            # ============================================================
            fail_count += 1
            logging.error(f"  -> 导入文件 {i}/{total_files}: {filename} ... ❌")
            if result.stderr:
                logging.error(f"      ⬇⬇⬇ SQL/Shell 错误详情 ⬇⬇⬇")
                logging.error(f"{result.stderr.strip()}")
                logging.error(f"      ⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆")
            else:
                logging.error(f"      -> 未知错误 (无标准错误输出 returned)")

    logging.info("\n所有文件导入尝试完毕。")

    # 4. 生成报告
    logging.info(f"\n>>> 阶段 4: 生成最终报告...")
    logging.info("=" * 50)

    # ... (后续报告代码保持不变)
    end_total_time = time.time()
    total_script_duration = end_total_time - start_total_time
    logging.info(f"脚本总执行时间: {total_script_duration:.3f} 秒")
    logging.info("-" * 50)
    logging.info(f"  - 成功导入文件数: {success_count}")
    logging.info(f"  - 失败导入文件数: {fail_count}")
    logging.info("-" * 50)

    if success_count > 0:
        total_rows_imported = success_count * ROWS_PER_FILE
        avg_time_per_file = total_import_duration / success_count
        if total_import_duration > 0:
            overall_throughput = int(total_rows_imported / total_import_duration)
        else:
            overall_throughput = 0

        logging.info("性能指标 (仅计算导入命令耗时):")
        logging.info(f"  - 纯导入吞吐量: {overall_throughput} 条/秒")

    logging.info("-" * 50)

    # 最终数据量验证
    logging.info("最终数据量验证...")
    final_count_table = f"{TARGET_TABLE_BASE}_wa"
    cmd = (
        f"docker exec -i {CONTAINER_NAME} psql -U {DB_USER} -d {DB_NAME} -t -c 'SELECT count(1) FROM {final_count_table};'"
        " 2>/dev/null | tr -d '[:space:]'"
    )
    try:
        result = run_command(cmd, check=False, capture_output=True)
        final_count = result.stdout.strip()
        if result.returncode == 0 and final_count and final_count.isdigit():
            logging.info(f"  -> '{final_count_table}' 表中的总记录数: {final_count}")
            expected_rows = success_count * ROWS_PER_FILE
            if int(final_count) == expected_rows:
                logging.info("  -> 【成功】数据量与预期完全相符！")
            else:
                logging.warning(f"  -> 【警告】最终数据量 ({final_count}) 与预期 ({expected_rows}) 不符。")
        else:
            logging.warning("  -> 【警告】无法获取最终数据量统计。")
    except Exception as e:
        logging.warning(f"  -> 验证步骤发生错误: {e}")
    logging.info("=" * 50)

if __name__ == "__main__":
    main()