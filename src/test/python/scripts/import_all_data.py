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
    """执行简单 shell 命令"""
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

# ==================== 核心导入函数 (重大修改) ====================
def import_single_file_with_lock(file_path):
    """
    使用 Python Popen 流式传输数据，精确控制换行符，避免 'literal newline' 错误。
    """

    # --- 步骤 1: 获取动态分区表名 (保持不变) ---
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
            raise ValueError(f"未能从 geomesa_wa_seq 获取分区名")
        logging.info(f"      -> 动态获取分区表名: {partition_name}")
    except Exception as e:
        logging.error(f"      -> 获取分区表名失败: {e}")
        if hasattr(e, 'stderr') and e.stderr: logging.error(f"      -> details: {e.stderr.strip()}")
        return subprocess.CompletedProcess(args=get_partition_cmd, returncode=1, stderr=str(e))

    # --- 步骤 2: 使用 Popen 进行精确的数据管道传输 ---

    lock_table_name = f'"{TARGET_TABLE_BASE}_wa"'
    copy_options = "WITH (FORMAT text, DELIMITER E'|', NULL E'')"
    copy_sql = f"COPY public.{partition_name}(fid,geom,dtg,taxi_id) FROM STDIN {copy_options};"

    # 1. 准备 SQL 头部
    sql_header = (
        f"BEGIN;\n"
        f"LOCK TABLE public.{lock_table_name} IN SHARE UPDATE EXCLUSIVE MODE;\n"
        f"{copy_sql}\n"
    ).encode('utf-8') # 转换为字节

    # 2. 准备 SQL 尾部 (确保 \. 单独一行)
    sql_footer = b"\\.\nCOMMIT;\n"

    # 3. 启动 psql 进程
    psql_cmd = f"docker exec -i {CONTAINER_NAME} psql -U {DB_USER} -d {DB_NAME} -q -v ON_ERROR_STOP=1"

    proc = None
    try:
        # 打开进程，准备向 stdin 写入
        proc = subprocess.Popen(
            psql_cmd,
            shell=True,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

        # A. 写入 SQL 头部
        proc.stdin.write(sql_header)

        # B. 流式写入文件内容
        has_trailing_newline = False
        # 使用二进制模式读取，避免编码问题，且效率更高
        with open(file_path, 'rb') as f:
            # 分块读取 (1MB 缓冲区)，比一次性读取省内存
            while chunk := f.read(1024 * 1024):
                proc.stdin.write(chunk)
                # 检查最后一个字节是否是换行符
                if chunk:
                    has_trailing_newline = chunk.endswith(b'\n')

        # C. 关键修正：只有当文件末尾没有换行符时，才补一个换行符
        if not has_trailing_newline:
            proc.stdin.write(b'\n')

        # D. 写入 SQL 尾部 (\. 和 COMMIT)
        proc.stdin.write(sql_footer)

        # E. 关闭 stdin，发送 EOF 信号，并获取输出
        stdout_bytes, stderr_bytes = proc.communicate()

        # 构建返回对象
        stdout_str = stdout_bytes.decode('utf-8', errors='replace')
        stderr_str = stderr_bytes.decode('utf-8', errors='replace')

        if proc.returncode != 0:
            # 模拟 CalledProcessError 以便主逻辑捕获
            raise subprocess.CalledProcessError(proc.returncode, psql_cmd, output=stdout_str, stderr=stderr_str)

        return subprocess.CompletedProcess(args=psql_cmd, returncode=0, stdout=stdout_str, stderr=stderr_str)

    except subprocess.CalledProcessError as e:
        # 捕获我们自己抛出的异常
        return subprocess.CompletedProcess(args=psql_cmd, returncode=e.returncode, stderr=e.stderr)
    except Exception as e:
        # 捕获 IO 错误或其他异常
        error_msg = f"Python Popen 异常: {str(e)}"
        logging.error(error_msg)
        if proc:
            proc.kill()
        return subprocess.CompletedProcess(args=psql_cmd, returncode=1, stderr=error_msg)


# ==================== 主逻辑 ====================
def main():
    logging.info("=" * 50)
    start_total_time = time.time()
    logging.info(f"开始全量数据导入流程 (带锁机制 - Python Popen优化版)...")
    logging.info("=" * 50)

    # 1. 清空目标表
    logging.info(f"\n>>> 阶段 1: 清空主分区表 '{TARGET_TABLE_BASE}_wa'...")
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

    end_total_time = time.time()
    total_script_duration = end_total_time - start_total_time
    logging.info(f"脚本总执行时间: {total_script_duration:.3f} 秒")
    logging.info("-" * 50)
    logging.info(f"  - 成功导入文件数: {success_count}")
    logging.info(f"  - 失败导入文件数: {fail_count}")
    logging.info("-" * 50)

    if success_count > 0:
        total_rows_imported = success_count * ROWS_PER_FILE
        if total_import_duration > 0:
            overall_throughput = int(total_rows_imported / total_import_duration)
        else:
            overall_throughput = 0
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