#!/usr/bin/env python3
import os
import sys
import time
import subprocess
from pathlib import Path

# ==================== 配置参数 ====================
CONTAINER_NAME = "my-postgis-container"
DB_USER = "postgres"
DB_NAME = "postgres"
TARGET_TABLE = "performance_wa"
TBL_DIR_IN_LOCAL = Path("/data6/zhangdw/datasets/beijingshi_tbl_100k")
ROWS_PER_FILE = 100000

# 辅助脚本路径（基于当前脚本所在目录）
SCRIPT_DIR = Path(__file__).parent.resolve()
DISABLE_SCRIPT = SCRIPT_DIR.parent / "bin" / "disable_geomesa_features.sh"
ENABLE_SCRIPT = SCRIPT_DIR.parent / "bin" / "enable_geomesa_features.sh"

# ==================== 工具函数 ====================
def run_command(cmd, check=True, capture_output=False):
    """执行 shell 命令，返回结果或打印错误"""
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
        print(f"命令执行失败: {e.cmd}", file=sys.stderr)
        print(f"错误输出: {e.stderr}", file=sys.stderr)
        sys.exit(1)

# ==================== 主逻辑 ====================
def main():
    print("=" * 50)
    start_total_time = time.time()  # 脚本总开始时间（秒，浮点数）
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - 开始全量数据导入流程...")
    print("=" * 50)

    # 1. 禁用 GeoMesa 特性
    print("\n>>> 阶段 1: 禁用 GeoMesa 特性...")
    run_command(f"bash {DISABLE_SCRIPT}")

    # 2. 清空目标表
    print(f"\n>>> 阶段 2: 清空写入缓冲区表 '{TARGET_TABLE}'...")
    run_command(
        f"docker exec -i {CONTAINER_NAME} psql -U {DB_USER} -d {DB_NAME} -c 'DELETE FROM {TARGET_TABLE};'"
    )
    print("表已清空。")

    # 3. 查找要导入的文件列表
    print("\n>>> 阶段 3: 查找数据文件...")
    tbl_files = sorted(TBL_DIR_IN_LOCAL.glob("*.tbl"))
    total_files = len(tbl_files)
    if total_files == 0:
        print(f"错误: 在目录 '{TBL_DIR_IN_LOCAL}' 中未找到任何 .tbl 文件。", file=sys.stderr)
        run_command(f"bash {ENABLE_SCRIPT}")
        sys.exit(1)
    print(f"共找到 {total_files} 个文件需要导入。")

    # 4. 循环导入文件并计时
    print("\n>>> 阶段 4: 开始循环导入文件...")
    success_count = 0
    fail_count = 0
    total_import_duration = 0.0  # 总导入耗时（秒）

    for i, file_path in enumerate(tbl_files, 1):
        filename = file_path.name
        current_file_num = i
        print(f"  -> 正在导入文件 {current_file_num}/{total_files}: {filename} ... ", end="", flush=True)

        # 记录单个文件导入开始时间
        import_start = time.time()

        # 执行导入（通过管道传递数据到 docker exec）
        cmd = (
            f"cat {file_path} | docker exec -i {CONTAINER_NAME} "
            f"psql -U {DB_USER} -d {DB_NAME} -q -v ON_ERROR_STOP=1 "
            f"-c 'COPY {TARGET_TABLE}(fid,geom,dtg,taxi_id) FROM STDIN WITH (FORMAT text, DELIMITER \"|\", NULL \"\");'"
        )
        result = run_command(cmd, check=False, capture_output=True)

        # 计算单个文件导入耗时
        import_end = time.time()
        import_duration = import_end - import_start

        # 检查执行结果
        if result.returncode == 0:
            success_count += 1
            total_import_duration += import_duration
            print(f"✅ (耗时: {import_duration:.3f}s)")
        else:
            fail_count += 1
            print(f"❌\n错误信息: {result.stderr}")
            # 如需遇到失败即停止，取消下面的注释
            # run_command(f"bash {ENABLE_SCRIPT}")
            # sys.exit(1)

    print("\n所有文件导入尝试完毕。")

    # 5. 恢复 GeoMesa 特性
    print(f"\n>>> 阶段 5: 恢复 GeoMesa 特性...")
    run_command(f"bash {ENABLE_SCRIPT}")

    # 6. 生成最终报告
    print(f"\n>>> 阶段 6: 生成最终报告...")
    print("=" * 50)
    print(" 全量数据导入完成 - 性能报告")
    print("=" * 50)

    # 总脚本执行时间
    end_total_time = time.time()
    total_script_duration = end_total_time - start_total_time
    print(f"脚本总执行时间: {total_script_duration:.3f} 秒")
    print("-" * 50)
    print("文件处理统计:")
    print(f"  - 成功导入文件数: {success_count}")
    print(f"  - 失败导入文件数: {fail_count}")
    print(f"  - 文件总数: {total_files}")
    print("-" * 50)

    # 性能指标（仅成功时计算）
    if success_count > 0:
        total_rows_imported = success_count * ROWS_PER_FILE
        avg_time_per_file = total_import_duration / success_count

        # 吞吐量计算（避免除以 0）
        if total_import_duration > 0:
            overall_throughput = int(total_rows_imported / total_import_duration)
            avg_ms_per_row = (total_import_duration * 1000) / total_rows_imported
        else:
            overall_throughput = 0
            avg_ms_per_row = 0.0

        print("性能指标 (仅计算导入命令耗时):")
        print(f"  - 纯数据导入总耗时: {total_import_duration:.3f} 秒")
        print(f"  - 成功导入总行数 (估算): {total_rows_imported}")
        print(f"  - 平均每个文件的导入时间: {avg_time_per_file:.3f} 秒")
        print(f"  - 平均每行处理时间: {avg_ms_per_row:.3f} 毫秒")
        print(f"  - 纯导入吞吐量: {overall_throughput} 条/秒")

    print("-" * 50)

    # 最终数据量验证
    print("最终数据量验证...")
    cmd = (
        f"docker exec -i {CONTAINER_NAME} psql -U {DB_USER} -d {DB_NAME} -t -c 'SELECT count(1) FROM performance;'"
        " 2>/dev/null | tr -d '[:space:]'"
    )
    result = run_command(cmd, check=False, capture_output=True)
    final_count = result.stdout.strip()

    if result.returncode == 0 and final_count:
        print(f"  -> 'performance' 视图中的总记录数: {final_count}")
        expected_min = success_count * ROWS_PER_FILE * 98 // 100  # 98% 预期值
        if int(final_count) >= expected_min:
            print("  -> 【成功】数据量符合预期！")
        else:
            print("  -> 【警告】最终数据量与成功导入文件数不符，请检查分区维护任务。")
    else:
        print("  -> 【警告】无法获取最终数据量统计。")

    print("=" * 50)

if __name__ == "__main__":
    main()