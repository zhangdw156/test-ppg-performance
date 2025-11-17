import os
import glob

def merge_tbl_by_lines(src_dir, batch_size):
    # 验证源文件夹是否存在
    if not os.path.isdir(src_dir):
        print(f"错误：源文件夹 '{src_dir}' 不存在，请检查路径")
        return

    # 获取源文件夹下所有.tbl文件（带完整路径），按文件名排序
    tbl_files = sorted(glob.glob(os.path.join(src_dir, "*.tbl")))
    if not tbl_files:
        print(f"提示：源文件夹 '{src_dir}' 中没有找到.tbl文件")
        return

    # 生成输出目录（与源文件夹同级：源文件夹名_100k）
    src_folder_name = os.path.basename(src_dir)
    src_parent_dir = os.path.dirname(src_dir)
    dst_dir = os.path.join(src_parent_dir, f"{src_folder_name}_100k")  # 10k表示10000行
    os.makedirs(dst_dir, exist_ok=True)
    print(f"输出目录已创建（与源文件夹同级）：{dst_dir}")

    # 初始化变量：当前累计行数、当前批次内容、输出文件序号
    current_line_count = 0  # 累计当前批次的行数
    current_batch = []      # 存储当前批次的内容
    file_index = 0          # 输出文件的序号（如merged_0.tbl、merged_1.tbl）

    # 遍历所有.tbl文件，逐行处理
    for file in tbl_files:
        file_name = os.path.basename(file)
        try:
            with open(file, 'r', encoding='utf-8') as f:
                print(f"开始处理文件：{file_name}")
                for line in f:
                    # 保留行尾换行符（如果需要去除空行可加判断：if line.strip()）
                    current_batch.append(line)
                    current_line_count += 1

                    # 当累计行数达到10000时，写入文件并重置
                    if current_line_count >= batch_size:
                        # 生成输出文件名
                        output_file = os.path.join(dst_dir, f"merged_{file_index}.tbl")
                        # 写入当前批次内容
                        with open(output_file, 'w', encoding='utf-8') as out_f:
                            out_f.writelines(current_batch)
                        print(f"已生成文件：{output_file}（{current_line_count}行）")
                        # 重置计数器和缓冲区
                        current_line_count = 0
                        current_batch = []
                        file_index += 1
        except Exception as e:
            print(f"警告：处理文件 {file_name} 时出错 - {str(e)}")
            continue  # 跳过错误文件，继续处理下一个

    # 处理剩余不足10000行的内容
    if current_batch:
        output_file = os.path.join(dst_dir, f"merged_{file_index}.tbl")
        with open(output_file, 'w', encoding='utf-8') as out_f:
            out_f.writelines(current_batch)
        print(f"已生成文件：{output_file}（{current_line_count}行，最后一批）")
        file_index += 1

    print(f"\n所有文件处理完成！共生成 {file_index} 个合并文件")

# 使用示例
if __name__ == "__main__":
    # 替换为你的.tbl文件所在文件夹路径
    source_directory = r"D:\datasets\beijingshi_tbl"
    # 每10000行合并一个文件（可修改batch_size参数调整行数）
    merge_tbl_by_lines(source_directory, batch_size=100_000)