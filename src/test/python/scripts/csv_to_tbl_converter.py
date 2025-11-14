import os
import csv
from datetime import datetime
import uuid

def generate_tbl_from_csv_folder(input_folder):
    """
    读取一个文件夹中的所有CSV文件，将每一行转换为.tbl格式的行，
    并使用制表符分隔，然后将结果保存到同级的_tbl文件夹中。

    :param input_folder: 包含CSV文件的输入文件夹路径。
    """
    if not os.path.isdir(input_folder):
        print(f"错误：输入目录 '{input_folder}' 不存在或不是一个目录。")
        return

    parent_dir = os.path.dirname(input_folder)
    folder_name = os.path.basename(input_folder)
    # 将输出文件夹的后缀改为 _tbl
    output_folder = os.path.join(parent_dir, f"{folder_name}_tbl")

    try:
        os.makedirs(output_folder, exist_ok=True)
        print(f".tbl文件将被保存到: {output_folder}")
    except OSError as e:
        print(f"错误：创建输出目录 '{output_folder}' 失败: {e}")
        return

    csv_files = [f for f in os.listdir(input_folder) if f.lower().endswith('.csv')]
    total_files = len(csv_files)

    if total_files == 0:
        print("在指定目录中未找到任何CSV文件。")
        return

    print(f"共找到 {total_files} 个CSV文件需要处理。")

    for i, filename in enumerate(csv_files, start=1):
        input_file_path = os.path.join(input_folder, filename)

        # 将输出文件的扩展名改为 .tbl
        output_filename = os.path.splitext(filename)[0] + '.tbl'
        output_file_path = os.path.join(output_folder, output_filename)

        print(f"\n[进度: {i}/{total_files}] 正在处理文件: '{filename}' ...")

        try:
            process_single_csv_to_tbl(input_file_path, output_file_path)
        except Exception as e:
            print(f"  处理文件 '{filename}' 时发生未知错误: {e}")

    print("\n所有文件处理完毕！")

def process_single_csv_to_tbl(csv_path, tbl_path):
    """
    处理单个CSV文件，生成对应的.tbl文件。
    """
    line_count = 0
    # 从文件名中提取 taxi_id
    try:
        taxi_id = int(os.path.basename(csv_path).split('.')[0])
    except (ValueError, IndexError):
        print(f"  警告: 无法从文件名 '{os.path.basename(csv_path)}' 中解析 taxi_id，将跳过此文件。")
        return

    with open(csv_path, mode='r', encoding='utf-8') as infile, \
            open(tbl_path, mode='w', encoding='utf-8', newline='') as outfile:

        csv_reader = csv.reader(infile, delimiter=',')
        # 使用'|'作为.tbl文件的分隔符
        tbl_writer = csv.writer(outfile, delimiter='|', quoting=csv.QUOTE_NONE, escapechar='\\')

        try:
            # 读取并跳过表头
            header = next(csv_reader)
            expected_header = ['dtg_str', 'lat', 'lng', 'speed', 'geohash']
            if header != expected_header:
                print(f"  警告: 文件 '{os.path.basename(csv_path)}' 的表头与预期不符。当前表头: {header}")
        except StopIteration:
            print(f"  警告: 文件 '{os.path.basename(csv_path)}' 是空的。")
            return

        # 逐行处理数据
        for j, row in enumerate(csv_reader, start=1):
            try:
                dtg_str = row[0]
                lat_str = row[1]
                lng_str = row[2]

                # 确保日期时间字符串格式正确
                dtg = datetime.strptime(dtg_str, '%Y-%m-%d %H:%M:%S')

                lat = float(lat_str)
                lng = float(lng_str)

                geom_wkt = f"SRID=4326;POINT({lng} {lat})"

                # <-- 新增：为每一行生成一个唯一的UUID作为Feature ID
                feature_id = uuid.uuid4()

                # <-- 修改：按照 (fid, geom, dtg, taxi_id) 的顺序组织数据
                # 将 feature_id 转换为字符串并放在最前面
                tbl_row = [str(feature_id), geom_wkt, dtg, taxi_id]

                # 将处理好的行写入.tbl文件
                tbl_writer.writerow(tbl_row)
                line_count += 1

            except (ValueError, IndexError) as e:
                print(f"  [行号: {j+1}] 跳过该行，因为格式错误: {row} -> 错误: {e}")

    print(f"  处理完成，成功生成 {line_count} 行数据到 '{os.path.basename(tbl_path)}'。")


# --- 主程序入口 ---
if __name__ == "__main__":
    # 请确保将此路径替换为您的CSV文件夹实际路径
    csv_folder_path = r"D:\datasets\beijingshi"

    # 调用主函数开始转换
    generate_tbl_from_csv_folder(csv_folder_path)