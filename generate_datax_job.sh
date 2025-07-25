#!/bin/bash

# 参数解析
while [[ $# -gt 0 ]]; do
    case "$1" in
        --db) db_name="$2"; shift ;;
        --table) table_name="$2"; shift ;;
         --output) output_file="$2"; shift ;;
        *) echo "未知参数: $1"; exit 1 ;;
    esac
    shift
done

# 必要参数检查
required_params=("db_name" "table_name")
for param in "${required_params[@]}"; do
    if [ -z "${!param}" ]; then
        echo "错误: 缺少必要参数 --${param//_/-}"
        exit 1
    fi
done

# 设置默认值
# mysql_port=${mysql_port:-3306}
output_file=${output_file:-"datax_job_${db_name}_${table_name}.json"}

# 模板文件路径
template_file="$(dirname "$0")/mysql_to_hdfs_template.json"

# 检查模板文件
if [ ! -f "$template_file" ]; then
    echo "错误: 模板文件 $template_file 不存在"
    exit 1
fi
echo $template_file
# 生成配置文件
sed \
    -e "s/\${db_name}/$db_name/g" \
    -e "s/\${table_name}/$table_name/g" \
    "$template_file" > "$output_file"

echo "配置文件已生成: $output_file"
echo "执行命令: python datax.py $output_file"
