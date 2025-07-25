import re
import argparse
from datetime import datetime

def parse_table_definition(table_def):
    # 提取表名
    table_name_match = re.search(r'CREATE TABLE `(\w+)`', table_def)
    if not table_name_match:
        return None
    table_name = table_name_match.group(1)
    
    # 提取表注释
    table_comment_match = re.search(r"COMMENT\s*=\s*'([^']+)'", table_def)
    table_comment = table_comment_match.group(1) if table_comment_match else f"{table_name} table"
    
    # 提取字段定义部分
    columns_section_match = re.search(r'\((.*?)\)[^)]*?ENGINE', table_def, re.DOTALL | re.IGNORECASE)
    if not columns_section_match:
        return None
    columns_section = columns_section_match.group(1).strip()
    
    # 分割字段定义
    column_defs = []
    current = ""
    paren_depth = 0
    for char in columns_section:
        if char == '(':
            paren_depth += 1
        elif char == ')':
            paren_depth -= 1
        
        if char == ',' and paren_depth == 0:
            column_defs.append(current.strip())
            current = ""
        else:
            current += char
    
    if current.strip():
        column_defs.append(current.strip())
    
    # 处理每个字段定义
    fields = []
    for col_def in column_defs:
        # 跳过约束定义（PRIMARY KEY, UNIQUE KEY等）
        if re.search(r'PRIMARY KEY|UNIQUE KEY|KEY|CONSTRAINT', col_def, re.IGNORECASE):
            continue
        
        # 提取字段名
        field_name_match = re.search(r'`(\w+)`', col_def)
        if not field_name_match:
            continue
        field_name = field_name_match.group(1)
        
        # 提取字段类型
        type_match = re.search(r'`\w+`\s+([\w()\d\s,]+)', col_def)
        if not type_match:
            continue
        raw_type = type_match.group(1).strip()
        
        # 提取注释
        comment_match = re.search(r"COMMENT\s+'([^']+)'", col_def)
        comment = comment_match.group(1) if comment_match else ""
        
        # 特殊处理ID字段注释
        if field_name == "id" and "AUTO_INCREMENT" in col_def and not comment:
            comment = "主键ID"
        
        # 映射到Hive类型
        hive_type = map_to_hive_type(raw_type)
        
        # 构建字段定义
        field_def = f"`{field_name}` {hive_type}"
        if comment:
            field_def += f" COMMENT '{comment}'"
        
        fields.append(field_def)
    
    return {
        "table_name": table_name,
        "table_comment": table_comment,
        "fields": fields
    }

def map_to_hive_type(mysql_type):
    # 标准化类型字符串
    mysql_type = mysql_type.lower().strip()
    
    # 处理带精度的类型
    if '(' in mysql_type:
        base_type = mysql_type.split('(')[0].strip()
        precision = mysql_type.split('(')[1].split(')')[0].strip()
    else:
        base_type = mysql_type
        precision = ""
    
    # 类型映射
    type_mapping = {
        'tinyint': 'tinyint',
        'smallint': 'smallint',
        'int': 'int',
        'bigint': 'bigint',
        'float': 'float',
        'double': 'double',
        'decimal': f'decimal({precision})' if precision else 'decimal',
        'char': 'string',
        'varchar': 'string',
        'text': 'string',
        'longtext': 'string',
        'tinytext': 'string',
        'date': 'string',
        'datetime': 'string',
        'timestamp': 'string',
        'bit': 'tinyint',
        'boolean': 'tinyint'
    }
    
    return type_mapping.get(base_type, 'string')

def mysql_to_hive_ddl(mysql_ddl,db_name):
    # 分割所有表定义
    table_defs = re.split(r';(?:\s*\n)?', mysql_ddl)
    
    hive_ddls = []
    
    for table_def in table_defs:
        if "CREATE TABLE" not in table_def:
            continue
        
        table_info = parse_table_definition(table_def)
        if not table_info:
            continue
        
        # 构建Hive DDL
        hive_ddl = f"CREATE EXTERNAL TABLE if not exists `ods.{db_name}_{table_info['table_name']}`(\n"
        hive_ddl += ",\n".join([f"  {f}" for f in table_info['fields']])
        hive_ddl += f"\n)\nCOMMENT '{table_info['table_comment']}'\n"
        hive_ddl += "PARTITIONED BY (`dt` string COMMENT '分区日期(格式YYYYMMDD)')\n"
        hive_ddl += "ROW FORMAT DELIMITED\n"
        hive_ddl += "  FIELDS TERMINATED BY '\\t'\n"
        hive_ddl += "  STORED AS TEXTFILE\n"
        hive_ddl += f"LOCATION '/{db_name}/{table_info['table_name']}';\n"
        
        hive_ddls.append(hive_ddl)
    
    return "\n\n".join(hive_ddls)

def main():
     # 创建命令行参数解析器
    parser = argparse.ArgumentParser(description='Convert MySQL DDL to Hive DDL')
    parser.add_argument('-db', '--database', type=str, default='ods', help='source database name')
    args = parser.parse_args() 
    
    # 读取MySQL DDL文件
    try:
        with open('mysql_ddl.sql', 'r', encoding='utf-8') as f:
            mysql_ddl = f.read()
    except FileNotFoundError:
        print(f"Error: Input file 'mysql_ddl.sql' not found.")
        return
    except Exception as e:
        print(f"Error reading input file: {e}")
        return
    
    # 转换DDL
    try:
        hive_ddl = mysql_to_hive_ddl(mysql_ddl, args.database)
    except Exception as e:
        print(f"Error converting DDL: {e}")
        return 
    
    # 保存Hive DDL文件
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    output_file = f'hive_ddl_{args.database}_{timestamp}.sql'
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("-- Generated Hive DDL\n")
        f.write(f"-- Conversion time: {datetime.now()}\n")
        f.write("-- Source: MySQL DDL\n\n")
        f.write(hive_ddl)
    
    print(f"Conversion completed! Hive DDL saved to {output_file}")

if __name__ == "__main__":
    main()
