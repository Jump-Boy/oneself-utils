#!/usr/bin/env bash

# 该脚本用来将指定insert values sql格式数据格式化为csv数据，以','作为分隔符。'\n'换行。

# 预处理sql数据脚本路径（最终结果与该路径目录同级）
target_sql=$1
sql_name=$(basename ${target_sql})
result_file=$(dirname ${target_sql})/${sql_name}-insert_sql_value.txt
all_data_count=0

function format_one_insert_value() {

    # 处理末尾的 ;
    local value=$1 \
    && value=${value#*VALUES \(} \
    && length_1=$(( ${#value} - 1 )) \
    && value=${value: 0: length_1} \
    && value="${value},"

    if [ $? != 0 ]
    then
        echo '数据初始化失败了，请检查预格式化的前置sql脚本数据'
        return 1;
    fi

    # 剩余数据
    local remaining_datas=${value}
    # 最终处理完的结果数据文件路径

    local prev_remaining_datas_len=0
    local data_count=0
    while(( ${#remaining_datas} != ${prev_remaining_datas_len} ))
    do
        # 为了避免数据中含有'('或')'，所以下面判断使用',('和'),'处理。
        # s),
        # s),(s),(s),    s),(s),    s),

        prev_remaining_datas_len=${#remaining_datas}
        # 从剩余字符串中截取第一次出现'),'左边所有字符串，得到内容为一条完整数据记录。
        data=${remaining_datas%%\)\,*}
        echo ${data} >> ${result_file}
        data_count=$(( ${data_count} + 1 ))
        echo "第${data_count}次完成"
        echo "remaining_datas长度 ${#remaining_datas}，data长度 ${#data}"

        # 截取剩余',('第一次出现至结尾的字符串，判断是否还有，若无 -1，则结束，否则继续
        remaining_datas=${remaining_datas#*\,\(}

    done

    echo "本轮处理数据 ${data_count} 条"
    all_all_data_count=$(( ${all_all_data_count} + ${data_count} ))

}


sh_result=0

# todo 文件格式检查

table_name=${sql_name%.sql*}
values_num=($(grep -n "INSERT INTO \`${table_name}\` VALUES" ${target_sql} | awk -F ':' '{print $1}'))
index=1
for value_num in ${values_num[*]}
do
    echo "第${index}轮insert values格式化数据开始 >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"

    value=$(awk "{ if(NR == ${value_num}){print \$0;} }" ${target_sql})
    format_one_insert_value "${value}"
    index=$(( ${index} + 1 ))
    fun_code=$?
    sh_result=$(( ${sh_result} | ${fun_code} ))
done

echo "本脚本共处理数据 ${all_data_count} 条"
exit ${sh_result}