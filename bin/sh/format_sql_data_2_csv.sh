#!/usr/bin/env bash

# 该脚本用来将指定insert values sql格式数据格式化为csv数据，以','作为分隔符。'\n'换行。

# 预处理sql数据脚本路径（最终结果与该路径目录同级）
target_sql=$1
sql_name=$(basename ${target_sql})
result_file=$(dirname ${target_sql})/${sql_name}-values.csv
job_log=$(dirname ${target_sql})/${sql_name}-format_job.log
all_data_count=0


# 参数1：预处理的（insert）values；
# 参数2：任务号（数值id）
function format_one_insert_value() {

    # 处理末尾的 ;
    local value=$1 \
    && value=${value#*VALUES \(} \
    && length_1=$(( ${#value} - 1 )) \
    && value=${value: 0: length_1} \
    && value="${value},"

    if [ $? != 0 ]
    then
        echo '数据初始化失败了，请检查预格式化的前置sql脚本数据' >> ${job_log}
        return 1;
    fi

    # 剩余数据
    local remaining_datas=${value}
    # 最终处理完的结果数据文件路径

    local job_id=$2
    local prev_remaining_datas_len=0
    local data_count=0
    local tmp_result_file=$(dirname ${target_sql})/${sql_name}-value_${job_id}.csv
    while(( ${#remaining_datas} != ${prev_remaining_datas_len} ))
    do
        # 为了避免数据中含有'('或')'，所以下面判断使用',('和'),'处理。
        # s),
        # s),(s),(s),    s),(s),    s),

        prev_remaining_datas_len=${#remaining_datas}
        # 从剩余字符串中截取第一次出现'),'左边所有字符串，得到内容为一条完整数据记录。
        data=${remaining_datas%%\)\,*}
        echo ${data} >> ${tmp_result_file}
        data_count=$(( ${data_count} + 1 ))
        # debug log（可注释！）
        echo "job_id: ${job_id}, 第${data_count}次完成" >> ${job_log}
        echo "job_id: ${job_id}, remaining_datas长度 ${#remaining_datas}，data长度 ${#data}" >> ${job_log}

        # 截取剩余',('第一次出现至结尾的字符串，判断是否还有，若无 -1，则结束，否则继续
        remaining_datas=${remaining_datas#*\,\(}

    done

    echo "$(date '+%Y-%m-%d %H:%M:%S') 本轮处理数据 ${data_count} 条" >> ${job_log}
    all_data_count=$(( ${all_data_count} + ${data_count} ))

}


sh_result=0
# 文件格式安全性检查
if [[ ${sql_name##*.} != 'sql' ]]
then
    echo '预格式化处理文件不是.sql格式！考虑到数据完整性，建议预处理文件是由mysql直接导出而来，若确保文件内容有效且数据完整，则可将文件后缀改为.sql'
    exit 1
fi

echo "$(date '+%Y-%m-%d %H:%M:%S') 任务准备启动 >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
table_name=${sql_name%.sql*}
values_num=($(grep -n "INSERT INTO \`${table_name}\` VALUES" ${target_sql} | awk -F ':' '{print $1}'))
index=1
for value_num in ${values_num[*]}
do
    echo "$(date '+%Y-%m-%d %H:%M:%S') 第${index}轮insert values格式化数据开始 >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" >> ${job_log}

    value=$(awk "{ if(NR == ${value_num}){print \$0;} }" ${target_sql})
    format_one_insert_value "${value}" ${value_num} &
    fun_code=$?
    sh_result=$(( ${sh_result} | ${fun_code} ))
    index=$(( ${index} + 1 ))
done

# 等所有values处理完成后，合并所有格式化后的结果csv为一个完整的csv
echo "$(date '+%Y-%m-%d %H:%M:%S') values数据格式化任务已遍历启动完成，等待后台执行完成中"
wait
echo "$(date '+%Y-%m-%d %H:%M:%S') values数据格式化任务已全部完成 <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<"
cat $(dirname ${target_sql})/${sql_name}-value_*.csv > ${result_file} && rm -f $(dirname ${target_sql})/${sql_name}-value_*.csv


echo "本次脚本运行共处理数据 ${all_data_count} 条" >> ${job_log}
echo "脚本执行完成，运行日志请看 ${job_log}"
exit ${sh_result}