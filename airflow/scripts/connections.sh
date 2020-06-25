#!/usr/bin/env bash

# #create list of all previous/default connections
# declare -a arr=($(airflow connections --list))
# #for every connection in airflow connection list, delete connection
# for i in "${!arr[@]}"; do 
#     if [ "${arr[$i]}" == '├────────────────────────────────┼─────────────────────────────┼────────────────────────────┼────────┼────────────────┼──────────────────────┼────────────────────────────────┤' ]; then
#         conn_str=${arr["$(( $i + 2 ))"]}
#         # conn_str='${arr["$(( $i + 2 ))"]}'
        
#         echo "Deleting $conn_str"
#         airflow connections --delete --conn_id $conn_str
#     fi    
# done

#get aws config values
config() {
    val=$(grep -E "^$1=" /config/aws.cfg | cut -d '=' -f 2-)
    echo -n $val
}

# add AWS connection to airflow

airflow connections --add --conn_id $(config AWS_CONN_ID) --conn_type aws \
                    --conn_login $(config KEY) --conn_password $(config SECRET)