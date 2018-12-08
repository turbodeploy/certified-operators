#!/bin/bash

# The script will take yaml configuration as argument and adjust all memory configuration into k8s environment.
source /opt/local/etc/turbo.conf
K8S_DIR="/opt/turbonomic/kubernetes"
YAML_DIR="${K8S_DIR}/yaml"
BACKUP_YAML_DIR="${K8S_DIR}/yaml_backup"
echo "Running `basename $0` for ${turboEnv} environment"
# Usage function
usage()
{
        echo "##############################################################################
usage:
`basename $0` <pod_config.yml>      -> Adjust pod configuration based on specified yaml.
############################################################################################"
        exit -1
}

[ -z "${1}" ] && usage
# Creating backup of current pod yamls
if [[ -d ${BACKUP_YAML_DIR} ]]
then
    rm -r ${YAML_DIR}
    cp -r ${BACKUP_YAML_DIR} ${YAML_DIR}
else
    cp -r ${YAML_DIR} ${BACKUP_YAML_DIR}
fi
function parse_yaml() {
    #############################################################
    # Parse yaml file and remove all spaces, colons.
    # Then collect all keys and add underscore(_) in between keys.
    # So that we can have unique keys to get appropriate values
    # Arguments:
    #   <yaml_file>
    # Returns:
    #   formatted key with values
    #   example:     development_base_api_j_option=("-Xmx512M")
    #############################################################
    local yaml_file=$1
    local s
    local w
    local fs

    # [[:space:]] matches any space, tab, newline, or carriage return
    s='[[:space:]]*'
    # matches words with alphabet, numbers or -,.,_
    w='[a-zA-Z0-9_.-]*'
    # An ASCII File Separator character and removing any double-quotes surrounding the value field
    fs="$(echo @|tr @ '\034')"
    (
        sed -ne '/^--/s|--||g; s|\"|\\\"|g; s/\s*$//g;' \
            -e "/#.*[\"\']/!s| #.*||g; /^#/s|#.*||g;" \
            -e  "s|^\($s\)\($w\)$s:$s\"\(.*\)\"$s\$|\1$fs\2$fs\3|p" \
            -e "s|^\($s\)\($w\)$s[:-]$s\(.*\)$s\$|\1$fs\2$fs\3|p" |

        awk -F"$fs" '{
            indent = length($1)/2;
            if (length($2) == 0) { conj[indent]="+";} else {conj[indent]="";}
            vname[indent] = $2;
            for (i in vname) {if (i > indent) {delete vname[i]}}
                if (length($3) > 0) {
                    vn=""; for (i=0; i<indent; i++) {vn=(vn)(vname[i])("_")}
                    printf("%s%s%s=(\"%s\")\n",vn, $2, conj[indent-1],$3);
                }
            }' |

        sed -e 's/_=/+=/g' |
        awk 'BEGIN {
                 FS="=";
                 OFS="="
             }
             /(-|\.).*=/ {
                 gsub("-|\\.", "_", $1)
             }
             { print }'

    ) < "$yaml_file"
}

function create_variables() {
    #############################################################
    # Create variables based on key and values from yaml
    #  example variable: development_base_api_j_option=("-Xmx512M")
    # Arguments:
    #   <yaml_file>
    # Returns:
    #   None
    #############################################################
    local yaml_file="$1"
    eval "$(parse_yaml "$yaml_file")"
}

create_variables $1

# Now let's take all pod yaml files and update
#    JAVA_OPTIONS, MEMORY_REQUEST and MEMORY_LIMIT information
#    based on specified configuration
for dir in `ls ${YAML_DIR}`
do
  if [[ -d "${YAML_DIR}/${dir}" ]]
  then
    for component_full_name in `ls ${YAML_DIR}/${dir}/`
    do
        if [ "${turboEnv}" == "dev" ]
        then
            component=$(echo $component_full_name | cut -d '.' -f1)
            j_option=$(eval "echo \$dev_${dir//-/_}_${component//-/_}_java_option")
            [[ ! -z ${j_option} ]] &&  sed -i.bak "s/JAVA_OPTIONS/${j_option}/g" $YAML_DIR/$dir/$component_full_name
            requests_memory=$(eval "echo \$dev_${dir//-/_}_${component//-/_}_requests_memory")
            [[ ! -z ${requests_memory} ]] &&  sed -i.bak "s/MEMORY_REQUEST/${requests_memory}/g" $YAML_DIR/$dir/$component_full_name
            limits_memory=$(eval "echo \$dev_${dir//-/_}_${component//-/_}_limits_memory")
            [[ ! -z ${limits_memory} ]] &&  sed -i.bak "s/MEMORY_LIMIT/${limits_memory}/g" $YAML_DIR/$dir/$component_full_name
        fi
        if [ "${turboEnv}" == "prod" ]
        then
            component=$(echo $component_full_name | cut -d '.' -f1)
            j_option=$(eval "echo \$prod_${dir//-/_}_${component//-/_}_java_option")
            [[ ! -z ${j_option} ]] &&  sed -i.bak "s/JAVA_OPTIONS/${j_option}/g" $YAML_DIR/$dir/$component_full_name
            requests_memory=$(eval "echo \$prod_${dir//-/_}_${component//-/_}_requests_memory")
            [[ ! -z ${requests_memory} ]] &&  sed -i.bak "s/MEMORY_REQUEST/${requests_memory}/g" $YAML_DIR/$dir/$component_full_name
            limits_memory=$(eval "echo \$prod_${dir//-/_}_${component//-/_}_limits_memory")
            [[ ! -z ${limits_memory} ]] &&  sed -i.bak "s/MEMORY_LIMIT/${limits_memory}/g" $YAML_DIR/$dir/$component_full_name
        fi
    done
  fi
done
