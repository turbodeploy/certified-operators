#!/bin/bash

function start_consul() {
    TARGET_NODE="$1"
    docker service create --replicas 1 --name consul --network turbosecure --constraint "node.hostname == ${TARGET_NODE}" \
    -e 'CONSUL_BIND_INTERFACE=eth0' \
    -e 'constraint:node==/node-4/' \
    -e 'consul.standalone=true' \
    --mount type=volume,src=consuldata,dst=/consul/data,volume-driver=local \
    --restart-condition on-failure \
    turbonomic/com.vmturbo.consul:latest vmt-server -client=0.0.0.0
}

function start_db() {
    TARGET_NODE="$1"
    docker service create --replicas 1 --name db --network turbosecure --constraint "node.hostname == ${TARGET_NODE}" \
    --mount type=volume,src=mysqldata,dst=/home/mysql/var/lib/mysql,volume-driver=local \
    --mount type=volume,src=mysql_log,dst=/home/mysql/var/log,volume-driver=local \
    --mount type=volume,src=mysql_var,dst=/home/mysql/var/run,volume-driver=local \
    --mount type=volume,src=mysql_tmp,dst=/home/mysql/tmp,volume-driver=local \
    turbonomic/db:latest
}

function start_auth() {
    TARGET_NODE="$1"
    docker service create --replicas 1 --name auth --network turbosecure --constraint "node.hostname == ${TARGET_NODE}" \
    -e 'component_type=auth' \
    -e 'instance_id=auth-1' \
    -e 'consul_host=consul' \
    --mount type=volume,src=auth,dst=/home/turbonomic/data,volume-driver=local \
    turbonomic/auth:latest
}

function start_clustermgr() {
    TARGET_NODE="$1"
    docker service create --replicas 1 --name clustermgr --network turbosecure --constraint "node.hostname == ${TARGET_NODE}" \
    -e 'consul_host=consul' \
    --mount type=volume,src=clustermgr,dst=/home/turbonomic/data,volume-driver=local \
    turbonomic/com.vmturbo.clustermgr:latest
}

function start_api() {
    TARGET_NODE="$1"
    docker service create --replicas 1 --name api --network turbosecure --constraint "node.hostname == ${TARGET_NODE}" \
    -e 'component_type=api' \
    -e 'instance_id=api-1' \
    -e 'consul_host=consul' \
    -e 'realtimeTopologyContextId=777777' \
    -p "80:8080" \
    -p "443:9443" \
    --mount type=volume,src=api,dst=/home/turbonomic/data,volume-driver=local \
    turbonomic/com.vmturbo.api.component:latest
}

function start_market() {
    TARGET_NODE="$1"
    docker service create --replicas 1 --name market --network turbosecure --constraint "node.hostname == ${TARGET_NODE}" \
    -e 'component_type=market' \
    -e 'instance_id=market-1' \
    -e 'consul_host=consul' \
    -e 'realtimeTopologyContextId=777777' \
    --mount type=volume,src=market,dst=/home/turbonomic/data,volume-driver=local \
    turbonomic/market-component:latest
}

function start_action-orchestrator() {
    TARGET_NODE="$1"
    docker service create --replicas 1 --name action-orchestrator --network turbosecure --constraint "node.hostname == ${TARGET_NODE}" \
    -e 'component_type=action-orchestrator' \
    -e 'instance_id=action-orchestrator-1' \
    -e 'consul_host=consul' \
    -e 'realtimeTopologyContextId=777777' \
    --mount type=volume,src=action-orchestrator,dst=/home/turbonomic/data,volume-driver=local \
    turbonomic/action-orchestrator:latest
}

function start_topology-processor() {
    TARGET_NODE="$1"
    docker service create --replicas 1 --name topology-processor --network turbosecure --constraint "node.hostname == ${TARGET_NODE}" \
    -e 'component_type=topology-processor' \
    -e 'instance_id=topology-processor-1' \
    -e 'consul_host=consul' \
    -e 'realtimeTopologyContextId=777777' \
    --mount type=volume,src=topology-processor,dst=/home/turbonomic/data,volume-driver=local \
    turbonomic/topology-processor:latest
}

function start_arangodb() {
    TARGET_NODE="$1"
    docker service create --replicas 1 --name arangodb --network turbosecure --constraint "node.hostname == ${TARGET_NODE}" \
    -e 'ARANGO_ROOT_PASSWORD=root' \
    --mount type=volume,src=arangodb,dst=/var/lib/arangodb3,volume-driver=local \
    --mount type=volume,src=arangodb-apps,dst=/var/lib/arangodb3-apps,volume-driver=local \
    --mount type=volume,src=arangodb-dump,dst=/home/arangodb/arangodb-dump,volume-driver=local \
    turbonomic/arangodb:latest
}

function start_repository() {
    TARGET_NODE="$1"
    docker service create --replicas 1 --name repository --network turbosecure --constraint "node.hostname == ${TARGET_NODE}" \
    -e 'consul_host=consul' \
    -e 'component_type=repository' \
    -e 'instance_id=repository-1' \
    -e 'REPOSITORY_ARANGODB_HOST=arangodb' \
    --mount type=volume,src=repository,dst=/home/turbonomic/data,volume-driver=local \
    turbonomic/com.vmturbo.repository-component:latest
}

function start_group() {
    TARGET_NODE="$1"
    docker service create --replicas 1 --name group --network turbosecure --constraint "node.hostname == ${TARGET_NODE}" \
    -e 'consul_host=consul' \
    -e 'realtimeTopologyContextId=777777' \
    -e 'component_type=group' \
    -e 'instance_id=group-1' \
    --mount type=volume,src=group,dst=/home/turbonomic/data,volume-driver=local \
    turbonomic/com.vmturbo.group:latest
}

function start_history() {
    TARGET_NODE="$1"
    docker service create --replicas 1 --name history --network turbosecure --constraint "node.hostname == ${TARGET_NODE}" \
    -e 'consul_host=consul' \
    -e 'component_type=history' \
    -e 'instance_id=history-1' \
    -e 'realtimeTopologyContextId=777777' \
    --mount type=volume,src=history,dst=/home/turbonomic/data,volume-driver=local \
    turbonomic/com.vmturbo.history:latest
}

function start_plan-orchestrator() {
    TARGET_NODE="$1"
    docker service create --replicas 1 --name plan-orchestrator --network turbosecure --constraint "node.hostname == ${TARGET_NODE}" \
    -e 'component_type=plan-orchestrator' \
    -e 'instance_id=plan-orchestrator-1' \
    -e 'consul_host=consul' \
    -e 'realtimeTopologyContextId=777777' \
    --mount type=volume,src=plan-orchestrator,dst=/home/turbonomic/data,volume-driver=local \
    turbonomic/com.vmturbo.plan.orchestrator:latest
}

function start_mediation-vcenter() {
    TARGET_NODE="$1"
    docker service create --replicas 1 --name mediation-vcenter --network turbosecure --constraint "node.hostname == ${TARGET_NODE}" \
    -e 'component_type=mediation-vcenter' \
    -e 'instance_id=mediation-vcenter-1' \
    -e 'consul_host=consul' \
    --mount type=volume,src=mediation-vcenter,dst=/home/turbonomic/data,volume-driver=local \
    turbonomic/com.vmturbo.mediation.vcenter.component:latest
}

function start_mediation-udt() {
    TARGET_NODE="$1"
    docker service create --replicas 1 --name mediation-udt --network turbosecure --constraint "node.hostname == ${TARGET_NODE}" \
    -e 'component_type=mediation-udt' \
    -e 'instance_id=mediation-udt-1' \
    -e 'consul_host=consul' \
    --mount type=volume,src=mediation-udt,dst=/home/turbonomic/data,volume-driver=local \
    turbonomic/com.vmturbo.mediation.udt.component:latest
}

# Starts the XL component and waits for it to fully initialize
function start_xl_component() {
    COMPONENT="$1"
    NODE="$2"
    MESSAGE="$3"
    echo "$(date): Starting $COMPONENT"
    CMD="start_${COMPONENT}"
    # Execute the command
    ${CMD} ${NODE} >/dev/null 2>&1
    if [[ $? == 1 ]]; then
        echo "${COMPONENT} has already started"
        return 0
    fi
    # See if we can tail that.
    if [ "$NODE" == "turbonomic" ] && [ ! -z "$MESSAGE" ]; then
        while true
        do
            docker logs $(docker ps -lq) | grep -q "${MESSAGE}"
            if [[ $? == 0 ]]; then
                break
            fi
            sleep 5
        done
    fi
}

# We store the images for worker node
function save_images_for_worker() {
    IMAGES=(db arangodb repository api history group)
    SRC="$1"
    DST="$2"
    mkdir "${DST}"
    for img in ${IMAGES[@]}
    do
        cp "${SRC}/${img}.tgz" "${DST}/${img}.tgz"
    done
}

# Starts the XL.
function start_all_xl() {
    # Clean up all services
    docker service ls -q | xargs docker service rm
    while true
    do
        NUM=$(docker ps -q | wc -l)
        if [ $NUM -eq 0 ]; then
            break
        fi
        echo "Remaining services ${NUM}"
        sleep 10
    done

    # Start primary and secondary
    start_xl_component consul "turbonomic" "New leader elected"
    start_xl_component clustermgr "turbonomic" "Started ClusterMgrMain"
    # We need DB here, so that AUTH doesn't complain much.
    start_xl_component db "worker1"

    start_xl_component auth "turbonomic"
    start_xl_component topology-processor "turbonomic"
    start_xl_component market "turbonomic"
    start_xl_component mediation-vcenter "turbonomic"
    start_xl_component plan-orchestrator "turbonomic"
    start_xl_component action-orchestrator "turbonomic"

    start_xl_component arangodb "worker1"
    start_xl_component repository "worker1"
    start_xl_component api "worker1"
    start_xl_component history "worker1"
    start_xl_component group "worker1"
}

