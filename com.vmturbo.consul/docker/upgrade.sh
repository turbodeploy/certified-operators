CONSUL_DATA_DIR=$1
CONSUL_CONFIG_DIR=$2

wait_for_consul () {
    echo $(date -u) "waiting for consul to start"

    # read arg of consul location or default to /bin/consul
    consul=${1:-/bin/consul}
    str="Error querying Consul"
    cmp="Error querying Consul"
    # perform a consul kv get
    # will continue to return 'error querying consul'
    # until consul cluster/agent is up and running
    while [[ "$cmp" == *"$str"* ]]; do
            cmp=$(.$consul kv get a 2>&1)
            sleep 1
    done

    echo $(date -u) "consul succesfully started up"
}

# start consul only accessible to native machine (by forcing to work in localhost)
# used for development/internal purposes
# function accepts arg represented as path to the binary consul
start_private_consul () {
    # read arg of consul location or default to /bin/consul
    consul=${1:-/bin/consul}
    shift
            exec $consul agent \
             -bootstrap-expect 1 \
             -config-dir="$CONSUL_CONFIG_DIR/server" \
             -config-dir="$CONSUL_CONFIG_DIR/local" \
             -data-dir="$CONSUL_DATA_DIR" \
             -bind=127.0.0.1 \
             "$@"
}

# remove data found in consul pv
clear_existing_consul_data () {
    echo $(date -u) "clearing existing data from older consul instance"
    if [ -d /consul/data/checks ]; then
        rm -rf /consul/data/checks/*
    fi

    if [ -d /consul/data/services ]; then
        rm -rf /consul/data/services/*
    fi

    if [ -d /consul/data/serf ]; then
        rm -rf /consul/data/serf/*
    fi

    if [ -d /consul/data/raft ]; then
        rm -rf /consul/data/raft/*
    fi
}

# 0.8.3 consul binary path
old_consul_binary="/consul/data/consul-0.8.3"

# kv export path
kv_store="/consul/data/kv_store.json"

echo $(date -u) "combining consul binary."
cat /consul.part* > $old_consul_binary
chmod 755 $old_consul_binary

echo $(date -u) "updating consul binding address to local loopback IP (127.0.0.1) to avoid external
connection."
###hack
# To handle changing host IP's always set up to set the 'peers' list so election will succeed
if [ -d /consul/data/raft ]
then
  echo  "[\"127.0.0.1:8300\"]" > /consul/data/raft/peers.json
else
  mkdir /consul/data/raft
fi
###/hack

# start consul
echo $(date -u) "starting private consul for upgrade."
start_private_consul $old_consul_binary &

# get pid of bg process (consul)
consul_pid=$!

# wait for consul to come up
wait_for_consul $old_consul_binary

# export kv store
echo $(date -u) "export consul kv store to ${kv_store}"
.$old_consul_binary "kv" "export" >> "${kv_store}"

# gracefully exit consul
echo $(date -u) "exiting private consul instance"
kill -2 $consul_pid
while kill -0 $consul_pid; do
    sleep 1
done

echo $(date -u) "delete old consul binary"
rm $old_consul_binary

# create backup
current_time=$(date "+%Y.%m.%d-%H.%M.%S")
cp -r /consul/data /consul/data/bak.$current_time

# remove existing data found in pv
clear_existing_consul_data

# start another private consul instance, this time the newer version
echo $(date -u) "starting private consul instance to import kv store"
start_private_consul &

# get pid of bg process (consul)
consul_pid=$!

    # wait for consul to come up
wait_for_consul

# import kv store
echo $(date -u) "import consul kv store from $kv_store"
/bin/consul "kv" "import" "@${kv_store}"

# gracefully exit consul
echo $(date -u) "exiting private consul instance"
kill -2 $consul_pid
while kill -0 $consul_pid; do
    sleep 1
done
