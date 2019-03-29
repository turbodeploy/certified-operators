#!/bin/bash -e

ADDR='=en0'
PORT=22
USER="$LOGNAME"
KEYFILE="$HOME/.ssh/id_rsa"
MANPATH="$HOME/actionscript/manifest.yaml"
HOSTKEY='*'
TURBOADDR=localhost
TURBOUSER=administrator
TURBOPASS="${TURBOPASS-administrator}"

function getArgs() {
    local opts="a:p:u:k:m:K:T:U:P:h"
    while getopts $opts o 2>/dev/null; do
	case "$o" in
	    a) ADDR="$OPTARG" ;;
	    p) PORT="$OPTARG" ;;
	    u) USER="$OPTARG" ;;
	    k) KEYFILE="$OPTARG" ;;
	    m) MANPATH="$OPTARG" ;;
	    K) HOSTKEY="$OPTARG" ;;
	    T) TURBOADDR="$OPTARG" ;;
	    U) TURBOUSER="$OPTARG" ;;
	    P) TURBOPASS="$OPTARG" ;;
	    h) HELP=true ;;
	    *)
		local badPos=$((OPTIND - 1))
		error Invalid arg: "${!badPos}" ;;
	esac
    done
    if [[ ! $HELP ]] ; then
	if [[ ! $ADDR ]] ; then error Must provide  target address; fi
	if [[ ! $USER ]] ; then error Must provide  target user id; fi
	if [[ ! $KEYFILE ]] ; then error Must provide  target key file; fi
	if [[ ! -f $KEYFILE ]] ; then error "Key file $KEYFILE does not exist"; fi
	if [[ ! $MANPATH ]] ; then error Must provide  manifest path; fi
	if [[ ! $TURBOADDR ]] ; then error Must provide  turbo host name/addr; fi
	if [[ ! $TURBOUSER ]] ; then error Must provide  turbo user id; fi
	if [[ ! $TURBOPASS ]] ; then error Must provide  turbo password; fi
	true
    fi
    if [[ $ERROR ]] ; then return 1 ; fi
    if [[ $ADDR == =* ]] ; then
	ADDR=$(ipconfig getifaddr ${ADDR#=})
    fi
}

function error() {
    echo "$@" >&2
    ERROR=true
}

function usage() {
    cat <<EOF | sed -e 's/^[ 	]*[|] \{0,1\}//' # that's a literal tab in there - stupid mac sed
    	| SYNOPSIS: Create an action script target in a running XL appliance
	|
    	| USAGE: $0 [options...]
	| Recognized options:
	|  -a address - supply host or ip addr of execution server
	|  -p port - supply SSH port on execution server
	|  -u user - supply userid for execution server
	|  -k keyfile - path to private key file for authenticating to execution server
	|  -m manifest - absolute path to manifest file on execution server
	|  -T turbo - host or IP where turbo XL appliance is running
	|  -U turbo user - user id for login to turbo
	|  -P turbo pass - password for login to turbo
	|
	| All options have defaults:
	|  -a the ip address of your 'en0' network interface. In general you can say
	|     =<interface> to use the ip address of the specified interface, so the
	|     default is like saying "-a =en0"
	|  -p 22
	|  -u the value of \$LOGNAME - generally your own user id
	|  -k \$HOME/.ssh/id_rsa - a typical location of a working key file
	|  -m \$HOME/actionscript/manifest.yaml
	|  -T localhost
	|  -U administrator
	|  -P the value of \$TURBOPASS, or "administrator" if that's not set
	|
	| The script will make two API requests to the turbo appliance: one to log in,
	| and another to create the target. A temporary cookie jar is established in
	| /tmp/.cookies, and it is deleted when the script finishes.
	| JSON output of both API calls are pretty-printed by the script.

EOF
}

function main() {
    local pkey=$(awk 'ORS="\\n"' "$KEYFILE")
    local cookies=/tmp/.cookies
    rm -rf $cookies
    local login="https://${TURBOADDR}/vmturbo/rest/login"
    echo Login response:
    curl -s -k -K POST -d "username=${TURBOUSER}&password=${TURBOPASS}" -c $cookies $login \
    	 | python -m json.tool
    local url="https://$TURBOADDR/vmturbo/rest/targets"
    local payload="$(getRequest "$pkey")"
    echo Target creation response:
    curl -s -k -K POST -d "$payload" -H 'Content-Type: application/json' -b $cookies "$url" \
	| python -m json.tool
    rm -rf $cookies
}

function getRequest() {
    local inputFields=$(mkarray \
			    "$(mkobject name '"nameOrAddress"' value '"'"$ADDR"'"')" \
			    "$(mkobject name '"port"' value $PORT)" \
			    "$(mkobject name '"manifestPath"' value '"'"$MANPATH"'"')" \
			    "$(mkobject name '"userid"' value '"'"$USER"'"')" \
			    "$(mkobject name '"privateKeyString"' value '"'"$1"'"')" \
			)
    echo "$(mkobject category '"Orchestrator"' type '"Action Script"' inputFields "$inputFields")"
}

function mkobject() {
    local obj=()
    for i in $(seq 1 2 $(( $# - 1 ))) ; do
	local iplus1=$((i + 1))
	objs+=('"'"${!i}"'": '"${!iplus1}")
    done
    local IFS=","
    echo "{${objs[*]}}"
}

function mkarray() {
    local IFS=","
    echo "[$*]"
}

if getArgs "$@"; then
    if [[ $HELP ]] ; then
	usage
    else
	main
    fi
else
    usage
    exit 1
fi
