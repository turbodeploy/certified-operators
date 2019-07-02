#!/bin/bash

# Library functions to be used in other scripts.

log_msg () {
    local msg=${@};
    echo "$msg"
    echo $msg | logger --tag mariadb-setup
}

