# DB Provisioner and Credentials Extractor

This utility can help provision a MySql database and provide a way to extract the database credentials and DSN by using the `auth` component of 
XL.

In order to work correctly it needs to have `auth` component up and running.

All the pyton scripts are in `/util` folder. You can run `provision_db.py` to provision a 
mysql database and `db.py creds` or `db.py dsn`, to get credentials or the dsn.

To run the scripts, there are a set of prerequisite steps.

## 1. Download necessary python libraries

All necessary libraries are enumerated in `requirements.txt` file.

Run: ```pip install -r requirements.txt ```

## 2. Setting Up The Environment

Some configuration files and/or environment variables are necessary in order to be able to run 
the python scripts. The following section will enumerate, for each python file, what are the 
prerequisites on order to be able to run the respective script.

## <db.py>

This script returns a DSN or CREDS depending on the argument used to run it [ dsn | creds ] and 


### Usage
> db.py [ dsn | creds ]
 
> export DSN=`/util/db.py dsn` 
> export CREDS=`/util/db.py creds`

### Purpose

When `dsn` argument is provided the script returns the DSN link to help with DB connection.
When `creds` argument is provided the script returns the credentials as `username:password`

### Prerequisites

* `DB_PATHS_CONFIG` : an environment variable that specify the location of `db.ini` configuration 
file.

* `db.ini` file (or environment variables) is required to provide the type of database you 
connect to, the port number, the host, and the username.

An example of this file is:
```renderscript
[database]
host = <db_host>
name = <db_name>
port = 3306
type = mysql
user = <user_name>
endpoint_name = <endpoint_name>
component = <component_name>

[log]
mode = console
```

The environment variables are taking priority over the values in the db.ini file. You can skip 
this file if you define all the values as environment variables using the following pattern: 
`DATABASE_<key>`. For example:
```renderscript
export DATABASE_NAME=dbname
export DATABASE_HOST=<host_name>
export DATABASE_PORT-3306
export DATABASE_USER=<user_name>
export DATABASE_ENDPOINT_NAME=<endpoint_name>
export DATABASE_COMPONENT=<component_name>
```
## <config_props.py>

### Usage

The functions from this file are called in the `db.py` file

### Purpose

It provides a flattened view of properties present in the CR definitions available in t8c-operator

### Prerequisites

The following environment variables need to be specified for different functions to work as 
expected:

* `TURBO_CONFIG_PROPERTIES_FILE` : contains the mount path for the YAML file constructed by 
operator and mounted in the image. It is usually stored under `/etc/turbonomic/propertie.yaml`. 
  If you don't have access to that file you can create your own with the following format:
 ```renderscript
defaultProperties:
  global:
    authHost: auth
    serverHttpPort : 8080
```
* `DB_PATHS_CONFIG` - the path to the config file (`db.ini`) 

## <provision_db.py>

### Usage

```renderscript
while ! /util/provision_db.py; do
    echo "Waiting 10 seconds to retry provisioning"
    sleep 10
done
```
### Purpose

Provisions a database for a db's internal state.
Provisioning is required if it has not yet occurred for the configured server.

The supported type is MySQL server.

### Prerequisites

The following environment variable needs to be declared in advance.

* `DB_PATHS_CONFIG` - the path to the config file (`db.ini`)
