# Using an External Timescale Database with Embedded Reporting

The features described here are supported beginning in Turbonomic v8.1.4.

If you wish to manage the TimescaleDB database server that will house data for
Turbonomic's Embedded Reporting feature, you can do so fairly easily, by editing
a few of the properties in your CR (Custom Resources) file. This file will be
located at `~/kubernetes/operator/deploy/crds/charts_v1alpha1_xl_cr.yaml`.

### A Bit About YAML Files

> Note: There are a few rules you must bear in mind whenever editing a YAML
> file such as this one:
> * Always uses spaces, not tabs, for all indentation. If your editor of
>   choice makes this difficult, you can use the linux `expand` utility
>   when you're done, to convert tabs to equivalent spaces.
> * Be careful to keep the same indentation for all properties in a given
>   section.
> * Never use the same property name twice in the same section. Doing this
>   will render the YAML file invalid, though in all likelihood you will not
>   see any notification of a problem. Rather, all but one of the property
>   definitions will be silently ignored. 

In this document we will refer to specific properties in the CR file using a "path" like 
`/spec/global/repository`. This means the property you find as follows:

1. Find a line that says `spec:` with no indentation at all.
2. Between that line and the next unindented line (not counting comments, which start with `#`),
   find a line that says `global:` and is at the next level of indentation.
3. Between that line and the next line with the same indentation, find a line that starts with
   `repository:`. That line is where the addressed property is defined.
   
Here's an example, where we're looking for the property at `/spec/global/repository`:
```
apiVersion: charts.helm.k8s.io/v1alpha1
kind: Xl
metadata:
  name: xl-release
spec:
  properties:
    global:
      repository:               # This is NOT the correct property
        ...
      
  # Global settings
  global:
    repository:                 # This is the one we're after
```
The first `repository` poperty is not at `/spec/global/repository`, but at 
`/spec/properties/global/repository`. Step 2 doesn't apply because the `global:` line is not
at the _next_ level of indentation, but the one after that. It is easy to get such cases confused 
while editing YAML.

## Basic Scenario - Nothing Provisioned

In the most basic scenario of an external server, a PostgreSQL server with the TimescaleDB
extension is provisioned somewhere in the cloud or in an on-prem VM, but no databases or users are
provisioned for use by Turbonomic. We currently support version 12.x and TimescaleDB 2.0.1.
Documentation regarding basic installation can be found 
[here](https://docs.timescale.com/v2.0/getting-started/installation)

The IP address or host name of the provisioned server can be configured in the CR file at
`/spec/global/externalTimescaleDBIP`, as in:

```
...
spec:
  ...
  global:
    ...
    externalTimescaleDBIP: <host-or-IP>
```
(Here we have included elipses -`...`- to illustrate that other lines may appear; we will omit
these in later illustrations.)


The value of this property should be either the fully-qualified domain name of the external server 
or its IP address, replacing the placeholder shown above including angle brackets.

In this scenario, upon initial startup of the appliance, needed databases and logins will be 
provisioned, using a root (super-user) connection for such operations. By default, we will use
the `postgres` login, and a password that can you can obtain from your sales or field representative.
If you wish to define your own values, you can configure them in the 
`/spec/properties/global/dbs/postgresDefault` property block as follows:

```
spec:
  properties:
    global:
      dbs:
        postgresDefault:
          rootUserName: <root-user>
          rootPassword: <root-password>  
```

## Pre-Provisioned Databases and Logins

In the second scenario, rather than having Turbonomic perform provisioning operations, you take
care of provisioning yourself. You will not need to configure root credentials for Turbonomic, and
you will have the option of choosing your own names for databases, schemas, and logins. If
provisioning is incomplete or incorrect, or if incorrect property values are configured, Turbonomic
will be unable to collect data for Embedded Reporting and/or display reports.

Here are the full set of properties that configure required names and passwords. Each is shown with
the default that will be assumed if that particular property is left out, so if you're happy with
those choices you can leave them out. (You will still be responsible for provisioning those objects).

These properties are spread among the `/spec/properties/global/dbs`, `/spec/properties/extractor/dbs`
and `/spec/grafana/grafana.ini/database` blocks:

```
spec:
  properties:
    global:
      dbs:
        extractor:
          databaseName: extractor
          schemaName: extractor
          query:
            userName: query
            password: <defaault-password>
    extractor:
      dbs:
        extractor:        
          userName: extractor
          password: <default-password>
        grafana:
          scehamName: grafana
  grafana.ini:
    database:
      name: grafana
      user: grafana_backend
      password: <default-password>   
```

A few points on why this structure appears as it does:

* The `/spec/properties/global` block contains properties that will be visible to all Turbonomic
  components. Here we have names and password associated with a read-only login to a database owned
  by the `extractor` component. This login will be used by both the `extractor` and the `api`
  component, so rather than configure it for those two components specifically (and risk that they
  are not in agreement), we have placed them among global properties.
* Properties for the `extractor` database appear in a properties block that is referred to within
  the `extractor` component by the name `"dbs.extractor"`. Similarly, the internal name used for the
  `query` login to the same database is `"dbs.extractor.qeury"`. The common prefix means that the
  `databaseName` and `schemaName` properties will be visible and identical for these two logins.
* There's another database used by the `grafana` component, but provisioning responsibiltiy for that
  database falls to the `extractor` component as well, so its properties could be specified in the
  `/spec/properties/extractor/dbs/grafana` block. However, we only specify `schemaName` above. 
  The reason is that we must also provide these values to the `grafana` component via property names
  that are determined Grafana, not by us. We provide automatic copying of the database and login 
  names and the login password from the `/spec/grafna/grafana.ini` block to the 
  `/spec/properties/extractor/dbs/grafana` block, so they need (should) not be specified there.
* The grafana configuration has property to specify a schema name, so if you want to choose that
  you'll want to configure it in the `/spec/properties/extractor/dbs/grafana` block as shown above.
  The `grafana` component will not include a schema name in any of the SQL that it sends to the
  database, so as long as the grafana login's "search path" is set properly, everything will work.
  
### Other Recommended Settings

In addition to the above settings, the following are recommended but not required:

```
spec:
  properties:
    global:
      dbs:
        postgresDefault:
            shouldProvisionDatabase: false
            shouldProvisionUser: false
            destructiveProvisioningEnabled: false
```

Placing these settings in the `/spec/properties/global/dbs/postgresDefault` block ensures that
they will apply to all PostgreSQL databases and users used by any Turbonomic component. If your
pre-provisioning is more selective than that, you can move the settings to more narrowly targeted
property blocks like `/spec/properties/extractor/dbs/grafana` or 
`/spec/properties/global/dbs/extractor`.

These properties have the following meanings:

* `shouldProvisionDatabase`: Determintes whether Turbonomic will attempt to provision any database
  in scope of the the definition. This is `true` by default for the Embedded Reporting databases
  in order to support the default behavior of provisioning all databases and users during initial
  startup.
* `shouldProvisionUser`: Determines whether Turbonomic will attempt to provision any logins in
  scope of the definition. This is `true` by default for Embedded Reporting logins in order to
  support the default behavior of provisioning all databases and users during initial startup.
* `destructiveProvisioningEnabled`: Determines whether, during its provisioning operations,
  Turbonomic will be allowed to perform destructive operations like dropping databases, schemas
  or users that are found to be mis-configured.  In the default installation scenario, the
  `public` schema that is created by PostgreSQL in any new database is dropped, to reduce the
  complexity of the overall model. Because this is considered a destructive operation, this option
  is `true` by default.

### Provisioning Requirements

The following operations will properly provision all database objects required by Embedded Reporting,
should you choose to take responsibility for that. We use the default names here for illustration;
please substitute your preferred names, matching what you have configured in your CR file.

* Create databases - one for `extractor` data, one for `grafana` data:
  ```
  CREATE DATABASE extractor;
  CREATE DATABASE grafana;
  ```
* Create database users:
  ```
  -- main read/write user for extractor data
  CREATE USER extractor PASSWORD '<password>';
  -- group for users with read-only access to extractor data
  CREATE ROLE readers_extractor_extractor;
  -- read-only user for extractor data as a member of that group
  CREATE USER query PASSWORD '<password>';
  GRANT CONNECT ON DATABASE extractor TO readers_extractor_extractor;
  GRANT readers_extractor_extractor TO query;
  -- read-write user for grafana data
  CREATE USER grafana_backend PASSWORD `<password>`;
  ```
* Create and prepare the schema for extractor data - must be you connected to extractor database
  ```
  CREATE SCHEMA extractor;
  -- read/write user has full access
  GRANT ALL PRIVILEGES ON SCHEMA extractor TO extractor;
  -- all users in readers group have read-only access
  GRANT USAGE on SCHEMA extractor TO readers_extractor_extractor;
  GRANT SELECT ON ALL TABLES IN SCHEMA extractor TO readers_extractor_extractor;
  -- make sure readers get access to any tables added in the future
  ALTER DEFAULT PRIVILEGES IN SCHEMA extractor GRANT SELECT ON TABLES TO readers_extractor_extractor;
  -- make the extractor and query users use the extractor schema by default
  ALTER ROLE extractor SET search_path TO `extractor`;
  ALTER ROLE query SET search_path TO `extractor`;
  -- install the timescaledb plugin into the extractor database using the extractor schema
  CREATE EXTENSION timescaledb SCHEMA extractor;
  ```
* Create and prepare the schema for grafana data - you must be connected to grafana database
  ```
  CREATE SCHEMA grafana;
  -- read/write user has full access
  GRANT ALL PRIVILEGES ON SCHEMA grafana TO grafana_backend;
  -- make sure the grafana user uses the grafana schema by default
  ALTER ROLE grafana_backend SET search_path TO 'grafana';
  ```

## Enabling Secure Connections

Customers who wish to enable TLS/SSL for their database connections are referred ot the PostgreSQL
documentation [here](https://www.postgresql.org/docs/12/ssl-tcp.html).

We have verified that the steps outlined in that document to create a self-signed certificate
produce a server that functions properly with all components of Embedded Reporting. Of course, we
do not recommend that customers use self-signed certificates in production instances.
