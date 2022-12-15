# Welcome to XL

Please check the [Turbonomic Architecture](https://vmturbo.atlassian.net/wiki/spaces/XD/pages/3274801175/Turbonomic+XL+Kubernetes+Deployment+Architecture) drawing. 

There are couple video presentations and materials that explain what Turbonomic is and how it works. 
Start with the [Understanding our product A](https://vmturbo.atlassian.net/wiki/spaces/XD/pages/edit-v2/3004334117) presentation by Danilo Florissi which is a demo of the whole system.
and continue with the [Understanding our product B](https://vmturbo.atlassian.net/wiki/spaces/XD/pages/edit-v2/3004334117) presentation by Emanuele Maccherani which is an architectural description of the product.

[Turbonomic 101](https://vmturbo.atlassian.net/wiki/spaces/XD/pages/3004334117/New+Hire+Onboarding) page is the entry point for onbording all new hire.

1. Prerequisites
2. [Maven Build Installation](#maven-build-installation)
3. [Troubleshooting Common Errors](#troubleshooting-common-errors)
   1. [Database Connection](#database-connection-error)
   2. [ContainerD Error](#containerd-error)
   3. [Address Connection](#could-not-connect-to-address)
   4. [Java version](#java-version-error)
4. [Contributing to the code](#code-contribution)
5. [Install Turbonomic XL in the Lab](https://vmturbo.atlassian.net/wiki/spaces/VMTsupport/pages/2774401748/Deploying+the+Turbonomic+OVA)
6. [Install Turbonomic XL on Kubernetes (Development)](https://vmturbo.atlassian.net/wiki/spaces/DEVOPS/pages/826114160/Turbonomic+XL+on+Kubernetes+Developer)
7. [Install Turbonomic XL on Kubernetes (Production)](https://vmturbo.atlassian.net/wiki/spaces/DEVOPS/pages/716931392/Turbonomic+XL+on+Kubernetes+Production)
8. [XL Development](#https://vmturbo.atlassian.net/wiki/spaces/XD/overview?homepageId=955678946)
   1. [Working with IntelliJ Editor](https://vmturbo.atlassian.net/wiki/spaces/Home/pages/78971073/Working+with+the+IntelliJ+editor#WorkingwiththeIntelliJeditor-UpdatingMaveninIntellij)
   2. [Loading Diagnostics into VM](https://vmturbo.atlassian.net/wiki/spaces/CE/pages/3044966418/How+to+load+customer+diagnostics+in+a+lab+instance)
9. [Operations Manager Test Results](https://vmturbo.testrail.com/index.php?/projects/overview/1)
10. [Automatic and Manual Test Cases](https://vmturbo.atlassian.net/wiki/spaces/Home/pages/17301567/Development+Targets+and+other+Target+Information#DevelopmentTargetsandotherTargetInformation-AppDynamics%2FDynatrace%2FDatadog%2FApplicationInsights%2FNewRelic%2FInstanaAppDynTrace)
11. [Test Team Wiki](https://vmturbo.atlassian.net/wiki/spaces/Home/pages/17301528/Test+Team+Wiki)
12. [Turbonomic Glossary](https://www.turbonomic.training/mod/glossary/view.php?id=185)
13. [Other Useful Links](#useful-links)

-----------
Before you start the build make sure:
* You are connected to TurboVPN. See [instructions](https://vmturbo.atlassian.net/wiki/spaces/Home/pages/38207490/Setting+Up+VPN).
* Your Docker/Rancher is connected. See [instructions](https://vmturbo.atlassian.net/wiki/spaces/XD/pages/3177807877/Rancher+Desktop).
>>NOTE: in the instructions that [Rancher v1.21.9](https://vmturbo.atlassian.net/wiki/spaces/XD/pages/3177807877/Rancher+Desktop) is the recommended one to be installed.
-----------
## Maven Build Installation 
Start with these instructions: [Setting up XL Dev Environment](https://vmturbo.atlassian.net/wiki/spaces/XD/pages/171666414/Setting+Up+XL+Dev+Environment)

If you hit an error while running the maven build you can:

1. Start by building a smaller component:
   ```shell
   cd xl/com.vmturbo.sql.utils
   mvn clean install -s ../build/settings.xml
    ```
2. When step 1 is successful, try with a more complex part of the project:
    ```shell
    cd xl/com.vmturbo.auth.component
    mvn clean install -s ../build/settings.xml -Prelease,docker -DskipTests
    ```
3. Before you go deeper in troubleshooting, try removing `~/.m2/repositories` and run `mvn clean install -U`.


4. Double check your VPN is still working. At times Rancher Desktop gets into a broken state


5. Return to `build` folder and try maven install again

-----------
## Troubleshooting Common Errors

### Database Connection Error
```shell
[ERROR] Failed to execute goal org.flywaydb:flyway-maven-plugin:4.2.0:migrate (default) on project auth: org.flywaydb.core.internal.dbsupport.FlywaySqlException: 
[ERROR] Unable to obtain Jdbc connection from DataSource (jdbc:mysql://localhost:3306) for user 'root': Could not connect to address=(host=localhost)(port=3306)(type=master) : Connection refused (Connection refused)
```
This is because it cannot find the connection to the database. See step 6 in the [WIKI](https://vmturbo.atlassian.net/wiki/spaces/XD/pages/171666414/Setting+Up+XL+Dev+Environment) for more info on how to fix this issue.

### NOTE:
> In order to use `Docker Desktop` in an enterprise setting you need a license.
> Buy a license through IBM or install `Rancher Desktop` with `nerdctl`. If you also add an alias in your `.bashrc` file like this:
```shell alias docker=nerdctl ``` you can then use `nerdctl` just as you use `docker` command.

### Containerd Error

```shell
FATA[0000] cannot access containerd socket "/run/k3s/containerd/containerd.sock" (hint: try running with `--address /var/run/docker/containerd/containerd.sock` to connect to Docker-managed containerd): no such file or directory 
```

If you get the following error when running `docker compose up -d db` your solution is to switch from `dockerd` to `containerd` option in the rancher-desktop.

The opposite might be necessary if you see error with docker. Read more about it: [Cannot connect to docker daemon](https://vmturbo.atlassian.net/wiki/spaces/XD/pages/3177807877/Rancher+Desktop#Cannot-connect-to-the-Docker-daemon)

### Could not connect to address

```shell
ERROR] Failed to execute goal org.flywaydb:flyway-maven-plugin:4.2.0:migrate (migrate) on project com.vmturbo.history.schema: org.flywaydb.core.internal.dbsupport.FlywaySqlException:
[ERROR] Unable to obtain Jdbc connection from DataSource (jdbc:mysql://localhost:3306) for user 'root': Could not connect to address=(host=localhost)(port=3306)(type=master) : Connection refused (Connection refused)
[ERROR] ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
[ERROR] SQL State  : 08
[ERROR] Error Code : -1
```

You can find the instructions on how to fix this issue step in the [Settinng Up XL Dev Environment](https://vmturbo.atlassian.net/wiki/spaces/XD/pages/1004503526/Setting+Up+XL+Dev+Environment+Kubernetes+Mac) page.
I will copy here the troubleshooting steps for easy access.

Build process needs access to a database and it cannot reach it for some reason. 
Sometimes the container dies when you upgrade docker. Easiest way to start it back up is:

```
# disable classic database
sudo launchctl unload -F /Library/LaunchDaemons/com.mysql.mysqld.plist
 
# make database container image
cd db
mvn clean install -Pdocker
# run database container
cd ../build
docker-compose up -d db
 
# try to build again
```

### Java version error

```shell
[ERROR] /Users/roxana/Github/Turbo/xl/com.vmturbo.cost.component/src/test/java/com/vmturbo/cost/component/entity/cost/SqlEntityCostStoreTest.java: valuesRow() in org.jooq.impl.TableRecordImpl cannot implement valuesRow() in org.jooq.Record10
[ERROR]   return type org.jooq.Row is not compatible with org.jooq.Row10<java.lang.Long,java.time.LocalDateTime,java.lang.Integer,java.lang.Integer,java.lang.Integer,java.math.BigDecimal,java.lang.Integer,java.lang.Long,java.lang.Long,java.lang.Long>
```

If you see this error there are few possible problems:
1. Turbo-XL does not support JDK 11 so you might need to install JDK 8. See [supporting multiple java versions on Mac](https://chamikakasun.medium.com/how-to-manage-multiple-java-version-in-macos-e5421345f6d0) .
There are also more details about this problem in the slack [thread](https://turbonomic.slack.com/archives/CPHMC9SBS/p1643110952063700) and the open [story](https://vmturbo.atlassian.net/browse/OM-79910) . 
2. You can run the mvn command with `-U/--update-snapshots` option, which forces downloading artifacts from remote repositories, rather than going to the local maven repository.
-----------

## Code Contribution

You cloned and build locally the XL project and now you are ready to make a contribution.

* Start off by creating a JIRA Task in the [backlog](https://vmturbo.atlassian.net/jira/software/c/projects/OM/issues). Check this short demo about [this task](https://vmturbo-portal1.sharepoint.com/engineering/Shared%20Documents/Forms/AllItems.aspx?id=%2Fengineering%2FShared%20Documents%2FProcess%2FNew%20Hire%20Training%2FTurbonomic%20Video%20Tutorial%2FCreating%20Jira%20Story%2Emp4&parent=%2Fengineering%2FShared%20Documents%2FProcess%2FNew%20Hire%20Training%2FTurbonomic%20Video%20Tutorial&p=true&ga=1).
* Every git branch should be called starting with the ID generated for the JIRA task created in the previous step, and should be included under `feature` or `bugfix`. For example, this is an example of a branch `feature/OM-86018-add-readme`
-----------
## Useful Links

* [Best Practices](https://vmturbo.atlassian.net/wiki/spaces/Home/pages/1016988368/Best+Practices)

* [XL Homepage](https://vmturbo.atlassian.net/wiki/spaces/XD/pages/132644946/XL+Homepage)

* [XL Development](https://vmturbo.atlassian.net/wiki/spaces/XD/overview?homepageId=955678946)

* [Dev Environment Setup Entry](https://vmturbo.atlassian.net/wiki/spaces/XD/pages/3004334117/New+Hire+Onboarding)

* [Turbonomic Platform Operator](https://github.com/turbonomic/t8c-install)

* [Build Status](http://jnm01.eng.vmturbo.com:8080/view/XL%20Staging/view/job%20overview/job/xl-staging-ci-build/)


 ## Reactor Summary for Turbonomic-XL

The list of goals performed during maven build in the exact order. 
You can start a build at a particular goal by appending `-rf :<goal_name>` to your `mvn install` command.
This will skip all previous goals and will start with the one provided in the command.


|[INFO] Turbonomic-XL ...................................... SUCCESS   |
|-----------------------------------------------------------------------|
| [INFO] com.vmturbo.versioning ............................. SUCCESS   |
| [INFO] com.vmturbo.components.baseimage ................... SUCCESS   |
| [INFO] syslog ............................................. SUCCESS   |
| [INFO] db ................................................. SUCCESS   |
| [INFO] components-api ..................................... SUCCESS   |
| [INFO] components-api-test ................................ SUCCESS   |
| [INFO] com.vmturbo.api.conversion ......................... SUCCESS   |
| [INFO] protoc-plugin-common ............................... SUCCESS   |
| [INFO] protoc-spring-rest ................................. SUCCESS   |
| [INFO] protoc-spring-rest-plugin .......................... SUCCESS   |
| [INFO] protoc-grpc-moles .................................. SUCCESS   |
| [INFO] protoc-grpc-moles-plugin ........................... SUCCESS   |
| [INFO] protoc-pojo-gen .................................... SUCCESS   |
| [INFO] protoc-pojo-gen-plugin ............................. SUCCESS   |
| [INFO] common-protobuf .................................... SUCCESS   |
| [INFO] components-api-root ................................ SUCCESS   |
| [INFO] com.vmturbo.clustermgr.api ......................... SUCCESS   |
| [INFO] kvstore ............................................ SUCCESS   |
| [INFO] oid ................................................ SUCCESS   |
| [INFO] common-api ......................................... SUCCESS   |
| [INFO] com.vmturbo.repository.api ......................... SUCCESS   |
| [INFO] com.vmturbo.group.api .............................. SUCCESS   |
| [INFO] auth-api ........................................... SUCCESS   |
| [INFO] component-status-api ............................... SUCCESS   |
| [INFO] com.vmturbo.components.common ...................... SUCCESS   |
| [INFO] com.vmturbo.influxdb ............................... SUCCESS   |
| [INFO] protoc-spring-rest-test ............................ SUCCESS   |
| [INFO] testFileNameFormat ................................. SUCCESS   |
| [INFO] testCommentsToSwagger .............................. SUCCESS   |
| [INFO] testEnum ........................................... SUCCESS   |
| [INFO] testImport ......................................... SUCCESS   |
| [INFO] testJavaKeyword .................................... SUCCESS   |
| [INFO] testJavaMultipleFiles .............................. SUCCESS   |
| [INFO] testJavaOpt ........................................ SUCCESS   |
| [INFO] testNameCollisions ................................. SUCCESS   |
| [INFO] testNested ......................................... SUCCESS   |
| [INFO] testNestedEnum ..................................... SUCCESS   |
| [INFO] testOneOf .......................................... SUCCESS   |
| [INFO] testOuterName ...................................... SUCCESS   |
| [INFO] testProto3 ......................................... SUCCESS   |
| [INFO] testServices ....................................... SUCCESS   |
| [INFO] testTypes .......................................... SUCCESS   |
| [INFO] testUnderscores .................................... SUCCESS   |
| [INFO] testHttpRoutes ..................................... SUCCESS   |
| [INFO] protoc-pojo-gen-test ............................... SUCCESS   |
| [INFO] testJavaOpt-pojo ................................... SUCCESS   |
| [INFO] protoc-grpc-moles-test ............................. SUCCESS   |
| [INFO] testMoleImport ..................................... SUCCESS   |
| [INFO] testMoleServices ................................... SUCCESS   |
| [INFO] testNameCollision .................................. SUCCESS   |
| [INFO] testNameOverride ................................... SUCCESS   |
| [INFO] diags .............................................. SUCCESS   |
| [INFO] com.vmturbo.kafka .................................. SUCCESS   |
| [INFO] com.vmturbo.zookeeper .............................. SUCCESS   |
| [INFO] com.vmturbo.nginx .................................. SUCCESS   |
| [INFO] hydra .............................................. SUCCESS   |
| [INFO] skupper ............................................ SUCCESS   |
| [INFO] xl-python .......................................... SUCCESS   |
| [INFO] grafana ............................................ SUCCESS   |
| [INFO] com.vmturbo.licensing .............................. SUCCESS   |
| [INFO] system-notification-receiver ....................... SUCCESS   |
| [INFO] system-notification-sender ......................... SUCCESS   |
| [INFO] sql-utils .......................................... SUCCESS   |
| [INFO] sql-test-utils ..................................... SUCCESS   |
| [INFO] auth-test .......................................... SUCCESS   |
| [INFO] com.vmturbo.components ............................. SUCCESS   |
| [INFO] auth ............................................... SUCCESS   |
| [INFO] topology-processor-api ............................. SUCCESS   |
| [INFO] action-orchestrator-api ............................ SUCCESS   |
| [INFO] history-component-api .............................. SUCCESS   |
| [INFO] com.vmturbo.history.schema ......................... SUCCESS   |
| [INFO] cost-api ........................................... SUCCESS   |
| [INFO] search-metadata .................................... SUCCESS   |
| [INFO] com.vmturbo.search.schema .......................... SUCCESS   |
| [INFO] extractor-schema ................................... SUCCESS   |
| [INFO] market-component-api ............................... SUCCESS   |
| [INFO] search-api ......................................... SUCCESS   |
| [INFO] com.vmturbo.extractor.api .......................... SUCCESS   |
| [INFO] identity ........................................... SUCCESS   |
| [INFO] com.vmturbo.plan.orchestrator.api .................. SUCCESS   |
| [INFO] com.vmturbo.group .................................. SUCCESS   |
| [INFO] com.vmturbo.api.component .......................... SUCCESS   |
| [INFO] com.vmturbo.communication.matrix ................... SUCCESS   |
| [INFO] trax ............................................... SUCCESS   |
| [INFO] cloud-common ....................................... SUCCESS   |
| [INFO] cost-calculation ................................... SUCCESS   |
| [INFO] reserved-instance-coverage-allocator ............... SUCCESS   |
| [INFO] cloud-commitment-analysis .......................... SUCCESS   |
| [INFO] com.vmturbo.clustermgr ............................. SUCCESS   |
| [INFO] com.vmturbo.consul ................................. SUCCESS   |
| [INFO] topology-graph ..................................... SUCCESS   |
| [INFO] stitching .......................................... SUCCESS   |
| [INFO] com.vmturbo.mediation.client.bootstrap ............. SUCCESS   |
| [INFO] com.vmturbo.mediation.client ....................... SUCCESS   |
| [INFO] topology-processor ................................. SUCCESS   |
| [INFO] com.vmturbo.discovery.deserializer ................. SUCCESS   |
| [INFO] sample-api ......................................... SUCCESS   |
| [INFO] sample ............................................. SUCCESS   |
| [INFO] components-test-utilities .......................... SUCCESS   |
| [INFO] action-orchestrator ................................ SUCCESS   |
| [INFO] market-component ................................... SUCCESS   |
| [INFO] com.vmturbo.repository-component ................... SUCCESS   |
| [INFO] ml-datastore ....................................... SUCCESS   |
| [INFO] extractor .......................................... SUCCESS   |
| [INFO] prometheus-config-manager .......................... SUCCESS   |
| [INFO] intersight-integration ............................. SUCCESS   |
| [INFO] com.vmturbo.topology.event.library ................. SUCCESS   |
| [INFO] com.vmturbo.testbases .............................. SUCCESS   |
| [INFO] com.vmturbo.history ................................ SUCCESS   |
| [INFO] kibitzer ........................................... SUCCESS   |
| [INFO] com.vmturbo.plan.orchestrator ...................... SUCCESS   |
| [INFO] com.vmturbo.cost.component ......................... SUCCESS   |
| [INFO] xl-sdk-probe-parent ................................ SUCCESS   |
| [INFO] com.vmturbo.mediation.webhook ...................... SUCCESS   |
| [INFO] com.vmturbo.mediation.webhook.component ............ SUCCESS   |
| [INFO] voltron ............................................ SUCCESS   |
| [INFO] com.vmturbo.mediation.delegatingprobe .............. SUCCESS   |
| [INFO] com.vmturbo.mediation.actionscript ................. SUCCESS   |
| [INFO] com.vmturbo.mediation.actionstream.kafka ........... SUCCESS   |
| [INFO] mediation .......................................... SUCCESS   |
| [INFO] com.vmturbo.mediation.delegatingprobe.component .... SUCCESS   |
| [INFO] com.vmturbo.mediation.actionscript.component ....... SUCCESS   |
| [INFO] com.vmturbo.mediation.apm.snmp.component ........... SUCCESS   |
| [INFO] com.vmturbo.mediation.apm.wmi.component ............ SUCCESS   |
| [INFO] com.vmturbo.mediation.appdynamics.component ........ SUCCESS   |
| [INFO] com.vmturbo.mediation.aws.billing.component ........ SUCCESS   |
| [INFO] com.vmturbo.mediation.aws.cloudbilling.component ... SUCCESS   |
| [INFO] com.vmturbo.mediation.aws.component ................ SUCCESS   |
| [INFO] com.vmturbo.mediation.aws.cost.component ........... SUCCESS   |
| [INFO] com.vmturbo.mediation.aws.lambda.component ......... SUCCESS   |
| [INFO] com.vmturbo.mediation.azure.component .............. SUCCESS   |
| [INFO] com.vmturbo.mediation.azure.volumes.component ...... SUCCESS   |
| [INFO] com.vmturbo.mediation.azure.cost.component ......... SUCCESS   |
| [INFO] com.vmturbo.mediation.azure.ea.component ........... SUCCESS   |
| [INFO] com.vmturbo.mediation.azure.billing.component ...... SUCCESS   |
| [INFO] com.vmturbo.mediation.azure.sp.component ........... SUCCESS   |
| [INFO] com.vmturbo.mediation.baremetal.component .......... SUCCESS   |
| [INFO] com.vmturbo.mediation.compellent.component ......... SUCCESS   |
| [INFO] com.vmturbo.mediation.udt .......................... SUCCESS   |
| [INFO] com.vmturbo.mediation.udt.component ................ SUCCESS   |
| [INFO] com.vmturbo.mediation.database.mssql.component ..... SUCCESS   |
| [INFO] com.vmturbo.mediation.database.mysql.component ..... SUCCESS   |
| [INFO] com.vmturbo.mediation.database.oracle.component .... SUCCESS   |
| [INFO] com.vmturbo.mediation.tomcat.component ............. SUCCESS   |
| [INFO] com.vmturbo.mediation.websphere.component .......... SUCCESS   |
| [INFO] com.vmturbo.mediation.weblogic.component ........... SUCCESS   |
| [INFO] com.vmturbo.mediation.jboss.component .............. SUCCESS   |
| [INFO] com.vmturbo.mediation.ibmstorage.flashsystem.component SUCCESS |
| [INFO] com.vmturbo.mediation.instana.component ............ SUCCESS   |
| [INFO] com.vmturbo.mediation.jvm.component ................ SUCCESS   |
| [INFO] com.vmturbo.mediation.datadog.component ............ SUCCESS   |
| [INFO] com.vmturbo.mediation.dynatrace.component .......... SUCCESS   |
| [INFO] com.vmturbo.mediation.gcp.sa.component ............. SUCCESS   |
| [INFO] com.vmturbo.mediation.gcp.cost.component ........... SUCCESS   |
| [INFO] com.vmturbo.mediation.gcp.project.component ........ SUCCESS   |
| [INFO] com.vmturbo.mediation.gcp.billing.component ........ SUCCESS   |
| [INFO] com.vmturbo.mediation.flexera.component ............ SUCCESS   |
| [INFO] com.vmturbo.mediation.tanium.component ............. SUCCESS   |
| [INFO] com.vmturbo.mediation.hds.component ................ SUCCESS   |
| [INFO] com.vmturbo.mediation.horizon.component ............ SUCCESS   |
| [INFO] com.vmturbo.mediation.hpe3par.component ............ SUCCESS   |
| [INFO] com.vmturbo.mediation.hyperflex.component .......... SUCCESS   |
| [INFO] com.vmturbo.mediation.hyperv.component ............. SUCCESS   |
| [INFO] com.vmturbo.mediation.intersight.hyperflex.component SUCCESS   |
| [INFO] com.vmturbo.mediation.intersight.server.component .. SUCCESS   |
| [INFO] com.vmturbo.mediation.intersight.ucs.component ..... SUCCESS   |
| [INFO] com.vmturbo.mediation.actionstream.kafka.component . SUCCESS   |
| [INFO] com.vmturbo.mediation.netapp.component ............. SUCCESS   |
| [INFO] com.vmturbo.mediation.newrelic.component ........... SUCCESS   |
| [INFO] com.vmturbo.mediation.appinsights.component ........ SUCCESS   |
| [INFO] com.vmturbo.mediation.nutanix.component ............ SUCCESS   |
| [INFO] com.vmturbo.mediation.oneview.component ............ SUCCESS   |
| [INFO] com.vmturbo.mediation.pure.component ............... SUCCESS   |
| [INFO] com.vmturbo.mediation.scaleio.component ............ SUCCESS   |
| [INFO] com.vmturbo.mediation.servicenow.component ......... SUCCESS   |
| [INFO] com.vmturbo.mediation.storagestressprobe.component . SUCCESS   |
| [INFO] com.vmturbo.mediation.stressprobe.component ........ SUCCESS   |
| [INFO] com.vmturbo.mediation.terraform.component .......... SUCCESS   |
| [INFO] com.vmturbo.mediation.ucs.component ................ SUCCESS   |
| [INFO] com.vmturbo.mediation.ucsdirector.component ........ SUCCESS   |
| [INFO] com.vmturbo.mediation.vcenter.browsing.component ... SUCCESS   |
| [INFO] com.vmturbo.mediation.vcenter.component ............ SUCCESS   |
| [INFO] com.vmturbo.mediation.vmax.component ............... SUCCESS   |
| [INFO] com.vmturbo.mediation.vmm.component ................ SUCCESS   |
| [INFO] com.vmturbo.mediation.vplex.component .............. SUCCESS   |
| [INFO] com.vmturbo.mediation.xtremio.component ............ SUCCESS   |
| [INFO] com.vmturbo.mediation.xen.component ................ SUCCESS   |
| [INFO] components-performance-test ........................ SUCCESS   |
| [INFO] system-tests ....................................... SUCCESS   |
