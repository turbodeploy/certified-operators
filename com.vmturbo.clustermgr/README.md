 The ClusterMgrMain is a utility to 
 
 
1. Maintain the VMT Cluster Configuration, including
  * a list of all available component types and the default property/value configuration for each 
  * a list of all the nodes in the cluster; for each node a list of ID's of components to run on that node; and for each Component ID, the current property/value configuration

2. Provide a CRUD HTTP API for the configuration in (1) above

3. Launch each of the VmtComponent Docker Containers configured to run on the current node (server).

4. Proved an HTTP API to retrieve diagnostic information from each currently running component.


The list of VmtComponents to launch, including component name and version number, is maintained on the shared
persistent key/value server (currently implemented via Consul) and is injected automatically by Spring Cloud Configuration.