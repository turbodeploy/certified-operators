Prometheus Telemetry sidecar container

* This component is responsible for reading telemetry opt-in setting
* from group component and updating the prometheus configuration
* to upload (or not) customer data to data cloud
* Implements a python script to call group component to get setting
* Implements python script to update prometheus configuration
