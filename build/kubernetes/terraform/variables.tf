variable "kubeconfig" {
  default = "~/.kube/config"
}

variable "name" {
  default = "xl-release"
}

variable "namespace" {
  default = "turbonomic"
}

variable "tag" {}

variable "externalIP" {}

variable "ui" {
  default = true
}

variable "history" {
  default = true
}

variable "reporting" {
  default = true
}

variable "metron" {
  default = false
}

# Monitoring
variable "grafana" {
  default = false
}

variable "prometheus" {
  default = false
}

# Logging
variable "elk" {
  default = false
}

# Medation
variable "actionscript" {
  default = false
}

variable "aix" {
  default = false
}

variable "appdynamics" {
  default = false
}

variable "aws" {
  default = false
}

variable "awsbilling" {
  default = false
}

variable "awscost" {
  default = false
}

variable "awslambda" {
  default = false
}

variable "azure" {
  default = false
}

variable "azurecost" {
  default = false
}

variable "azurevolumes" {
  default = false
}

variable "cloudfoundry" {
  default = false
}

variable "compellent" {
  default = false
}

variable "dynatrace" {
  default = false
}

variable "hpe3par" {
  default = false
}

variable "hds" {
  default = false
}

variable "hyperflex" {
  default = false
}

variable "hyperv" {
  default = false
}

variable "istio" {
  default = false
}

variable "mssql" {
  default = false
}

variable "netapp" {
  default = false
}

variable "netflow" {
  default = false
}

variable "oneview" {
  default = false
}

variable "openstack" {
  default = false
}

variable "pivotal" {
  default = false
}

variable "pure" {
  default = false
}

variable "rhv" {
  default = false
}

variable "scaleio" {
  default = false
}

variable "snmp" {
  default = false
}

variable "tetration" {
  default = false
}

variable "ucs" {
  default = false
}

variable "ucsdirector" {
  default = false
}

variable "vcd" {
  default = false
}

variable "vcenter" {
  default = false
}

variable "vcenterbrowsing" {
  default = false
}

variable "vmax" {
  default = false
}

variable "vmm" {
  default = false
}

variable "vplex" {
  default = false
}

variable "wmi" {
  default = false
}

variable "xtremio" {
  default = false
}

variable "action-orchestrator_memory" {}
variable "history_memory" {}
variable "market_memory" {}
variable "repository_memory" {}
variable "topology-processor_memory" {}
variable "mediation-appdynamics_memory" {}
variable "mediation-vcenter_memory" {}
