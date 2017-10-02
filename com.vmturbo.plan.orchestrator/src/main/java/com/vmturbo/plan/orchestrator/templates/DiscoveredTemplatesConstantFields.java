package com.vmturbo.plan.orchestrator.templates;

/**
 * Contains all the UI displayed template fields. Right now in order to convert
 * {@link com.vmturbo.platform.common.dto.ProfileDTOREST.EntityProfileDTO} object to our template
 * model, we need pre-defined all the UI displayed template fields. Later on, when probe send us
 * new template model proto buff, it will contains all the required field names.
 */
public class DiscoveredTemplatesConstantFields {

    public static final String VM_COMPUTE_NUM_OF_VCPU = "numOfCpu";

    public static final String VM_COMPUTE_VCPU_SPEED = "cpuSpeed";

    public static final String VM_COMPUTE_CPU_CONSUMED_FACTOR = "cpuConsumedFactor";

    public static final String VM_COMPUTE_MEM_SIZE = "memorySize";

    public static final String VM_COMPUTE_MEM_CONSUMED_FACTOR = "memoryConsumedFactor";

    public static final String VM_COMPUTE_IO_THROUGHPUT_CONSUMED = "ioThroughputConsumed";

    public static final String VM_COMPUTE_NETWORK_THROUGHPUT_CONSUMED = "networkThroughputConsumed";

    public static final String VM_STORAGE_DISK_SIZE = "diskSize";

    public static final String VM_STORAGE_DISK_IOPS_CONSUMED = "diskIopsConsumed";

    public static final String VM_STORAGE_DISK_CONSUMED_FACTOR = "diskConsumedFactor";

    public static final String PM_COMPUTE_NUM_OF_CORE = "numOfCores";

    public static final String PM_COMPUTE_CPU_SPEED = "cpuSpeed";

    public static final String PM_COMPUTE_IO_THROUGHPUT_SIZE = "ioThroughput";

    public static final String PM_COMPUTE_MEM_SIZE = "memorySize";

    public static final String PM_COMPUTE_NETWORK_THROUGHTPUT_SIZE = "networkThroughput";

    public static final String PM_INFRA_POWER_SIZE = "powerSize";

    public static final String PM_INFRA_SPACE_SIZE = "spaceSize";

    public static final String PM_INFRA_COOLING_SIZE = "coolingSize";

    public static final String STORAGE_DISK_IOPS = "diskIops";

    public static final String STORAGE_DISK_SIZE = "diskSize";
}
