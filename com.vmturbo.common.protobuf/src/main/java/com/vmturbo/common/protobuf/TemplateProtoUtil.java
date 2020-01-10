package com.vmturbo.common.protobuf;

import java.util.Iterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.plan.TemplateDTO.GetTemplatesResponse;
import com.vmturbo.common.protobuf.plan.TemplateDTO.SingleTemplateResponse;

/**
 * Utilities for easier interaction with the protos defined in TemplateDTO.
 */
public class TemplateProtoUtil {

    public static final String VM_COMPUTE_NUM_OF_VCPU = "numOfCpu";

    public static final String VM_COMPUTE_VCPU_SPEED = "cpuSpeed";

    public static final String VM_COMPUTE_CPU_CONSUMED_FACTOR = "cpuConsumedFactor";

    public static final float VM_COMPUTE_CPU_CONSUMED_FACTOR_DEFAULT_VALUE = 0.5f;

    public static final String VM_COMPUTE_MEM_SIZE = "memorySize";

    public static final String VM_COMPUTE_MEM_CONSUMED_FACTOR = "memoryConsumedFactor";

    public static final float VM_COMPUTE_MEM_CONSUMED_FACTOR_DEFAULT_VALUE = 0.75f;

    public static final String VM_COMPUTE_IO_THROUGHPUT_CONSUMED = "ioThroughputConsumed";

    public static final String VM_COMPUTE_IO_THROUGHPUT_SIZE = "ioThroughputSize";

    public static final String VM_COMPUTE_NETWORK_THROUGHPUT_CONSUMED = "networkThroughputConsumed";

    public static final String VM_COMPUTE_NETWORK_THROUGHPUT_SIZE = "networkThroughputSize";

    public static final String VM_STORAGE_DISK_SIZE = "diskSize";

    public static final String VM_STORAGE_DISK_IOPS = "diskIops";

    public static final String VM_STORAGE_DISK_IOPS_CONSUMED = "diskIopsConsumed";

    public static final String VM_STORAGE_DISK_CONSUMED_FACTOR = "diskConsumedFactor";

    public static final float VM_STORAGE_DISK_CONSUMED_FACTOR_DEFAULT_VALUE = 1.0f;

    public static final String PM_COMPUTE_NUM_OF_CORE = "numOfCores";

    public static final String PM_COMPUTE_CPU_SPEED = "cpuSpeed";

    public static final String PM_COMPUTE_IO_THROUGHPUT_SIZE = "ioThroughputSize";

    public static final String PM_COMPUTE_MEM_SIZE = "memorySize";

    public static final String PM_COMPUTE_NETWORK_THROUGHPUT_SIZE = "networkThroughputSize";

    public static final String PM_INFRA_POWER_SIZE = "powerSize";

    public static final String PM_INFRA_SPACE_SIZE = "spaceSize";

    public static final String PM_INFRA_COOLING_SIZE = "coolingSize";

    public static final String STORAGE_DISK_IOPS = "diskIops";

    public static final String STORAGE_DISK_SIZE = "diskSize";

    private TemplateProtoUtil() {}

    /**
     * Flatten an iterator over chunks of {@link SingleTemplateResponse}s into a stream of
     * {@link SingleTemplateResponse}s.
     *
     * @param responseIt The iterator, returned from an RPC call to the templates service.
     * @return A stream of {@link SingleTemplateResponse}s contained in the response..
     */
    @Nonnull
    public static Stream<SingleTemplateResponse> flattenGetResponse(
            @Nonnull final Iterator<GetTemplatesResponse> responseIt) {
        final Iterable<GetTemplatesResponse> iterable = () -> responseIt;
        return StreamSupport.stream(iterable.spliterator(), false)
            .flatMap(resp -> resp.getTemplatesList().stream());
    }
}
