package com.vmturbo.extractor.topology.attributes;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.extractor.models.ModelDefinitions.HistoricalAttributes;
import com.vmturbo.extractor.models.Table.Record;
import com.vmturbo.extractor.schema.enums.AttrType;
import com.vmturbo.sql.utils.DbEndpoint;

/**
 * Unit tests for {@link NumCpuAttributeProcessorFactory}.
 */
public class NumCpuAttributeProcessorFactoryTest {
    private final MutableFixedClock clock = new MutableFixedClock(1_000_000);

    private final long forceUpdateIntervalMs = 10;

    private final DbEndpoint endpoint = mock(DbEndpoint.class);

    private final TopologyInfo topologyInfo = TopologyInfo.newBuilder()
            .setTopologyId(1)
            .setCreationTime(clock.millis())
            .build();

    /**
     * Verify that the number of CPUs gets correctly extracted from a {@link TopologyEntityDTO}
     * and put into a {@link Record}.
     */
    @Test
    public void testNumCpuProcessorFactory() {
        NumCpuAttributeProcessorFactory f = new NumCpuAttributeProcessorFactory(clock, forceUpdateIntervalMs, TimeUnit.MILLISECONDS);
        HistoricalAttributeProcessor<Integer> processor = f.newProcessor(endpoint);
        Record record = processor.processEntity(TopologyEntityDTO.newBuilder()
                .setEntityType(ApiEntityType.VIRTUAL_MACHINE.typeNumber())
                .setOid(1)
                .setDisplayName("foo")
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                        .setVirtualMachine(VirtualMachineInfo.newBuilder()
                                .setNumCpus(10)))
                .build(), topologyInfo);
        assertNotNull(record);
        assertThat(record.get(HistoricalAttributes.TYPE), is(AttrType.NUM_VCPU));
        assertThat(record.get(HistoricalAttributes.INT_VALUE), is(10));
    }

    /**
     * Verify that the processor returns null if the {@link TopologyEntityDTO} does not have
     * num vcpus.
     */
    @Test
    public void testNumCpuProcessorFactoryIgnoreIfMissingData() {
        NumCpuAttributeProcessorFactory f = new NumCpuAttributeProcessorFactory(clock, forceUpdateIntervalMs, TimeUnit.MILLISECONDS);
        HistoricalAttributeProcessor<Integer> processor = f.newProcessor(endpoint);
        Record record = processor.processEntity(TopologyEntityDTO.newBuilder()
                .setEntityType(ApiEntityType.VIRTUAL_MACHINE.typeNumber())
                .setOid(1)
                .setDisplayName("foo")
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                        // No num cpus.
                        .setVirtualMachine(VirtualMachineInfo.newBuilder()))
                .build(), topologyInfo);
        assertNull(record);
    }

}