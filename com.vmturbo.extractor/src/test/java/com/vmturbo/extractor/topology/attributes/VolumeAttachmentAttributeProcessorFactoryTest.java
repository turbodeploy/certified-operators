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
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualVolumeInfo;
import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.extractor.models.ModelDefinitions.HistoricalAttributes;
import com.vmturbo.extractor.models.Table.Record;
import com.vmturbo.extractor.schema.enums.AttrType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.AttachmentState;
import com.vmturbo.sql.utils.DbEndpoint;

/**
 * Unit tests for {@link VolumeAttachmentAttributeProcessorFactory}.
 */
public class VolumeAttachmentAttributeProcessorFactoryTest {
    private final MutableFixedClock clock = new MutableFixedClock(1_000_000);

    private final long forceUpdateIntervalMs = 10;

    private final DbEndpoint endpoint = mock(DbEndpoint.class);

    private final TopologyInfo topologyInfo = TopologyInfo.newBuilder()
            .setTopologyId(1)
            .setCreationTime(clock.millis())
            .build();

    private HistoricalAttributeProcessor<Boolean> processor =
            new VolumeAttachmentAttributeProcessorFactory(clock, forceUpdateIntervalMs, TimeUnit.MILLISECONDS).newProcessor(endpoint);

    /**
     * Test that an ATTACHED volume state gets mapped to a "true" value.
     */
    @Test
    public void testAttachedVolume() {
        Record record = processor.processEntity(TopologyEntityDTO.newBuilder()
                .setEntityType(ApiEntityType.VIRTUAL_VOLUME.typeNumber())
                .setOid(1)
                .setDisplayName("foo")
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                        .setVirtualVolume(VirtualVolumeInfo.newBuilder()
                                .setAttachmentState(AttachmentState.ATTACHED)))
                .build(), topologyInfo);
        assertNotNull(record);
        assertThat(record.get(HistoricalAttributes.TYPE), is(AttrType.VOLUME_ATTACHED));
        assertThat(record.get(HistoricalAttributes.BOOL_VALUE), is(true));
    }

    /**
     * Test that an UNATTACHED volume state gets mapped to a "false" value.
     */
    @Test
    public void testNonAttachedVolume() {
        Record record = processor.processEntity(TopologyEntityDTO.newBuilder()
                .setEntityType(ApiEntityType.VIRTUAL_VOLUME.typeNumber())
                .setOid(1)
                .setDisplayName("foo")
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                        .setVirtualVolume(VirtualVolumeInfo.newBuilder()
                                .setAttachmentState(AttachmentState.UNATTACHED)))
                .build(), topologyInfo);
        assertNotNull(record);
        assertThat(record.get(HistoricalAttributes.TYPE), is(AttrType.VOLUME_ATTACHED));
        assertThat(record.get(HistoricalAttributes.BOOL_VALUE), is(false));
    }

    /**
     * Test that a non-volume gets mapped to null.
     */
    @Test
    public void testNonVolume() {
        Record record = processor.processEntity(TopologyEntityDTO.newBuilder()
                .setEntityType(ApiEntityType.VIRTUAL_MACHINE.typeNumber())
                .setOid(1)
                .setDisplayName("foo")
                .build(), topologyInfo);
        assertNull(record);
    }


}