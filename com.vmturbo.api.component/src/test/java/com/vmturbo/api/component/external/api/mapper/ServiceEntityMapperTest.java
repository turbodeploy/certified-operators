package com.vmturbo.api.component.external.api.mapper;

import static org.junit.Assert.assertEquals;

import java.util.Collections;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.search.Search.Entity;
import com.vmturbo.common.protobuf.tag.Tag.TagValuesDTO;
import com.vmturbo.common.protobuf.tag.Tag.Tags;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.DiscoveryOrigin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Origin;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.common.mapping.EnvironmentTypeMapper;
import com.vmturbo.common.protobuf.topology.UIEntityState;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.processor.api.AccountValue;
import com.vmturbo.topology.processor.api.ProbeInfo;
import com.vmturbo.topology.processor.api.TargetInfo;
import com.vmturbo.topology.processor.api.TopologyProcessor;

/**
 * Tests the methods of {@link ServiceEntityMapper}.
 */
public class ServiceEntityMapperTest {
    private final TopologyProcessor topologyProcessor = Mockito.mock(TopologyProcessor.class);
    private final ServiceEntityMapper mapper = new ServiceEntityMapper(topologyProcessor);
    private final static long TARGET_ID = 10L;
    private final static String TARGET_DISPLAY_NAME = "display name";
    private final static String PROBE_TYPE = "probe type";

    /**
     * Tests the correct translation from {@link TopologyEntityDTO} to {@link ServiceEntityApiDTO}.
     */
    @Test
    public void testToServiceEntityApiDTO() throws Exception {
        mockTopologyProcessorOutputs();

        final String displayName = "entity display name";
        final long oid = 152L;
        final EntityType entityType = EntityType.VIRTUAL_MACHINE;
        final EntityState entityState = EntityState.POWERED_ON;
        final EnvironmentType environmentType = EnvironmentType.ON_PREM;
        final String tagKey = "tag";
        final String tagValue = "value";

        final TopologyEntityDTO topologyEntityDTO =
            TopologyEntityDTO.newBuilder()
                .setDisplayName(displayName)
                .setOid(oid)
                .setEntityType(entityType.getNumber())
                .setEntityState(entityState)
                .setEnvironmentType(environmentType)
                .setOrigin(
                    Origin.newBuilder()
                        .setDiscoveryOrigin(
                            DiscoveryOrigin.newBuilder().addDiscoveringTargetIds(TARGET_ID)))
                .setTags(
                    Tags.newBuilder()
                        .putTags(
                            tagKey,
                            TagValuesDTO.newBuilder().addValues(tagValue).build()))
                .build();

        final ServiceEntityApiDTO serviceEntityApiDTO =
            mapper.toServiceEntityApiDTO(topologyEntityDTO, null);

        Assert.assertEquals(displayName, serviceEntityApiDTO.getDisplayName());
        Assert.assertEquals(oid, (long)Long.valueOf(serviceEntityApiDTO.getUuid()));
        Assert.assertEquals(
            entityType.getNumber(),
            UIEntityType.fromString(serviceEntityApiDTO.getClassName()).typeNumber());
        Assert.assertEquals(
            entityState,
            UIEntityState.fromString(serviceEntityApiDTO.getState()).toEntityState());
        Assert.assertEquals(
            environmentType,
            EnvironmentTypeMapper.fromApiToXL(serviceEntityApiDTO.getEnvironmentType()).get());
        Assert.assertEquals(1, serviceEntityApiDTO.getTags().size());
        Assert.assertEquals(1, serviceEntityApiDTO.getTags().get(tagKey).size());
        Assert.assertEquals(tagValue, serviceEntityApiDTO.getTags().get(tagKey).get(0));
        checkTargetApiDTO(serviceEntityApiDTO.getDiscoveredBy());
    }

    /**
     * Tests the correct translation from {@link Entity} to {@link ServiceEntityApiDTO}.
     */
    @Test
    public void testSeDTO() throws Exception {
        final long oid = 123456;
        final EntityState state = EntityState.MAINTENANCE;
        final String displayName = "foo";
        final int vmType = EntityType.VIRTUAL_MACHINE_VALUE;

        Entity entity = Entity.newBuilder()
                .setOid(oid)
                .setState(state)
                .setDisplayName(displayName)
                .setType(vmType)
                .addTargetIds(TARGET_ID)
                .build();

        mockTopologyProcessorOutputs();

        ServiceEntityApiDTO seDTO = mapper.toServiceEntityApiDTO(entity, Collections.emptyMap());
        assertEquals(displayName, seDTO.getDisplayName());
        assertEquals(String.valueOf(oid), seDTO.getUuid());
        assertEquals(EntityState.MAINTENANCE.name(), seDTO.getState());
        assertEquals(UIEntityType.VIRTUAL_MACHINE.apiStr(), seDTO.getClassName());
        checkTargetApiDTO(seDTO.getDiscoveredBy());
    }

    private void mockTopologyProcessorOutputs() throws Exception {
        final TargetInfo targetInfo = Mockito.mock(TargetInfo.class);
        final ProbeInfo probeInfo = Mockito.mock(ProbeInfo.class);
        final AccountValue accountValue = Mockito.mock(AccountValue.class);
        Mockito.when(targetInfo.getId()).thenReturn(TARGET_ID);
        final long probeId = 11L;
        Mockito.when(targetInfo.getProbeId()).thenReturn(probeId);
        Mockito.when(targetInfo.getAccountData()).thenReturn(Collections.singleton(accountValue));
        Mockito.when(accountValue.getName()).thenReturn(TargetInfo.TARGET_ADDRESS);
        Mockito.when(accountValue.getStringValue()).thenReturn(TARGET_DISPLAY_NAME);
        Mockito.when(probeInfo.getId()).thenReturn(probeId);
        Mockito.when(probeInfo.getType()).thenReturn(PROBE_TYPE);
        Mockito.when(topologyProcessor.getProbe(Mockito.eq(probeId))).thenReturn(probeInfo);
        Mockito.when(topologyProcessor.getTarget(Mockito.eq(TARGET_ID))).thenReturn(targetInfo);
    }

    private void checkTargetApiDTO(TargetApiDTO targetApiDTO) {
        Assert.assertNotNull(targetApiDTO);
        Assert.assertEquals(TARGET_ID, (long)Long.valueOf(targetApiDTO.getUuid()));
        Assert.assertEquals(TARGET_DISPLAY_NAME, targetApiDTO.getDisplayName());
        Assert.assertEquals(PROBE_TYPE, targetApiDTO.getType());
    }
}
