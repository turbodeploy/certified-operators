package com.vmturbo.api.component.external.api.mapper;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Optional;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper.DiscoveredByCache;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.tag.Tag.TagValuesDTO;
import com.vmturbo.common.protobuf.tag.Tag.Tags;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.DiscoveryOrigin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Origin;
import com.vmturbo.common.protobuf.topology.UIEntityState;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.common.mapping.EnvironmentTypeMapper;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.processor.api.AccountValue;
import com.vmturbo.topology.processor.api.ProbeInfo;
import com.vmturbo.topology.processor.api.TargetInfo;
import com.vmturbo.topology.processor.api.TopologyProcessor;

/**
 * Tests the methods of {@link ServiceEntityMapper}.
 */
public class ServiceEntityMapperTest {
    private final TopologyProcessor topologyProcessor = mock(TopologyProcessor.class);
    private final DiscoveredByCache discoveredByCache = mock(DiscoveredByCache.class);
    private final ServiceEntityMapper mapper = new ServiceEntityMapper(topologyProcessor);
    private final static long TARGET_ID = 10L;
    private final static String TARGET_DISPLAY_NAME = "display name";
    private final static String PROBE_TYPE = "probe type";

    @Test
    public void testApiToServiceEntity() {
        final ServiceEntityMapper mapper = new ServiceEntityMapper(discoveredByCache);

        TargetApiDTO targetApiDTO = new TargetApiDTO();
        targetApiDTO.setType("foo");
        when(discoveredByCache.getTargetApiDTO(TARGET_ID)).thenReturn(Optional.of(targetApiDTO));

        final String displayName = "entity display name";
        final long oid = 152L;
        final EntityType entityType = EntityType.VIRTUAL_MACHINE;
        final EntityState entityState = EntityState.POWERED_ON;
        final EnvironmentType environmentType = EnvironmentType.ON_PREM;
        final String tagKey = "tag";
        final String tagValue = "value";

        final ApiPartialEntity apiEntity =
            ApiPartialEntity.newBuilder()
                .setDisplayName(displayName)
                .setOid(oid)
                .setEntityType(entityType.getNumber())
                .setEntityState(entityState)
                .setEnvironmentType(environmentType)
                .addDiscoveringTargetIds(TARGET_ID)
                .setTags(
                    Tags.newBuilder()
                        .putTags(
                            tagKey,
                            TagValuesDTO.newBuilder().addValues(tagValue).build()))
                .build();

        final ServiceEntityApiDTO serviceEntityApiDTO =
            mapper.toServiceEntityApiDTO(apiEntity);

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

        assertThat(serviceEntityApiDTO.getDiscoveredBy(), is(targetApiDTO));
    }

    @Test
    public void testSetBasicMinimalFields() {
        MinimalEntity minimalEntity = MinimalEntity.newBuilder()
            .setOid(7L)
            .setDisplayName("foo")
            .setEntityType(UIEntityType.VIRTUAL_MACHINE.typeNumber())
            .build();

        ServiceEntityApiDTO dto = ServiceEntityMapper.toBasicEntity(minimalEntity);

        assertThat(dto.getUuid(), is("7"));
        assertThat(dto.getDisplayName(), is("foo"));
        assertThat(dto.getClassName(), is(UIEntityType.VIRTUAL_MACHINE.apiStr()));
    }

    /**
     * Tests the correct translation from {@link TopologyEntityDTO} to {@link ServiceEntityApiDTO}.
     */
    @Test
    public void testToServiceEntityApiDTO() throws Exception {
        final ServiceEntityMapper mapper = new ServiceEntityMapper(discoveredByCache);

        TargetApiDTO targetApiDTO = new TargetApiDTO();
        targetApiDTO.setType("foo");
        when(discoveredByCache.getTargetApiDTO(TARGET_ID)).thenReturn(Optional.of(targetApiDTO));

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
            mapper.toServiceEntityApiDTO(topologyEntityDTO);

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

        assertThat(serviceEntityApiDTO.getDiscoveredBy(), is(targetApiDTO));
    }

    @Test
    public void testDiscoveryCacheLoadAndCacheTarget() throws Exception {
        mockTopologyProcessorOutputs();

        DiscoveredByCache discoveredByCache = new DiscoveredByCache(topologyProcessor);
        Optional<TargetApiDTO> dto = discoveredByCache.getTargetApiDTO(TARGET_ID);
        checkTargetApiDTO(dto.get());

        verify(topologyProcessor).getTarget(TARGET_ID);

        // THe second time should return the same thing, but not make a subsequent call.
        Optional<TargetApiDTO> dto2 = discoveredByCache.getTargetApiDTO(TARGET_ID);
        checkTargetApiDTO(dto2.get());
        verify(topologyProcessor, times(1)).getTarget(TARGET_ID);
    }

    @Test
    public void testDiscoveryCacheClearOnTargetUpdate() throws Exception {
        mockTopologyProcessorOutputs();

        DiscoveredByCache discoveredByCache = new DiscoveredByCache(topologyProcessor);
        Optional<TargetApiDTO> dto = discoveredByCache.getTargetApiDTO(TARGET_ID);
        checkTargetApiDTO(dto.get());

        verify(topologyProcessor).getTarget(TARGET_ID);

        TargetInfo targetInfo = mock(TargetInfo.class);
        when(targetInfo.getId()).thenReturn(TARGET_ID);
        discoveredByCache.onTargetChanged(targetInfo);

        Optional<TargetApiDTO> dto2 = discoveredByCache.getTargetApiDTO(TARGET_ID);
        checkTargetApiDTO(dto2.get());
        // Should have made a subsequent call.
        verify(topologyProcessor, times(2)).getTarget(TARGET_ID);
    }

    @Test
    public void testDiscoveryCacheClearOnTargetRemove() throws Exception {
        mockTopologyProcessorOutputs();

        DiscoveredByCache discoveredByCache = new DiscoveredByCache(topologyProcessor);
        Optional<TargetApiDTO> dto = discoveredByCache.getTargetApiDTO(TARGET_ID);
        checkTargetApiDTO(dto.get());

        verify(topologyProcessor).getTarget(TARGET_ID);

        discoveredByCache.onTargetRemoved(TARGET_ID);

        Optional<TargetApiDTO> dto2 = discoveredByCache.getTargetApiDTO(TARGET_ID);
        checkTargetApiDTO(dto2.get());
        // Should have made a subsequent call.
        verify(topologyProcessor, times(2)).getTarget(TARGET_ID);
    }

    private void mockTopologyProcessorOutputs() throws Exception {
        final TargetInfo targetInfo = mock(TargetInfo.class);
        final ProbeInfo probeInfo = mock(ProbeInfo.class);
        final AccountValue accountValue = mock(AccountValue.class);
        when(targetInfo.getId()).thenReturn(TARGET_ID);
        final long probeId = 11L;
        when(targetInfo.getProbeId()).thenReturn(probeId);
        when(targetInfo.getAccountData()).thenReturn(Collections.singleton(accountValue));
        when(accountValue.getName()).thenReturn(TargetInfo.TARGET_ADDRESS);
        when(accountValue.getStringValue()).thenReturn(TARGET_DISPLAY_NAME);
        when(probeInfo.getId()).thenReturn(probeId);
        when(probeInfo.getType()).thenReturn(PROBE_TYPE);
        when(topologyProcessor.getProbe(Mockito.eq(probeId))).thenReturn(probeInfo);
        when(topologyProcessor.getTarget(Mockito.eq(TARGET_ID))).thenReturn(targetInfo);
    }

    private void checkTargetApiDTO(TargetApiDTO targetApiDTO) {
        Assert.assertNotNull(targetApiDTO);
        Assert.assertEquals(TARGET_ID, (long)Long.valueOf(targetApiDTO.getUuid()));
        Assert.assertEquals(TARGET_DISPLAY_NAME, targetApiDTO.getDisplayName());
        Assert.assertEquals(PROBE_TYPE, targetApiDTO.getType());
    }
}
