package com.vmturbo.api.component.external.api.mapper;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;

import javax.annotation.Nonnull;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.processor.api.TopologyProcessor;
import com.vmturbo.topology.processor.api.util.ImmutableThinProbeInfo;
import com.vmturbo.topology.processor.api.util.ImmutableThinTargetInfo;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;
import com.vmturbo.topology.processor.api.util.ThinTargetCache.ThinTargetInfo;

/**
 * Tests the methods of {@link ServiceEntityMapper}.
 */
public class ServiceEntityMapperTest {
    private final TopologyProcessor topologyProcessor = mock(TopologyProcessor.class);
    private final ThinTargetCache targetCache = mock(ThinTargetCache.class);
    private final static long TARGET_ID = 10L;
    private final static String TARGET_DISPLAY_NAME = "display name";
    private final static String PROBE_TYPE = "probe type";
    private final static String PROBE_CATEGORY = "probe category";
    private final static long PROBE_ID = 123123123;

    @Before
    public void setup() {
        final ThinTargetInfo thinTargetInfo = ImmutableThinTargetInfo.builder()
            .probeInfo(ImmutableThinProbeInfo.builder()
                .category(PROBE_CATEGORY)
                .type(PROBE_TYPE)
                .oid(PROBE_ID)
                .build())
            .displayName(TARGET_DISPLAY_NAME)
            .oid(TARGET_ID)
            .build();
        when(targetCache.getTargetInfo(TARGET_ID)).thenReturn(Optional.of(thinTargetInfo));
    }

    @Test
    public void testApiToServiceEntity() {
        final ServiceEntityMapper mapper = new ServiceEntityMapper(targetCache);

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

        checkDiscoveredBy(serviceEntityApiDTO.getDiscoveredBy());
    }

    private void checkDiscoveredBy(@Nonnull final TargetApiDTO targetApiDTO) {
        assertThat(targetApiDTO.getUuid(), is(Long.toString(TARGET_ID)));
        assertThat(targetApiDTO.getDisplayName(), is(TARGET_DISPLAY_NAME));
        assertThat(targetApiDTO.getCategory(), is(PROBE_CATEGORY));
        assertThat(targetApiDTO.getType(), is(PROBE_TYPE));
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
        final ServiceEntityMapper mapper = new ServiceEntityMapper(targetCache);

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

        checkDiscoveredBy(serviceEntityApiDTO.getDiscoveredBy());
    }

}
