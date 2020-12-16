package com.vmturbo.api.component.external.api.mapper;

import static com.vmturbo.api.component.external.api.mapper.EntityDetailsMapper.ENHANCED_BY_PROP;
import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.api.dto.entity.DetailDataApiDTO;
import com.vmturbo.api.dto.entity.EntityDetailsApiDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PerTargetEntityInformation;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.DiscoveryOrigin;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityOrigin;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;
import com.vmturbo.topology.processor.api.util.ThinTargetCache.ThinProbeInfo;
import com.vmturbo.topology.processor.api.util.ThinTargetCache.ThinTargetInfo;

/**
 * Tests the methods of {@link EntityDetailsMapper}.
 */
public class EntityDetailsMapperTest {

    private final EntityDetailsMapper entityDetailsMapper = new EntityDetailsMapper(Mockito.mock(ThinTargetCache.class));

    private static final String PROBE_TYPE_APPDY = "AppDynamics";
    private static final String PROBE_TYPE_AWS = "AWS";
    private static final String PROBE_STORAGE_BROWSING =  "vCenter Storage Browsing";
    private static final long TARGET_ID_1 = 1L;
    private static final long TARGET_ID_2 = 2L;

    /**
     * Tests the correct translation from {@link TopologyEntityDTO} to {@link EntityDetailsApiDTO}.
     */
    @Test
    public void testToEntityDetailsApiDTO() {
        final long oid = 1L;

        final TopologyEntityDTO topologyEntityDTO = TopologyEntityDTO.newBuilder()
                .setOid(oid)
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .build();

        final EntityDetailsApiDTO result =
                entityDetailsMapper.toEntityDetails(topologyEntityDTO);

        assertEquals(oid, result.getUuid());
    }

    /**
     * Tests the correct processing of the "Enhanced by" property.
     * Case: the topology entity has 1 secondary target.
     */
    @Test
    public void testEnhancedByProperty() {
        final ThinTargetCache targetCache = Mockito.mock(ThinTargetCache.class);
        final EntityDetailsMapper mapper = new EntityDetailsMapper(targetCache);
        final TopologyEntityDTO entityDTO = createTopologyDto(new long[]{TARGET_ID_1});

        final ThinTargetInfo targetInfo = getTargetInfo(PROBE_TYPE_APPDY);
        Mockito.when(targetCache.getTargetInfo(TARGET_ID_1)).thenReturn(Optional.of(targetInfo));

        final EntityDetailsApiDTO detailsApiDTO = mapper.toEntityDetails(entityDTO);
        validateDataDtoEnhancedProperty(detailsApiDTO, PROBE_TYPE_APPDY);
    }

    /**
     * Tests the correct processing of the "Enhanced by" property.
     * Case: the topology entity has 2 secondary targets.
     */
    @Test
    public void testEnhancedByPropertyTwoSecondaryTargets() {

        final ThinTargetCache targetCache = Mockito.mock(ThinTargetCache.class);
        final EntityDetailsMapper mapper = new EntityDetailsMapper(targetCache);

        final TopologyEntityDTO entityDTO = createTopologyDto(new long[]{TARGET_ID_1, TARGET_ID_2});
        final ThinTargetInfo targetInfo1 = getTargetInfo(PROBE_TYPE_APPDY);
        final ThinTargetInfo targetInfo2 = getTargetInfo(PROBE_TYPE_AWS);
        Mockito.when(targetCache.getTargetInfo(TARGET_ID_1)).thenReturn(Optional.of(targetInfo1));
        Mockito.when(targetCache.getTargetInfo(TARGET_ID_2)).thenReturn(Optional.of(targetInfo2));

        final EntityDetailsApiDTO detailsApiDTO = mapper.toEntityDetails(entityDTO);
        final String expectedEnhancedPropValue = PROBE_TYPE_APPDY + ", " + PROBE_TYPE_AWS;
        validateDataDtoEnhancedProperty(detailsApiDTO, expectedEnhancedPropValue);

    }

    /**
     * Test the handling of hidden targets.
     * They are not added to the "Enhanced by" property.
     */
    @Test
    public void testNotShowingHiddenTargets() {

        final ThinTargetCache targetCache = Mockito.mock(ThinTargetCache.class);
        final EntityDetailsMapper mapper = new EntityDetailsMapper(targetCache);

        final TopologyEntityDTO entityDTO = createTopologyDto(new long[]{TARGET_ID_1});
        final ThinTargetInfo targetInfo = Mockito.mock(ThinTargetInfo.class);
        final ThinProbeInfo probeInfo = Mockito.mock(ThinProbeInfo.class);
        Mockito.when(probeInfo.type()).thenReturn(PROBE_STORAGE_BROWSING);
        Mockito.when(targetInfo.probeInfo()).thenReturn(probeInfo);
        Mockito.when(targetInfo.isHidden()).thenReturn(true);
        Mockito.when(targetCache.getTargetInfo(TARGET_ID_1)).thenReturn(Optional.of(targetInfo));

        final EntityDetailsApiDTO detailsApiDTO = mapper.toEntityDetails(entityDTO);
        Assert.assertNotNull(detailsApiDTO.getDetails());
        Assert.assertTrue(detailsApiDTO.getDetails().isEmpty());

    }

    private void validateDataDtoEnhancedProperty(@Nonnull EntityDetailsApiDTO detailsApiDTO,
                                                 @Nonnull String expectedEnhancedPropValue) {
        Assert.assertNotNull(detailsApiDTO.getDetails());

        final List<DetailDataApiDTO> details = detailsApiDTO.getDetails();
        Assert.assertFalse(details.isEmpty());

        final DetailDataApiDTO enhancedPropDto = details.stream()
                .filter(d -> d.getKey().equals(ENHANCED_BY_PROP)).findFirst().orElse(null);
        Assert.assertNotNull(enhancedPropDto);
        Assert.assertEquals(expectedEnhancedPropValue, enhancedPropDto.getValue());
    }

    private ThinTargetInfo getTargetInfo(@Nonnull String probeCategory) {
        final ThinTargetInfo targetInfo = Mockito.mock(ThinTargetInfo.class);
        final ThinProbeInfo probeInfo = Mockito.mock(ThinProbeInfo.class);
        Mockito.when(probeInfo.type()).thenReturn(probeCategory);
        Mockito.when(targetInfo.probeInfo()).thenReturn(probeInfo);
        return targetInfo;
    }

    private TopologyEntityDTO createTopologyDto(long[] secondaryTargetsIds) {
        DiscoveryOrigin.Builder originBuilder = DiscoveryOrigin.newBuilder()
                .putDiscoveredTargetData(10L, PerTargetEntityInformation.newBuilder()
                        .setOrigin(EntityOrigin.DISCOVERED).build());
        for (long targetId : secondaryTargetsIds) {
            originBuilder.putDiscoveredTargetData(targetId, PerTargetEntityInformation.newBuilder()
                    .setOrigin(EntityOrigin.PROXY).build());
        }
        return TopologyEntityDTO.newBuilder()
                .setOid(100L)
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setOrigin(TopologyEntityDTO.Origin.newBuilder()
                        .setDiscoveryOrigin(originBuilder.build())
                        .build())
                .build();
    }
}
