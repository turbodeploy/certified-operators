package com.vmturbo.stitching.utilities;

import static com.vmturbo.platform.common.builders.CommodityBuilders.vCpuMHz;
import static com.vmturbo.stitching.utilities.CopyCommodities.copyCommodities;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.stitching.StitchingEntity;

/**
 * Tests for {@link CopyCommodities}.
 */
public class CopyCommoditiesTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private final StitchingEntity sourceEntity = mock(StitchingEntity.class);
    private final StitchingEntity destinationEntity = mock(StitchingEntity.class);
    private final StitchingEntity provider = mock(StitchingEntity.class);

    @Test
    public void testCopyWithExistingProvider() {
        final List<CommodityDTO> sourceProviderList = new ArrayList<>();
        sourceProviderList.add(vCpuMHz().build());

        final List<CommodityDTO> destProviderList = new ArrayList<>();
        destProviderList.add(vCpuMHz().build());
        final Map<StitchingEntity, List<CommodityDTO>> destProviderMap = new HashMap<>();
        destProviderMap.put(provider, destProviderList);

        when(sourceEntity.getCommoditiesBoughtByProvider()).thenReturn(ImmutableMap.of(provider, sourceProviderList));
        when(destinationEntity.getCommoditiesBoughtByProvider()).thenReturn(destProviderMap);

        assertEquals(1, destinationEntity.getCommoditiesBoughtByProvider().get(provider).size());
        copyCommodities().from(sourceEntity).to(destinationEntity);
        assertEquals(2, destinationEntity.getCommoditiesBoughtByProvider().get(provider).size());
    }

    @Test
    public void testCopyWithNewProvider() {
        final List<CommodityDTO> sourceProviderList = new ArrayList<>();
        sourceProviderList.add(vCpuMHz().build());

        final Map<StitchingEntity, List<CommodityDTO>> destProviderMap = new HashMap<>();
        when(sourceEntity.getCommoditiesBoughtByProvider()).thenReturn(ImmutableMap.of(provider, sourceProviderList));
        when(destinationEntity.getCommoditiesBoughtByProvider()).thenReturn(destProviderMap);

        assertNull(destinationEntity.getCommoditiesBoughtByProvider().get(provider));
        copyCommodities().from(sourceEntity).to(destinationEntity);
        assertEquals(1, destinationEntity.getCommoditiesBoughtByProvider().get(provider).size());
    }
}