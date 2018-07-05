package com.vmturbo.stitching.utilities;

import static com.vmturbo.platform.common.builders.CommodityBuilders.vCpuMHz;
import static com.vmturbo.platform.common.builders.CommodityBuilders.vMemKB;
import static com.vmturbo.stitching.utilities.CopyCommodities.copyCommodities;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.CommodityBoughtMetaData;
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
        final List<CommodityDTO.Builder> sourceProviderList = new ArrayList<>();
        sourceProviderList.add(vCpuMHz().build().toBuilder());

        final List<CommodityDTO.Builder> destProviderList = new ArrayList<>();
        destProviderList.add(vCpuMHz().build().toBuilder());
        final Map<StitchingEntity, List<CommodityDTO.Builder>> destProviderMap = new HashMap<>();
        destProviderMap.put(provider, destProviderList);

        when(sourceEntity.getCommoditiesBoughtByProvider()).thenReturn(ImmutableMap.of(provider, sourceProviderList));
        when(destinationEntity.getCommoditiesBoughtByProvider()).thenReturn(destProviderMap);

        assertEquals(1, destinationEntity.getCommoditiesBoughtByProvider().get(provider).size());
        copyCommodities().from(sourceEntity).to(destinationEntity);
        assertEquals(2, destinationEntity.getCommoditiesBoughtByProvider().get(provider).size());
    }

    @Test
    public void testCopyWithNewProvider() {
        final List<CommodityDTO.Builder> sourceProviderList = new ArrayList<>();
        sourceProviderList.add(vCpuMHz().build().toBuilder());

        final Map<StitchingEntity, List<CommodityDTO.Builder>> destProviderMap = new HashMap<>();
        when(sourceEntity.getCommoditiesBoughtByProvider()).thenReturn(ImmutableMap.of(provider, sourceProviderList));
        when(destinationEntity.getCommoditiesBoughtByProvider()).thenReturn(destProviderMap);

        assertNull(destinationEntity.getCommoditiesBoughtByProvider().get(provider));
        copyCommodities().from(sourceEntity).to(destinationEntity);
        assertEquals(1, destinationEntity.getCommoditiesBoughtByProvider().get(provider).size());
    }

    @Test
    public void testCopyWithExistingProviderWithBoughtMetaData() {
        final List<CommodityDTO.Builder> sourceProviderList = new ArrayList<>();
        sourceProviderList.add(vCpuMHz().build().toBuilder());
        sourceProviderList.add(vMemKB().build().toBuilder());
        // create a bought commodity meta data filter that contains only one of the two commodities
        // in the source and also contains an irrelevant commodity not provided by the source.
        final Collection<CommodityType> commodityBoughtData = ImmutableList.of(CommodityType.VCPU,
                CommodityType.BALLOONING);
        final Collection<CommodityBoughtMetaData> boughtFilter =
                ImmutableList.of(new CommodityBoughtMetaData(EntityType.PHYSICAL_MACHINE,
                        commodityBoughtData));

        final List<CommodityDTO.Builder> destProviderList = new ArrayList<>();
        destProviderList.add(vCpuMHz().build().toBuilder());
        final Map<StitchingEntity, List<CommodityDTO.Builder>> destProviderMap = new HashMap<>();
        destProviderMap.put(provider, destProviderList);

        when(sourceEntity.getCommoditiesBoughtByProvider()).thenReturn(ImmutableMap.of(provider, sourceProviderList));
        when(destinationEntity.getCommoditiesBoughtByProvider()).thenReturn(destProviderMap);
        when(provider.getEntityType()).thenReturn(EntityType.PHYSICAL_MACHINE);

        assertEquals(1, destinationEntity.getCommoditiesBoughtByProvider().get(provider).size());
        copyCommodities(boughtFilter).from(sourceEntity).to(destinationEntity);
        assertEquals(2, destinationEntity.getCommoditiesBoughtByProvider().get(provider).size());
    }

}