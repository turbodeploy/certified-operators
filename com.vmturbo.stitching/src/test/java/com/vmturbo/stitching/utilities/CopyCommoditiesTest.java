package com.vmturbo.stitching.utilities;

import static com.vmturbo.platform.common.builders.CommodityBuilders.vCpuMHz;
import static com.vmturbo.platform.common.builders.CommodityBuilders.vMemKB;
import static com.vmturbo.stitching.utilities.CopyCommodities.copyCommodities;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.CommodityBoughtMetadata;
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
    public void testCopyWithExistingProviderSameCommodity() {
        // try to copy the same commodity from source to destination
        // The number of commodities should not change, but the value of used should get written to
        // the destination commodity
        final List<CommodityDTO.Builder> sourceProviderList = Lists.newArrayList(
                vCpuMHz().used(10.0D).build().toBuilder());
        final CommoditiesBought sourceProviderCommoditiesBought = new CommoditiesBought(sourceProviderList);

        when(sourceEntity.getCommodityBoughtListByProvider()).thenReturn(ImmutableMap.of(provider,
                Lists.newArrayList(sourceProviderCommoditiesBought)));

        final List<CommodityDTO.Builder> destProviderList = Lists.newArrayList(
                vCpuMHz().build().toBuilder());
        final CommoditiesBought destProviderCommoditiesBought = new CommoditiesBought(destProviderList);
        final Map<StitchingEntity, List<CommoditiesBought>> destProviderMap = new HashMap<>();
        destProviderMap.put(provider, Lists.newArrayList(destProviderCommoditiesBought));

        when(destinationEntity.getCommodityBoughtListByProvider()).thenReturn(destProviderMap);
        when(destinationEntity.getMatchingCommoditiesBought(provider,
                sourceProviderCommoditiesBought)).thenReturn(Optional.of(
                destProviderCommoditiesBought));

        assertEquals(1,
                destinationEntity.getCommodityBoughtListByProvider().get(provider).size());
        assertEquals(0,
                destinationEntity.getCommodityBoughtListByProvider().get(provider).iterator()
                        .next().getBoughtList().get(0).getUsed(), 0.1D);
        copyCommodities().from(sourceEntity).to(destinationEntity);
        assertEquals(1,
                destinationEntity.getCommodityBoughtListByProvider().get(provider).size());
        assertEquals(10.0D,
                destinationEntity.getCommodityBoughtListByProvider().get(provider).iterator()
                        .next().getBoughtList().get(0).getUsed(), 0.1D);
    }

    @Test
    public void testCopyWithNewProvider() {
        final List<CommodityDTO.Builder> sourceProviderList = new ArrayList<>();
        sourceProviderList.add(vCpuMHz().build().toBuilder());
        final CommoditiesBought sourceProviderCommoditiesBought = new CommoditiesBought(sourceProviderList);

        when(sourceEntity.getCommodityBoughtListByProvider()).thenReturn(ImmutableMap.of(provider,
                Lists.newArrayList(sourceProviderCommoditiesBought)));
        when(destinationEntity.getCommodityBoughtListByProvider()).thenReturn(new HashMap<>());
        when(destinationEntity.getMatchingCommoditiesBought(provider,
                sourceProviderCommoditiesBought)).thenReturn(Optional.empty());

        assertNull(destinationEntity.getCommodityBoughtListByProvider().get(provider));
        copyCommodities().from(sourceEntity).to(destinationEntity);
        assertEquals(1, destinationEntity.getCommodityBoughtListByProvider().get(provider).size());
    }

    @Test
    public void testCopyWithExistingProviderWithBoughtMetaData() {
        final List<CommodityDTO.Builder> sourceProviderList = new ArrayList<>();
        sourceProviderList.add(vCpuMHz().used(20.0D).resizable(true).build().toBuilder());
        sourceProviderList.add(vMemKB().build().toBuilder());
        final CommoditiesBought sourceProviderCommoditiesBought = new CommoditiesBought(sourceProviderList);
        // create a bought commodity meta data filter that contains only one of the two commodities
        // in the source and also contains an irrelevant commodity not provided by the source.
        final Collection<CommodityType> commodityBoughtTypeList = ImmutableList.of(
                CommodityType.VCPU,
                CommodityType.BALLOONING);
        final Collection<CommodityBoughtMetadata> boughtFilter =
                ImmutableList.of(CommodityBoughtMetadata.newBuilder()
                        .setProviderType(EntityType.PHYSICAL_MACHINE)
                        .addAllCommodityMetadata(commodityBoughtTypeList).build());

        final List<CommodityDTO.Builder> destProviderList = new ArrayList<>();
        destProviderList.add(vCpuMHz().build().toBuilder());
        final CommoditiesBought destProviderCommoditiesBought = new CommoditiesBought(destProviderList);
        final Map<StitchingEntity, List<CommoditiesBought>> destProviderMap = new HashMap<>();
        destProviderMap.put(provider, Lists.newArrayList(destProviderCommoditiesBought));

        when(sourceEntity.getCommodityBoughtListByProvider()).thenReturn(ImmutableMap.of(provider,
                Lists.newArrayList(sourceProviderCommoditiesBought)));
        when(destinationEntity.getCommodityBoughtListByProvider()).thenReturn(destProviderMap);
        when(destinationEntity.getMatchingCommoditiesBought(provider,
                sourceProviderCommoditiesBought)).thenReturn(Optional.of(
                destProviderCommoditiesBought));
        when(provider.getEntityType()).thenReturn(EntityType.PHYSICAL_MACHINE);

        assertEquals(1, destinationEntity.getCommodityBoughtListByProvider().get(provider).size());
        copyCommodities(boughtFilter).from(sourceEntity).to(destinationEntity);
        assertEquals(1, destinationEntity.getCommodityBoughtListByProvider().get(provider).size());
        assertEquals(20.0D,
                destinationEntity.getCommodityBoughtListByProvider().get(provider).iterator().next()
                        .getBoughtList().get(0).getUsed(), 0.1D);
        assertFalse(destinationEntity.getCommodityBoughtListByProvider().get(provider).iterator()
                .next().getBoughtList().get(0).hasReservation());
    }
}