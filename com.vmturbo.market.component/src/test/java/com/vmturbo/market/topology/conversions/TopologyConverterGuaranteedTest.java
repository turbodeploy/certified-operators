package com.vmturbo.market.topology.conversions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableSet;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
import com.vmturbo.cost.calculation.pricing.CloudRateExtractor;
import com.vmturbo.market.runner.FakeEntityCreator;
import com.vmturbo.market.topology.conversions.ConsistentScalingHelper.ConsistentScalingHelperFactory;
import com.vmturbo.market.topology.conversions.TierExcluder.TierExcluderFactory;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.test.utils.FeatureFlagTestRule;

/**
 * Tests for proper handling of {@link TopologyConverter} inclodeVDC argument
 * and proper handling of guaranteed buyers.
 */
public class TopologyConverterGuaranteedTest {

    private static final TopologyInfo REALTIME_TOPOLOGY_INFO =  TopologyInfo.newBuilder()
            .setTopologyType(TopologyType.REALTIME)
            .build();

    private static final long VDC1_OID = 50001;
    private static final long VDC2_OID = 50002;
    private static final long DP1_OID = 60001;
    private static final long DPOD_OID = 50003;
    private static final long HOST_OID = 50004;
    private static final long VM1_OID = 70001;
    private static final long VM2_OID = 70002;
    private static final CommodityType MEM_ALLOC = CommodityType.newBuilder()
            .setType(CommodityDTO.CommodityType.MEM_ALLOCATION_VALUE)
            .build();
    private static Map<Long, TopologyEntityDTO> entities;

    private CloudRateExtractor marketCloudRateExtractor = mock(CloudRateExtractor.class);

    private CloudCostData ccd = mock(CloudCostData.class);

    private TierExcluderFactory tierExcluderFactory = mock(TierExcluderFactory.class);
    private ConsistentScalingHelperFactory consistentScalingHelperFactory =
            mock(ConsistentScalingHelperFactory.class);
    private ReversibilitySettingFetcher reversibilitySettingFetcher =
            mock(ReversibilitySettingFetcher.class);

    /**
     * Rule to manage feature flag enablement.
     */
    @Rule
    public FeatureFlagTestRule mergedPeakFeatureFlag =
            new FeatureFlagTestRule(FeatureFlags.ENABLE_MERGED_PEAK_UPDATE_FUNCTION);

    /**
     * Create a topology with two VDCs, one that qualifies as guaranteed buyer and one that doesn't,
     * a DPod and a PM.
     */
    @Before
    public void setup() {
        IdentityGenerator.initPrefix(0);
        // Not a guaranteed buyer
        TopologyEntityDTO vdc1 = TopologyEntityDTO.newBuilder()
                        .setOid(VDC1_OID)
                        .setEntityType(EntityType.VIRTUAL_DATACENTER_VALUE)
                        .build();
        // Guaranteed buyer because it consumes from a host
        TopologyEntityDTO vdc2 = TopologyEntityDTO.newBuilder()
                        .setOid(VDC2_OID)
                        .setEntityType(EntityType.VIRTUAL_DATACENTER_VALUE)
                        .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                            .setProviderEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                            .setProviderId(HOST_OID)
                            .addCommodityBought(CommodityBoughtDTO.newBuilder()
                                .setCommodityType(MEM_ALLOC)))
                        .build();
        final TopologyEntityDTO dp1 = TopologyEntityDTO.newBuilder()
                        .setOid(DP1_OID)
                        .setEntityType(EntityType.VIRTUAL_DATACENTER_VALUE)
                        .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                                        .setProviderEntityType(EntityType.VIRTUAL_DATACENTER_VALUE)
                                        .setProviderId(VDC2_OID)
                                        .addCommodityBought(CommodityBoughtDTO.newBuilder()
                                                        .setCommodityType(MEM_ALLOC)))
                        .build();
        // Guaranteed buyer because it is a DPod
        TopologyEntityDTO dpod = TopologyEntityDTO.newBuilder()
                        .setOid(DPOD_OID)
                        .setEntityType(EntityType.DPOD_VALUE)
                        .build();
        // Not a guaranteed buyer (sells to one)
        TopologyEntityDTO pm = TopologyEntityDTO.newBuilder()
                        .setOid(HOST_OID)
                        .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                        .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                            .setCommodityType(MEM_ALLOC)
                            .build())
                        .build();
        TopologyEntityDTO vm1 = TopologyEntityDTO.newBuilder()
                        .setOid(VM1_OID)
                        .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                        .setEntityState(EntityState.UNKNOWN)
                        .build();
        TopologyEntityDTO vm2 = TopologyEntityDTO.newBuilder()
                        .setOid(VM2_OID)
                        .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                        .setEntityState(EntityState.MAINTENANCE)
                        .build();
        entities = Stream.of(vdc1, vdc2, dp1, dpod, pm, vm1, vm2)
                .collect(Collectors.toMap(TopologyEntityDTO::getOid, Function.identity()));
        when(ccd.getExistingRiBought()).thenReturn(new ArrayList());
        when(tierExcluderFactory.newExcluder(any(), any(), any())).thenReturn(mock(TierExcluder.class));
        ConsistentScalingHelper consistentScalingHelper = mock(ConsistentScalingHelper.class);
        when(consistentScalingHelper.getScalingGroupId(any())).thenReturn(Optional.empty());
        when(consistentScalingHelper.getScalingGroupUsage(any())).thenReturn(Optional.empty());
        when(consistentScalingHelperFactory.newConsistentScalingHelper(any(), any()))
            .thenReturn(consistentScalingHelper);
    }

    /**
     * Test the converter when includeGuaranteedBuyers is false.
     */
    @Test
    public void testExcludeVDCs() {
        // includeVDC is false
        TopologyConverter converter =
            new TopologyConverter(REALTIME_TOPOLOGY_INFO, marketCloudRateExtractor, ccd,
                CommodityIndex.newFactory(), tierExcluderFactory, consistentScalingHelperFactory,
                    reversibilitySettingFetcher);
        Collection<TraderTO> traders = converter.convertToMarket(entities);
        /*
         VDCs and DPs are skipped, VMs in maintenance and unknown state are not skipped
         for trader creation
         */
        assertEquals(3, traders.size());
        List<Long> traderOids = traders.stream().map(TraderTO::getOid).collect(Collectors.toList());
        assertFalse(traderOids.contains(DPOD_OID));
        assertTrue(traderOids.contains(HOST_OID));
        List<Long> guaranteedBuyers = traders.stream()
                        .filter(trader ->  trader.getSettings().getGuaranteedBuyer())
                        .map(TraderTO::getOid).collect(Collectors.toList());
        assertEquals(0, guaranteedBuyers.size());
        assertFalse(guaranteedBuyers.contains(DPOD_OID));
    }

    /**
     * Test the converter when includeGuaranteedBuyers is true.
     */
    @Test
    public void testIncludeVDCs() {
        TopologyConverter converter =
            new TopologyConverter(REALTIME_TOPOLOGY_INFO, true,
                MarketAnalysisUtils.QUOTE_FACTOR, MarketAnalysisUtils.LIVE_MARKET_MOVE_COST_FACTOR,
                marketCloudRateExtractor, ccd, CommodityIndex.newFactory(), tierExcluderFactory,
                consistentScalingHelperFactory, reversibilitySettingFetcher, MarketAnalysisUtils.PRICE_WEIGHT_SCALE,
                false, Mockito.mock(FakeEntityCreator.class));
        Collection<TraderTO> traders = converter.convertToMarket(entities);
        assertEquals(7, traders.size());
        final Collection<Long> guaranteedBuyers = traders.stream()
            .filter(trader ->  trader.getSettings().getGuaranteedBuyer())
            .map(TraderTO::getOid).collect(Collectors.toSet());
        assertEquals(2, guaranteedBuyers.size());
        Assert.assertThat(guaranteedBuyers,
                        CoreMatchers.is(ImmutableSet.of(VDC2_OID, DPOD_OID)));
    }
}
