package com.vmturbo.market.runner.cost;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.cost.Pricing.OnDemandPriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTable;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.DiscountApplicator;
import com.vmturbo.cost.calculation.DiscountApplicator.DiscountApplicatorFactory;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor;
import com.vmturbo.market.runner.cost.MarketPriceTable.ComputePriceBundle;
import com.vmturbo.market.runner.cost.MarketPriceTable.ComputePriceBundle.ComputePrice;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.PricingDTO.ComputeTierPriceList;
import com.vmturbo.platform.sdk.common.PricingDTO.ComputeTierPriceList.ComputeTierConfigPrice;
import com.vmturbo.platform.sdk.common.PricingDTO.Price;

public class MarketPriceTableTest {

    private static final long TIER_ID = 7;

    private static final long REGION_ID = 8;

    private static final double LINUX_PRICE = 10;

    private static final double SUSE_PRICE_ADJUSTMENT = 5;

    private static final PriceTable PRICE_TABLE = PriceTable.newBuilder()
        .putOnDemandPriceByRegionId(REGION_ID, OnDemandPriceTable.newBuilder()
            .putComputePricesByTierId(TIER_ID, ComputeTierPriceList.newBuilder()
                .setBasePrice(ComputeTierConfigPrice.newBuilder()
                    .setGuestOsType(OSType.LINUX)
                    .addPrices(Price.newBuilder()
                        .setPriceAmount(CurrencyAmount.newBuilder().setAmount(LINUX_PRICE))))
                .addPerConfigurationPriceAdjustments(ComputeTierConfigPrice.newBuilder()
                    .setGuestOsType(OSType.SUSE)
                    .addPrices(Price.newBuilder()
                        .setPriceAmount(CurrencyAmount.newBuilder().setAmount(SUSE_PRICE_ADJUSTMENT))))
                .build())
            .build())
        .build();

    private static final TopologyEntityDTO TIER = TopologyEntityDTO.newBuilder()
        .setEntityType(EntityType.COMPUTE_TIER_VALUE)
        .setOid(TIER_ID)
        .build();

    private static final TopologyEntityDTO REGION = TopologyEntityDTO.newBuilder()
        .setEntityType(EntityType.REGION_VALUE)
        .setOid(REGION_ID)
        .build();

    private CloudCostData cloudCostData = mock(CloudCostData.class);

    private CloudTopology<TopologyEntityDTO> topology = mock(CloudTopology.class);

    private EntityInfoExtractor<TopologyEntityDTO> infoExtractor = mock(EntityInfoExtractor.class);

    private DiscountApplicatorFactory<TopologyEntityDTO> discountApplicatorFactory =
            mock(DiscountApplicatorFactory.class);

    @Before
    public void setup() {
        when(cloudCostData.getPriceTable()).thenReturn(PRICE_TABLE);
        when(topology.getEntity(REGION_ID)).thenReturn(Optional.of(REGION));
        when(topology.getEntity(TIER_ID)).thenReturn(Optional.of(TIER));
    }

    @Test
    public void testComputePriceBundleNoDiscount() {
        final long baId = 7L;
        doReturn(ImmutableMap.of(baId, makeBusinessAccount(baId, DiscountApplicator.noDiscount())))
            .when(topology).getEntities();
        final MarketPriceTable mktPriceTable = new MarketPriceTable(cloudCostData, topology,
                infoExtractor, discountApplicatorFactory);
        final ComputePriceBundle priceBundle = mktPriceTable.getComputePriceBundle(TIER_ID, REGION_ID);
        assertThat(priceBundle.getPrices(), containsInAnyOrder(
                new ComputePrice(baId, OSType.LINUX, LINUX_PRICE),
                new ComputePrice(baId, OSType.SUSE, LINUX_PRICE + SUSE_PRICE_ADJUSTMENT)));
    }

    @Test
    public void testComputePriceBundleWithDiscount() {
        final long noDiscountBaId = 7L;
        final DiscountApplicator<TopologyEntityDTO> noDiscount = DiscountApplicator.noDiscount();

        final long discountBaId = 17L;
        final DiscountApplicator<TopologyEntityDTO> discount = mock(DiscountApplicator.class);
        when(discount.getDiscountPercentage(TIER_ID)).thenReturn(0.2);

        doReturn(ImmutableMap.of(
                noDiscountBaId, makeBusinessAccount(noDiscountBaId, noDiscount),
                discountBaId, makeBusinessAccount(discountBaId, discount)))
            .when(topology).getEntities();

        final MarketPriceTable mktPriceTable = new MarketPriceTable(cloudCostData, topology,
                infoExtractor, discountApplicatorFactory);
        final ComputePriceBundle priceBundle = mktPriceTable.getComputePriceBundle(TIER_ID, REGION_ID);
        assertThat(priceBundle.getPrices(), containsInAnyOrder(
                new ComputePrice(noDiscountBaId, OSType.LINUX, LINUX_PRICE),
                new ComputePrice(noDiscountBaId, OSType.SUSE, LINUX_PRICE + SUSE_PRICE_ADJUSTMENT),
                new ComputePrice(discountBaId, OSType.LINUX, LINUX_PRICE * 0.8),
                new ComputePrice(discountBaId, OSType.SUSE, (LINUX_PRICE + SUSE_PRICE_ADJUSTMENT) * 0.8)
        ));
    }

    private TopologyEntityDTO makeBusinessAccount(final long id,
              final DiscountApplicator<TopologyEntityDTO> discountApplicator) {
        final TopologyEntityDTO businessAccount = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
                .setOid(id)
                .build();
        doReturn(discountApplicator).when(discountApplicatorFactory).accountDiscountApplicator(id, topology, infoExtractor, cloudCostData);
        return businessAccount;
    }
}
