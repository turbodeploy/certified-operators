package com.vmturbo.cloud.commitment.analysis.pricing;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.cloud.commitment.analysis.demand.ComputeTierDemand;
import com.vmturbo.cloud.commitment.analysis.demand.ScopedCloudTierInfo;
import com.vmturbo.cloud.commitment.analysis.pricing.DefaultPricingAnalyzer.DefaultPricingAnalyzerFactory;
import com.vmturbo.cloud.commitment.analysis.spec.ReservedInstanceSpecData;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.cost.Pricing.ReservedInstancePriceTable;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostDataRetrievalException;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.integration.PricingResolver;
import com.vmturbo.cost.calculation.pricing.CloudRateExtractor;
import com.vmturbo.cost.calculation.pricing.CloudRateExtractor.CloudRateExtractorFactory;
import com.vmturbo.cost.calculation.pricing.CloudRateExtractor.ComputePriceBundle;
import com.vmturbo.cost.calculation.pricing.CloudRateExtractor.CoreBasedLicensePriceBundle;
import com.vmturbo.cost.calculation.pricing.ImmutableCoreBasedLicensePriceBundle;
import com.vmturbo.cost.calculation.topology.AccountPricingData;
import com.vmturbo.cost.calculation.topology.TopologyEntityInfoExtractor;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;
import com.vmturbo.platform.sdk.common.PricingDTO.Price;
import com.vmturbo.platform.sdk.common.PricingDTO.ReservedInstancePrice;

public class DefaultPricingAnalyzerTest {

    private final PricingResolver<TopologyEntityDTO> pricingResolver = mock(PricingResolver.class);

    private final TopologyEntityInfoExtractor entityInfoExtractor = mock(TopologyEntityInfoExtractor.class);

    private final CloudRateExtractorFactory cloudRateExtractorFactory = mock(CloudRateExtractorFactory.class);

    private final CloudRateExtractor cloudRateExtractor = mock(CloudRateExtractor.class);

    private final DefaultPricingAnalyzerFactory pricingAnalyzerFactory =
            new DefaultPricingAnalyzerFactory(pricingResolver, entityInfoExtractor, cloudRateExtractorFactory);

    private final AccountPricingData accountPricingData = mock(AccountPricingData.class);

    private final long accountOid = 1;

    private final long reservedInstanceSpecId = 2;

    private final ReservedInstancePriceTable riPriceTable = ReservedInstancePriceTable.newBuilder()
            .putRiPricesBySpecId(reservedInstanceSpecId, ReservedInstancePrice.newBuilder()
                    .setUpfrontPrice(Price.newBuilder()
                            .setPriceAmount(CurrencyAmount.newBuilder().setAmount(12 * 730 * 2.0).build())
                            .build())
                    .setRecurringPrice(Price.newBuilder()
                            .setPriceAmount(CurrencyAmount.newBuilder().setAmount(3.0).build())
                            .build())
                    .build())
            .build();

    private final CloudTopology<TopologyEntityDTO> cloudTopology = mock(CloudTopology.class);

    private final ScopedCloudTierInfo cloudTierInfo = ScopedCloudTierInfo.builder()
            .accountOid(1)
            .regionOid(2)
            .serviceProviderOid(3)
            .cloudTierDemand(ComputeTierDemand.builder()
                    .cloudTierOid(4)
                    .osType(OSType.LINUX)
                    .tenancy(Tenancy.DEFAULT)
                    .build())
            .build();

    private final TopologyEntityDTO cloudTier = TopologyEntityDTO.newBuilder()
            .setOid(cloudTierInfo.cloudTierDemand().cloudTierOid())
            .setEntityType(EntityType.COMPUTE_TIER_VALUE)
            .build();

    @Before
    public void setup() throws CloudCostDataRetrievalException {
        when(cloudRateExtractorFactory.newRateExtractor(any(), any())).thenReturn(cloudRateExtractor);
        when(pricingResolver.getAccountPricingDataByBusinessAccount(any())).thenReturn(
                ImmutableMap.of(accountOid, accountPricingData));
        when(pricingResolver.getRIPriceTablesByAccount(any())).thenReturn(
                ImmutableMap.of(accountOid, riPriceTable));

        when(cloudTopology.getEntity(eq(cloudTier.getOid()))).thenReturn(Optional.of(cloudTier));
    }

    /**
     * Test not finding the cloud tier for on-demand rates.
     */
    @Test
    public void testOnDemandTierNotFound() throws CloudCostDataRetrievalException {

        // setup the cloud tier
        when(cloudTopology.getEntity(anyLong())).thenReturn(Optional.empty());

        // setup the pricing analyzer
        final CloudCommitmentPricingAnalyzer pricingAnalyzer = pricingAnalyzerFactory.newPricingAnalyzer(cloudTopology);

        final CloudTierPricingData pricingData = pricingAnalyzer.getTierDemandPricing(cloudTierInfo);

        assertTrue(pricingData.isEmpty());
    }

    /**
     * Test not finding the pricing data for the requested account.
     */
    @Test
    public void testOnDemandPricingDataNotFound() throws CloudCostDataRetrievalException {

        // setup the cloud tier info
        final ScopedCloudTierInfo cloudTierInfoB = ScopedCloudTierInfo.builder()
                .from(cloudTierInfo)
                .accountOid(123)
                .build();

        // setup the pricing analyzer
        final CloudCommitmentPricingAnalyzer pricingAnalyzer = pricingAnalyzerFactory.newPricingAnalyzer(cloudTopology);

        final CloudTierPricingData pricingData = pricingAnalyzer.getTierDemandPricing(cloudTierInfoB);

        assertTrue(pricingData.isEmpty());
    }

    @Test
    public void testComputeTierPriceBundleNotFound() throws CloudCostDataRetrievalException {

        // setup the pricing analyzer
        final CloudCommitmentPricingAnalyzer pricingAnalyzer = pricingAnalyzerFactory.newPricingAnalyzer(cloudTopology);

        final CloudTierPricingData pricingData = pricingAnalyzer.getTierDemandPricing(cloudTierInfo);

        assertTrue(pricingData.isEmpty());
    }

    @Test
    public void testComputeTierPricingMatch() throws CloudCostDataRetrievalException {

        // setup the compute price bundle
        final ComputePriceBundle computePriceBundle = ComputePriceBundle.newBuilder()
                .addPrice(cloudTierInfo.accountOid(),
                        ((ComputeTierDemand)cloudTierInfo.cloudTierDemand()).osType(),
                        2.0,
                        1.0,
                        false)
                .build();
        when(cloudRateExtractor.getComputePriceBundle(any(), anyLong(), any())).thenReturn(computePriceBundle);

        // setup the pricing analyzer
        final CloudCommitmentPricingAnalyzer pricingAnalyzer = pricingAnalyzerFactory.newPricingAnalyzer(cloudTopology);

        final CloudTierPricingData pricingData = pricingAnalyzer.getTierDemandPricing(cloudTierInfo);

        // Assertions
        final ComputeTierPricingData expectedPricingData = ComputeTierPricingData.builder()
                .onDemandComputeRate(2.0)
                .onDemandLicenseRate(1.0)
                .build();
        assertFalse(pricingData.isEmpty());
        assertThat(pricingData, equalTo(expectedPricingData));
    }

    @Test
    public void testComputeTierMatchWithReservedLicense() throws CloudCostDataRetrievalException {

        // setup the compute price bundle
        final ComputePriceBundle computePriceBundle = ComputePriceBundle.newBuilder()
                .addPrice(cloudTierInfo.accountOid(),
                        ((ComputeTierDemand)cloudTierInfo.cloudTierDemand()).osType(),
                        2.0,
                        1.0,
                        false)
                .build();
        when(cloudRateExtractor.getComputePriceBundle(any(), anyLong(), any())).thenReturn(computePriceBundle);

        // setup reserved license bundle
        final CoreBasedLicensePriceBundle licensePriceBundle = ImmutableCoreBasedLicensePriceBundle.builder()
                .licenseCommodityType(CommodityType.newBuilder()
                        .setType(CommodityDTO.CommodityType.LICENSE_ACCESS_VALUE)
                        .build())
                .isBurstableCPU(false)
                .osType(((ComputeTierDemand)cloudTierInfo.cloudTierDemand()).osType())
                .numCores(1)
                .price(3.0)
                .build();
        when(cloudRateExtractor.getReservedLicensePriceBundles(any(), any())).thenReturn(ImmutableSet.of(licensePriceBundle));

        // setup the pricing analyzer
        final CloudCommitmentPricingAnalyzer pricingAnalyzer = pricingAnalyzerFactory.newPricingAnalyzer(cloudTopology);

        final CloudTierPricingData pricingData = pricingAnalyzer.getTierDemandPricing(cloudTierInfo);

        // Assertions
        final ComputeTierPricingData expectedPricingData = ComputeTierPricingData.builder()
                .onDemandComputeRate(2.0)
                .onDemandLicenseRate(1.0)
                .reservedLicenseRate(3.0)
                .build();
        assertFalse(pricingData.isEmpty());
        assertThat(pricingData, equalTo(expectedPricingData));
    }

    @Test
    public void testCloudCommitmentPricing() throws CloudCostDataRetrievalException {

        final ReservedInstanceSpecData riSpecData = mock(ReservedInstanceSpecData.class);
        when(riSpecData.specId()).thenReturn(reservedInstanceSpecId);
        when(riSpecData.spec()).thenReturn(ReservedInstanceSpec.newBuilder()
                .setReservedInstanceSpecInfo(ReservedInstanceSpecInfo.newBuilder()
                        .setType(ReservedInstanceType.newBuilder().setTermYears(1).build())
                        .build())
                .build());

        // setup the pricing analyzer
        final CloudCommitmentPricingAnalyzer pricingAnalyzer = pricingAnalyzerFactory.newPricingAnalyzer(cloudTopology);

        final CloudCommitmentPricingData commitmentPricingData = pricingAnalyzer.getCloudCommitmentPricing(
                riSpecData, ImmutableSet.of(accountOid, 23123L));

        final RIPricingData expectedRIPricingData = RIPricingData.builder()
                .hourlyUpFrontRate(2.0)
                .hourlyRecurringRate(3.0)
                .build();

        assertThat(commitmentPricingData, instanceOf(RIPricingData.class));

    }
}
