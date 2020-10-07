package com.vmturbo.cost.component.cca;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.cloud.commitment.analysis.demand.ComputeTierDemand;
import com.vmturbo.cloud.commitment.analysis.demand.ScopedCloudTierDemand;
import com.vmturbo.cloud.commitment.analysis.pricing.CCAPriceHolder;
import com.vmturbo.cloud.commitment.analysis.pricing.CloudCommitmentPricingData;
import com.vmturbo.cloud.commitment.analysis.pricing.RIPricingData;
import com.vmturbo.cloud.commitment.analysis.pricing.TierDemandPricingData;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.DemandClassification;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.AggregateCloudTierDemand;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.AggregateCloudTierDemand.EntityInfo;
import com.vmturbo.cloud.commitment.analysis.spec.CloudCommitmentSpecData;
import com.vmturbo.cloud.commitment.analysis.spec.CommitmentSpecDemand;
import com.vmturbo.cloud.commitment.analysis.spec.CommitmentSpecDemandSet;
import com.vmturbo.cloud.commitment.analysis.spec.ImmutableReservedInstanceSpecData;
import com.vmturbo.cloud.commitment.analysis.spec.ReservedInstanceSpecData;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.AllocatedDemandClassification;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.cost.Pricing.OnDemandPriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.ReservedInstancePriceTable;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.components.api.SetOnce;
import com.vmturbo.cost.calculation.DiscountApplicator;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostDataRetrievalException;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.pricing.CloudRateExtractor;
import com.vmturbo.cost.calculation.pricing.CloudRateExtractor.ComputePriceBundle;
import com.vmturbo.cost.calculation.pricing.CloudRateExtractor.ComputePriceBundle.Builder;
import com.vmturbo.cost.calculation.topology.AccountPricingData;
import com.vmturbo.cost.calculation.topology.TopologyEntityInfoExtractor;
import com.vmturbo.cost.component.pricing.BusinessAccountPriceTableKeyStore;
import com.vmturbo.cost.component.pricing.PriceTableStore;
import com.vmturbo.cost.component.topology.LocalCostPricingResolver;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;
import com.vmturbo.platform.sdk.common.PricingDTO.ComputeTierPriceList;
import com.vmturbo.platform.sdk.common.PricingDTO.ComputeTierPriceList.ComputeTierConfigPrice;
import com.vmturbo.platform.sdk.common.PricingDTO.LicensePriceEntry;
import com.vmturbo.platform.sdk.common.PricingDTO.LicensePriceEntry.LicensePrice;
import com.vmturbo.platform.sdk.common.PricingDTO.Price;
import com.vmturbo.platform.sdk.common.PricingDTO.Price.Unit;
import com.vmturbo.platform.sdk.common.PricingDTO.ReservedInstancePrice;

/**
 * Testing the Local CloudCommitmentPricingAnalyzer.
 */
public class LocalCloudCommitmentPricingAnalyzerTest {
    private final LocalCostPricingResolver pricingResolver = mock(LocalCostPricingResolver.class);

    private final BusinessAccountPriceTableKeyStore businessAccountPriceTableKeyStore = mock(BusinessAccountPriceTableKeyStore.class);

    final PriceTableStore priceTableStore = mock(PriceTableStore.class);

    private static final float ON_DEMAND_RATE = 100f;
    private static final float WINDOWS_LICENSE_RATE = 50f;

    private static final float WINDOWS_RESERVED_LICENSE_RATE = 40f;

    private static final long Account_Aws_Oid = 1111L;

    private static final long REGION_AWS = 111;

    private static final long RI_SPEC_ID = 22L;

    private static final float UPFRONT_PRICE = 8760f;
    private static final float RECURRING_PRICE = 2f;

    private static final long awsServiceProviderId = 55555L;

    private static final long computeTierOid = 1L;
    private static final long TIER2_OID = 2L;

    private static final long priceTableKey = 11111L;

    private static final ReservedInstanceSpecInfo RI_SPEC_INFO =
            ReservedInstanceSpecInfo.newBuilder()
                    .setTenancy(Tenancy.DEFAULT)
                    .setOs(OSType.WINDOWS)
                    .build();

    final CloudTopology cloudTopology = mock(CloudTopology.class);

    private LocalCloudCommitmentPricingAnalyzer cloudCommitmentPricingAnalyzer;

    private final DiscountApplicator discountApplicator = mock(DiscountApplicator.class);

    private static final ComputeTierDemand computeTierDemand = ComputeTierDemand.builder()
            .cloudTierOid(computeTierOid).osType(OSType.WINDOWS).tenancy(Tenancy.DEFAULT).build();

    private static final TopologyEntityDTO computeTier = TopologyEntityDTO.newBuilder().setEnvironmentType(
            EnvironmentType.CLOUD)
            .setEntityType(EntityType.COMPUTE_TIER_VALUE)
            .setOid(computeTierOid)
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder().setComputeTier(
                    ComputeTierInfo.newBuilder().setNumCoupons(8).setBurstableCPU(false).setNumCores(8).build()).build())
            .setDisplayName("tier1")
            .build();

    private static final TopologyEntityDTO businessAccount = TopologyEntityDTO.newBuilder().setEnvironmentType(
            EnvironmentType.CLOUD)
            .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
            .setOid(Account_Aws_Oid)
            .setDisplayName("AWS Account")
            .build();

    private static final TopologyEntityDTO Region = TopologyEntityDTO.newBuilder().setEnvironmentType(
            EnvironmentType.CLOUD)
            .setEntityType(EntityType.REGION_VALUE)
            .setOid(REGION_AWS)
            .setDisplayName("AWS Region")
            .build();

    Optional<ReservedInstanceSpecData> riSpecData = Optional.of(ImmutableReservedInstanceSpecData.builder()
            .spec(ReservedInstanceSpec.newBuilder().setId(RI_SPEC_ID).setReservedInstanceSpecInfo(
                    ReservedInstanceSpecInfo.newBuilder().setType(ReservedInstanceType.newBuilder().setTermYears(1).build()).setOs(OSType.WINDOWS).setTenancy(Tenancy.DEFAULT)
                            .setRegionId(REGION_AWS).setTierId(12L).build()).build()).cloudTier(computeTier).build());

    private static Map<EntityInfo, Double> entityInfoDoubleMap = populateEntityInfoDoubleMap();

    private static final AggregateCloudTierDemand cloudTierDemand = AggregateCloudTierDemand.builder()
            .demandByEntity(entityInfoDoubleMap)
            .accountOid(Account_Aws_Oid).regionOid(REGION_AWS).serviceProviderOid(awsServiceProviderId)
            .cloudTierDemand(computeTierDemand).classification(DemandClassification.of(AllocatedDemandClassification.ALLOCATED)).build();

    private CommitmentSpecDemandSet commitmentSpecDemandSet;

    private final TopologyEntityInfoExtractor topologyEntityInfoExtractor = mock(TopologyEntityInfoExtractor.class);

    private CCAPriceHolder ccaPriceHolder = mock(CCAPriceHolder.class);

    private final SetOnce<CCAPriceHolder> ccaPriceHolderSetOnce = new SetOnce<>();

    private CloudRateExtractor cloudRateExtractor = mock(CloudRateExtractor.class);

    private Map<Long, AccountPricingData> accountPricingDataMap = constructBusinessAccountToAccountPricingDataMap();

    /**
     * /**
     * Setup the test.
     *
     * @throws CloudCostDataRetrievalException Cloud Cost Data Retrieval Exception
     * @throws NoSuchFieldException A no such field exception.
     * @throws IllegalAccessException An illegal access exception.
     */
    @Before
    public void setup()
            throws CloudCostDataRetrievalException, NoSuchFieldException, IllegalAccessException {
        cloudCommitmentPricingAnalyzer = new LocalCloudCommitmentPricingAnalyzer(pricingResolver,
                businessAccountPriceTableKeyStore, priceTableStore, topologyEntityInfoExtractor);
        ccaPriceHolderSetOnce.trySetValue(ccaPriceHolder);
        Field field = cloudCommitmentPricingAnalyzer.getClass().getDeclaredField("ccaPriceHolderSetOnce");
        field.setAccessible(true);
        field.set(cloudCommitmentPricingAnalyzer, ccaPriceHolderSetOnce);
        when(pricingResolver.getAccountPricingDataByBusinessAccount(cloudTopology)).thenReturn(accountPricingDataMap);
        when(cloudTopology.getAllEntitiesOfType(EntityType.BUSINESS_ACCOUNT_VALUE)).thenReturn(
                ImmutableList.of(businessAccount));
        when(cloudTopology.getEntity(computeTierOid)).thenReturn(Optional.of(computeTier));
        when(cloudTopology.getEntity(REGION_AWS)).thenReturn(Optional.of(Region));
        when(ccaPriceHolder.getAccountPricingDataMap()).thenReturn(accountPricingDataMap);
        when(ccaPriceHolder.getRiPriceTableMap()).thenReturn(getRIPriceTableByBusinessAccount());
        when(ccaPriceHolder.getCloudRateExtractor()).thenReturn(cloudRateExtractor);
        Set<AggregateCloudTierDemand> cloudTierDemandSet = ImmutableSet.of(cloudTierDemand);
        when(businessAccountPriceTableKeyStore.fetchPriceTableKeyOidsByBusinessAccount(ImmutableSet.of(Account_Aws_Oid)))
                .thenReturn(getPriceTableKeyByPriceTableMap());
        when(priceTableStore.getRiPriceTables(ImmutableSet.of(priceTableKey))).thenReturn(getRIPriceTableByPriceTableKeyOidMap());
        Map<CloudCommitmentSpecData, Set<AggregateCloudTierDemand>> tierDemandByDataMap = new HashMap<>();
        tierDemandByDataMap.put(riSpecData.get(), cloudTierDemandSet);
        CommitmentSpecDemand specDemand = CommitmentSpecDemand.builder()
                .cloudCommitmentSpecData(riSpecData.get()).addAllAggregateCloudTierDemandSet(cloudTierDemandSet).build();
        commitmentSpecDemandSet = CommitmentSpecDemandSet.builder().addCommitmentSpecDemand(specDemand).build();
    }

    /**
     * Test with no discounts applied.
     */
    @Test
    public void testLookupOnDemandRateNoAdjustment() {
        when(cloudRateExtractor.getComputePriceBundle(computeTier, REGION_AWS, accountPricingDataMap.get(Account_Aws_Oid)))
                .thenReturn(getUndiscountedOnDemandComputePriceBundle());
        when(cloudRateExtractor.getReservedLicensePriceBundle(accountPricingDataMap.get(Account_Aws_Oid),
                REGION_AWS, computeTier)).thenReturn(getUndiscountedReservedLicensePriceBundle());
        for (CommitmentSpecDemand specDemand : commitmentSpecDemandSet.commitmentSpecDemand()) {
            for (ScopedCloudTierDemand cloudTierDemand : specDemand.aggregateCloudTierDemandSet()) {
                TierDemandPricingData tierDemandPricingData = cloudCommitmentPricingAnalyzer.getTierDemandPricing(cloudTierDemand, cloudTopology);
                assert (tierDemandPricingData.onDemandRate() == 10f);
                assert (tierDemandPricingData.reservedLicenseRate() == 1f);
            }
            CloudCommitmentSpecData specData = specDemand.cloudCommitmentSpecData();
            CloudCommitmentPricingData cloudCommitmentPricingData = cloudCommitmentPricingAnalyzer
                    .getCloudCommitmentPricing(specData, Sets.newHashSet(Account_Aws_Oid), cloudTopology);
            assert (((RIPricingData)cloudCommitmentPricingData).reservedInstanceRate() == 3f);
        }
    }

    /**
     * Test with 50% on demand and reserved license cost discounted.
     */
    @Test
    public void test50perCentPriceAdjustment() {
        when(cloudRateExtractor.getComputePriceBundle(computeTier, REGION_AWS, accountPricingDataMap.get(Account_Aws_Oid))).thenReturn(
                getDiscountedOnDemandComputePriceBundle());
        when(cloudRateExtractor.getReservedLicensePriceBundle(accountPricingDataMap.get(Account_Aws_Oid),
                REGION_AWS, computeTier)).thenReturn(getDiscountedReservedLicensePriceBundle());
        for (CommitmentSpecDemand specDemand : commitmentSpecDemandSet.commitmentSpecDemand()) {
            for (ScopedCloudTierDemand cloudTierDemand : specDemand.aggregateCloudTierDemandSet()) {
                TierDemandPricingData tierDemandPricingData = cloudCommitmentPricingAnalyzer.getTierDemandPricing(cloudTierDemand, cloudTopology);
                assert (tierDemandPricingData.onDemandRate() == 5f);
                assert (tierDemandPricingData.reservedLicenseRate() == 0.5f);
            }
            CloudCommitmentSpecData specData = specDemand.cloudCommitmentSpecData();
            CloudCommitmentPricingData cloudCommitmentPricingData = cloudCommitmentPricingAnalyzer
                    .getCloudCommitmentPricing(specData, Sets.newHashSet(Account_Aws_Oid), cloudTopology);
            assert (((RIPricingData)cloudCommitmentPricingData).reservedInstanceRate() == 3f);
        }
    }

    private PriceTable mockOnDemandPrices(final PriceTableStore priceTableStore) {
        //mock on-demand prices
        ComputeTierPriceList computeTierPriceList = ComputeTierPriceList.newBuilder()
                .setBasePrice(ComputeTierConfigPrice.newBuilder()
                        .addPrices(Price.newBuilder()
                                .setUnit(Unit.HOURS)
                                .setPriceAmount(
                                        CurrencyAmount.newBuilder().setAmount(ON_DEMAND_RATE))))
                .build();

        ComputeTierPriceList computeTierPriceList2 = ComputeTierPriceList.newBuilder()
                .setBasePrice(ComputeTierConfigPrice.newBuilder()
                        .addPrices(Price.newBuilder()
                                .setUnit(Unit.HOURS)
                                .setPriceAmount(
                                        CurrencyAmount.newBuilder().setAmount(ON_DEMAND_RATE))).setGuestOsType(OSType.LINUX))
                .addPerConfigurationPriceAdjustments(ComputeTierConfigPrice.newBuilder()
                        .setGuestOsType(OSType.WINDOWS)
                        .setTenancy(Tenancy.DEFAULT)
                        .addPrices(Price.newBuilder()
                                .setUnit(Unit.HOURS)
                                .setPriceAmount(
                                        CurrencyAmount.newBuilder().setAmount(WINDOWS_LICENSE_RATE))))
                .build();

        Map<Long, ComputeTierPriceList> computeTierPriceListMap =
                ImmutableMap.<Long, ComputeTierPriceList>builder().put(computeTierOid,
                        computeTierPriceList).put(TIER2_OID, computeTierPriceList2).build();

        Map<Long, OnDemandPriceTable> onDemandPriceTableMapByRegion =
                ImmutableMap.<Long, OnDemandPriceTable>builder().put(REGION_AWS,
                        OnDemandPriceTable.newBuilder()
                                .putAllComputePricesByTierId(computeTierPriceListMap)
                                .build()).build();

        PriceTable priceTable = PriceTable.newBuilder()
                .putAllOnDemandPriceByRegionId(onDemandPriceTableMapByRegion)
                .addOnDemandLicensePrices(LicensePriceEntry.newBuilder()
                        .setOsType(OSType.WINDOWS)
                        .addLicensePrices(licensePrice(1, price(Unit.HOURS, 1d)))
                        .addLicensePrices(licensePrice(2, price(Unit.HOURS, 10d)))
                        .addLicensePrices(licensePrice(8, price(Unit.HOURS, WINDOWS_LICENSE_RATE)))
                        .build())
                .addReservedLicensePrices(LicensePriceEntry.newBuilder()
                        .setOsType(OSType.WINDOWS)
                        .addLicensePrices(licensePrice(1, price(Unit.HOURS, 2d)))
                        .addLicensePrices(licensePrice(2, price(Unit.HOURS, 5d)))
                        .addLicensePrices(licensePrice(8, price(Unit.HOURS, WINDOWS_RESERVED_LICENSE_RATE)))
                        .build())
                .build();

        Map<Long, PriceTable> priceTableMap =
                ImmutableMap.<Long, PriceTable>builder().put(Account_Aws_Oid, priceTable).build();

        when(priceTableStore.getPriceTables(any())).thenReturn(priceTableMap);
        return priceTable;
    }

    private ReservedInstancePriceTable mockRIPrices() {
        //mock RI prices
        ReservedInstancePrice riPrice = ReservedInstancePrice.newBuilder()
                .setUpfrontPrice(Price.newBuilder()
                        .setPriceAmount(CurrencyAmount.newBuilder().setAmount(UPFRONT_PRICE)))
                .setRecurringPrice(Price.newBuilder()
                        .setPriceAmount(CurrencyAmount.newBuilder().setAmount(RECURRING_PRICE)))
                .build();

        Map<Long, ReservedInstancePrice> reservedInstancePriceMap =
                ImmutableMap.<Long, ReservedInstancePrice>builder().put(RI_SPEC_ID, riPrice)
                        .build();

        ReservedInstancePriceTable reservedInstancePriceTable =
                ReservedInstancePriceTable.newBuilder()
                        .putAllRiPricesBySpecId(reservedInstancePriceMap)
                        .build();
        return reservedInstancePriceTable;
    }

    private Map<Long, AccountPricingData> constructBusinessAccountToAccountPricingDataMap() {
        PriceTable priceTable = mockOnDemandPrices(priceTableStore);
        AccountPricingData accountPricingData = new AccountPricingData(discountApplicator,
                priceTable, 111111L);
        Map<Long, AccountPricingData> accountPricingDataByBusinessAccountMap = new HashMap<>();
        accountPricingDataByBusinessAccountMap.put(Account_Aws_Oid, accountPricingData);
        return accountPricingDataByBusinessAccountMap;
    }

    private  Map<Long, Long> getPriceTableKeyByPriceTableMap() {
        Map<Long, Long> priceTableKeyByBusinessAccountOidMap = new HashMap<>();
        priceTableKeyByBusinessAccountOidMap.put(businessAccount.getOid(), priceTableKey);
        return priceTableKeyByBusinessAccountOidMap;
    }

    private Map<Long, ReservedInstancePriceTable> getRIPriceTableByBusinessAccount() {
        Map<Long, ReservedInstancePriceTable> riPriceTableByBusinessAccount = new HashMap<>();
        riPriceTableByBusinessAccount.put(Account_Aws_Oid, mockRIPrices());
        return riPriceTableByBusinessAccount;
    }

    private Map<Long, ReservedInstancePriceTable> getRIPriceTableByPriceTableKeyOidMap() {
        Map<Long, ReservedInstancePriceTable> riPriceTableByPriceTableKey = new HashMap<>();
        riPriceTableByPriceTableKey.put(priceTableKey, mockRIPrices());
        return riPriceTableByPriceTableKey;
    }

    private static Price price(Price.Unit unit, double amount) {
        return Price.newBuilder()
                .setUnit(unit)
                .setPriceAmount(CurrencyAmount.newBuilder()
                        .setAmount(amount)
                        .build())
                .build();
    }

    private static LicensePrice licensePrice(int numCores, Price price) {
        return LicensePrice.newBuilder()
                .setNumberOfCores(numCores)
                .setPrice(price)
                .build();
    }

    private ComputePriceBundle getUndiscountedOnDemandComputePriceBundle() {
        Builder computePriceBundle = ComputePriceBundle.newBuilder();
        computePriceBundle.addPrice(REGION_AWS, OSType.WINDOWS, 10d, false);
        computePriceBundle.addPrice(REGION_AWS, OSType.LINUX, 5d, true);
        return computePriceBundle.build();
    }

    private ComputePriceBundle getUndiscountedReservedLicensePriceBundle() {
        Builder computePriceBundle = ComputePriceBundle.newBuilder();
        computePriceBundle.addPrice(REGION_AWS, OSType.WINDOWS, 1d, false);
        computePriceBundle.addPrice(REGION_AWS, OSType.LINUX, 0.5d, true);
        return computePriceBundle.build();
    }

    private ComputePriceBundle getDiscountedOnDemandComputePriceBundle() {
        Builder computePriceBundle = ComputePriceBundle.newBuilder();
        computePriceBundle.addPrice(REGION_AWS, OSType.WINDOWS, 5d, false);
        computePriceBundle.addPrice(REGION_AWS, OSType.LINUX, 2.5d, true);
        return computePriceBundle.build();
    }

    private ComputePriceBundle getDiscountedReservedLicensePriceBundle() {
        Builder computePriceBundle = ComputePriceBundle.newBuilder();
        computePriceBundle.addPrice(REGION_AWS, OSType.WINDOWS, 0.5d, false);
        computePriceBundle.addPrice(REGION_AWS, OSType.LINUX, 0.25d, true);
        return computePriceBundle.build();
    }

    private static Map<EntityInfo, Double>  populateEntityInfoDoubleMap() {
        Map<EntityInfo, Double> entityInfoDoubleMap = new HashMap<>();
        entityInfoDoubleMap.put(EntityInfo.builder().entityOid(12L).build(), 0.5);
        entityInfoDoubleMap.put(EntityInfo.builder().entityOid(13L).build(), 1.0);
        entityInfoDoubleMap.put(EntityInfo.builder().entityOid(14L).build(), 1.5);
        return entityInfoDoubleMap;
    }
}


