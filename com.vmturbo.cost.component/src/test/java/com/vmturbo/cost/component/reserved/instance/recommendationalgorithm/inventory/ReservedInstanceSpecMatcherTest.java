package com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.inventory;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.components.common.setting.CategoryPathConstants;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopologyFactory;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopologyFactory.DefaultTopologyEntityCloudTopologyFactory;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceSpecStore;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.ReservedInstancePurchaseConstraints;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand.AccountGroupingIdentifier;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand.ImmutableAccountGroupingIdentifier;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand.ImmutableRIBuyDemandCluster;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand.ImmutableRIBuyRegionalContext;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand.RIBuyDemandCluster;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand.RIBuyRegionalContext;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.inventory.ReservedInstanceSpecMatcher.ReservedInstanceSpecData;
import com.vmturbo.group.api.GroupMemberRetriever;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType.OfferingClass;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType.PaymentOption;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;
import com.vmturbo.reserved.instance.coverage.allocator.rules.RICoverageRuleConfig;

/**
 * Tests the class ReservedInstanceSpecMatcher.
 */
public class ReservedInstanceSpecMatcherTest {

    private static final long REGION_AWS = 111;
    private static final long PRIMARY_ACCOUNT_1_OID = 222;
    private static final long ACCOUNT_ID = 444;
    private static final long ZONE_ID = 666;
    private static final long AWS_TIER_ID = 888L;
    private static final long AZURE_TIER_ID = 999L;
    private static final long VM_ID = 101L;
    private static final long PRIMARY_VM_ID = 102L;
    private static final long SPEC_ID_1 = 777L;
    private static final long SPEC_ID_2 = 555L;
    private static final long AWS_SP = 1L;
    private static final long AZURE_SP = 2L;
    private static final long REGION_AZURE = 11L;
    private static final String T1 = "T1";
    private static final String T2 = "T2";

    private static final TopologyEntityDTO REGION = TopologyEntityDTO.newBuilder()
            .setOid(REGION_AWS)
            .setDisplayName("region")
            .setEntityType(EntityType.REGION_VALUE)
            .setEnvironmentType(EnvironmentType.CLOUD)
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                    .setConnectedEntityId(2222)
                    .setConnectedEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
                    .setConnectionType(ConnectionType.OWNS_CONNECTION))
            .build();

    private static final ReservedInstanceType RI_TYPE =
            ReservedInstanceType.newBuilder().setTermYears(1).build();
    private static final Cost.ReservedInstanceSpecInfo riSpecInfo =
            Cost.ReservedInstanceSpecInfo.newBuilder()
                    .setOs(OSType.RHEL)
                    .setTenancy(Tenancy.DEFAULT)
                    .setType(RI_TYPE)
                    .setPlatformFlexible(true)
                    .build();

    private static final Cost.ReservedInstanceSpec RI_TO_PURCHASE =
            Cost.ReservedInstanceSpec.newBuilder().setReservedInstanceSpecInfo(riSpecInfo).build();

    private static final AccountGroupingIdentifier ACCOUNT_GROUPING =
            ImmutableAccountGroupingIdentifier.builder()
                    .groupingType(AccountGroupingIdentifier.AccountGroupingType.BILLING_FAMILY)
                    .id(1)
                    .tag("AccountGrouping")
                    .build();

    private static final TopologyEntityDTO COMPUTE_TIER = TopologyEntityDTO.newBuilder()
            .setOid(AWS_TIER_ID)
            .setEntityType(EntityType.COMPUTE_TIER_VALUE)
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                    .setComputeTier(ComputeTierInfo.newBuilder().setFamily(T1).build())
                    .build())
            .build();

    private static final RIBuyDemandCluster RI_BUY_DEMAND_CLUSTER =
            ImmutableRIBuyDemandCluster.builder()
                    .accountOid(PRIMARY_ACCOUNT_1_OID)
                    .platform(OSType.LINUX)
                    .tenancy(Tenancy.DEFAULT)
                    .computeTier(COMPUTE_TIER)
                    .regionOrZoneOid(REGION_AWS)
                    .build();


    private static final RIBuyRegionalContext REGIONAL_CONTEXT =
            ImmutableRIBuyRegionalContext.builder()
                    .region(REGION)
                    .riSpecToPurchase(RI_TO_PURCHASE)
                    .computeTier(COMPUTE_TIER)
                    .accountGroupingId(ACCOUNT_GROUPING)
                    .contextTag("Context")
                    .analysisTag("Analysis")
                    .addDemandClusters(RI_BUY_DEMAND_CLUSTER)
                    .build();

    private static final Map<Long, TopologyEntityDTO> entityMap = getEntityMap();
    private final TopologyEntityCloudTopologyFactory cloudTopologyFactory =
            new DefaultTopologyEntityCloudTopologyFactory(mock(GroupMemberRetriever.class));
    private final ReservedInstanceSpecStore riSpecStore = mock(ReservedInstanceSpecStore.class);
    private final ReservedInstanceSpecMatcherFactory matcherFactory =
            new ReservedInstanceSpecMatcherFactory(riSpecStore);

    private ImmutableMap<String, ReservedInstancePurchaseConstraints> purchaseConstraints;
    private ReservedInstanceSpec awsSpec;
    private ReservedInstanceSpec azureSpec;

    /**
     * Test set up to be run before each test method.
     */
    @Before
    public void setup() {
        ReservedInstancePurchaseConstraints awsConstraints = mock(ReservedInstancePurchaseConstraints.class);
        Mockito.when(awsConstraints.getOfferingClass()).thenReturn(OfferingClass.STANDARD);
        Mockito.when(awsConstraints.getPaymentOption()).thenReturn(PaymentOption.PARTIAL_UPFRONT);
        Mockito.when(awsConstraints.getTermInYears()).thenReturn(1);

        ReservedInstancePurchaseConstraints azureConstraints = mock(ReservedInstancePurchaseConstraints.class);
        Mockito.when(azureConstraints.getOfferingClass()).thenReturn(OfferingClass.CONVERTIBLE);
        Mockito.when(azureConstraints.getPaymentOption()).thenReturn(PaymentOption.ALL_UPFRONT);
        Mockito.when(azureConstraints.getTermInYears()).thenReturn(3);

        purchaseConstraints =
                ImmutableMap.of(CategoryPathConstants.AWS.toUpperCase(), awsConstraints,
                        CategoryPathConstants.AZURE.toUpperCase(), azureConstraints);
    }

    /**
     * Test for AWS service provider constraints.
     */
    @Test
    public void testMatchToPurchasingRISpecDataForAWS() {

        awsSpec = createRISpec(SPEC_ID_1, REGION_AWS, AWS_TIER_ID, 1,
                OfferingClass.STANDARD, PaymentOption.PARTIAL_UPFRONT, OSType.LINUX);
        azureSpec = createRISpec(SPEC_ID_2, REGION_AZURE, AZURE_TIER_ID, 3,
                OfferingClass.CONVERTIBLE, PaymentOption.ALL_UPFRONT, OSType.WINDOWS);
        final List<ReservedInstanceSpec> riSpecs = ImmutableList.of(awsSpec, azureSpec);

        when(riSpecStore.getAllRISpecsForRegion(any(Long.class))).thenReturn(riSpecs);

        ReservedInstanceSpecMatcher matcher = matcherFactory.createRegionalMatcher(
                cloudTopologyFactory.newCloudTopology(entityMap.values().stream()),
                purchaseConstraints, REGION_AWS);
        Optional<ReservedInstanceSpecData> spec =
                matcher.matchToPurchasingRISpecData(entityMap.get(REGION_AWS),
                        entityMap.get(AWS_TIER_ID), OSType.LINUX, Tenancy.DEFAULT);
        verifySpecs(spec, awsSpec);
    }

    /**
     * Test for AWS service provider constraints.
     */
    @Test
    public void testNoMatchToPurchasingRISpecDataForAWS() {

        awsSpec = createRISpec(SPEC_ID_1, REGION_AWS, AWS_TIER_ID, 1,
                OfferingClass.STANDARD, PaymentOption.ALL_UPFRONT, OSType.LINUX);
        azureSpec = createRISpec(SPEC_ID_2, REGION_AZURE, AZURE_TIER_ID, 3,
                OfferingClass.CONVERTIBLE, PaymentOption.ALL_UPFRONT, OSType.WINDOWS);
        final List<ReservedInstanceSpec> riSpecs =
                ImmutableList.of(awsSpec, azureSpec);
        when(riSpecStore.getAllRISpecsForRegion(any(Long.class))).thenReturn(riSpecs);
        ReservedInstanceSpecMatcher matcher = matcherFactory.createRegionalMatcher(
                cloudTopologyFactory.newCloudTopology(entityMap.values().stream()),
                purchaseConstraints, REGION_AWS);
        Optional<ReservedInstanceSpecData> spec =
                matcher.matchToPurchasingRISpecData(entityMap.get(REGION_AWS),
                        entityMap.get(AWS_TIER_ID), OSType.LINUX, Tenancy.DEFAULT);
        Assert.assertFalse("Matched a spec unexpectedly", spec.isPresent());
    }

    /**
     * Test for AWS service provider constraints.
     */
    @Test
    public void testMatchToPurchasingRISpecDataForAzure() {

        awsSpec = createRISpec(SPEC_ID_1, REGION_AWS, AWS_TIER_ID, 1,
                OfferingClass.STANDARD, PaymentOption.ALL_UPFRONT, OSType.LINUX);
        azureSpec = createRISpec(SPEC_ID_2, REGION_AZURE, AZURE_TIER_ID, 3,
                OfferingClass.CONVERTIBLE, PaymentOption.ALL_UPFRONT, OSType.WINDOWS);
        final List<ReservedInstanceSpec> riSpecs =
                ImmutableList.of(awsSpec, azureSpec);
        when(riSpecStore.getAllRISpecsForRegion(any(Long.class))).thenReturn(riSpecs);

        ReservedInstanceSpecMatcher matcher = matcherFactory.createRegionalMatcher(
                cloudTopologyFactory.newCloudTopology(entityMap.values().stream()),
                purchaseConstraints, REGION_AZURE);
        Optional<ReservedInstanceSpecData> spec =
                matcher.matchToPurchasingRISpecData(entityMap.get(REGION_AZURE),
                        entityMap.get(AZURE_TIER_ID), OSType.WINDOWS, Tenancy.DEFAULT);
        verifySpecs(spec, azureSpec);
    }

    private void verifySpecs(final Optional<ReservedInstanceSpecData> spec,
                             final ReservedInstanceSpec expectedSpec) {
        Assert.assertTrue("No RI Spec matched the constraints.", spec.isPresent());
        Assert.assertNotNull("Retrieved a null RI spec!", spec.get());
        Assert.assertEquals("Actual and expected RI specs do not match.",
                expectedSpec, spec.get().reservedInstanceSpec());
    }

    /**
     * Test for AWS service provider constraints.
     */
    @Test
    public void testNoMatchToPurchasingRISpecDataForAzure() {

        awsSpec = createRISpec(SPEC_ID_1, REGION_AWS, AWS_TIER_ID, 1,
                OfferingClass.STANDARD, PaymentOption.ALL_UPFRONT, OSType.LINUX);
        azureSpec = createRISpec(SPEC_ID_2, REGION_AZURE, AZURE_TIER_ID, 1,
                OfferingClass.CONVERTIBLE, PaymentOption.ALL_UPFRONT, OSType.WINDOWS);
        final List<ReservedInstanceSpec> riSpecs =
                ImmutableList.of(awsSpec, azureSpec);
        when(riSpecStore.getAllRISpecsForRegion(any(Long.class))).thenReturn(riSpecs);
        ReservedInstanceSpecMatcher matcher = matcherFactory.createRegionalMatcher(
                cloudTopologyFactory.newCloudTopology(entityMap.values().stream()),
                purchaseConstraints, REGION_AWS);
        Optional<ReservedInstanceSpecData> spec =
                matcher.matchToPurchasingRISpecData(entityMap.get(REGION_AZURE),
                        entityMap.get(AZURE_TIER_ID), OSType.WINDOWS, Tenancy.DEFAULT);
        Assert.assertFalse("Matched a spec unexpectedly", spec.isPresent());
    }

    /**
     * Creates a test RI Spec.
     *
     * @param specId RI Spec OID
     * @param regionId Region
     * @param tier1Id The compute tier Id.
     * @param termYears The RI term in years.
     * @param offeringClass The Offering class.
     * @param paymentOption Payment Option.
     * @param osType The OSType
     * @return A Reserved Instance Spec.
     */
    private ReservedInstanceSpec createRISpec(final long specId, final long regionId, final long tier1Id,
                                              final int termYears, final OfferingClass offeringClass,
                                              final PaymentOption paymentOption, OSType osType) {

        ReservedInstanceSpec riSpec = ReservedInstanceSpec.newBuilder()
                .setReservedInstanceSpecInfo(ReservedInstanceSpecInfo.newBuilder()
                        .setTierId(tier1Id)
                        .setRegionId(regionId)
                        .setType(ReservedInstanceType.newBuilder().setTermYears(termYears)
                                .setOfferingClass(offeringClass)
                                .setPaymentOption(paymentOption).build())
                        .setOs(osType)
                        .setSizeFlexible(true)
                        .setPlatformFlexible(true)
                        .setTenancy(Tenancy.DEFAULT).build())
                .setId(specId).build();
        return riSpec;
    }

    /**
     * Test {@link ReservedInstanceSpecMatcher#matchDemandContextToRISpecs}.
     */
    @Test
    public void testMatchDemandContextToRISpecs() {
        awsSpec = createRISpec(SPEC_ID_1, REGION_AWS, AWS_TIER_ID, 1, OfferingClass.STANDARD,
                PaymentOption.ALL_UPFRONT, OSType.LINUX);
        azureSpec =
                createRISpec(SPEC_ID_2, REGION_AZURE, AZURE_TIER_ID, 1, OfferingClass.CONVERTIBLE,
                        PaymentOption.ALL_UPFRONT, OSType.WINDOWS);
        final List<ReservedInstanceSpec> riSpecs = ImmutableList.of(awsSpec, azureSpec);
        when(riSpecStore.getAllRISpecsForRegion(any(Long.class))).thenReturn(riSpecs);
        ReservedInstanceSpecMatcher matcher = matcherFactory.createRegionalMatcher(
                cloudTopologyFactory.newCloudTopology(entityMap.values().stream()),
                purchaseConstraints, REGION_AWS);

        RICoverageRuleConfig ruleConfig = mock(RICoverageRuleConfig.class);
        when(ruleConfig.isPlatformFlexible()).thenReturn(Optional.of(true));
        when(ruleConfig.isSizeFlexible()).thenReturn(Optional.of(true));

        Set<Long> result = matcher.matchDemandContextToRISpecs(ruleConfig, REGIONAL_CONTEXT,
                RI_BUY_DEMAND_CLUSTER);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(SPEC_ID_1, (long)result.iterator().next());
    }

    private static Map<Long, TopologyEntityDTO> getEntityMap() {
        Map<Long, TopologyEntityDTO> entityMap = new HashMap<>();
        TopologyEntityDTO awsSP = TopologyEntityDTO.newBuilder()
                .setEnvironmentType(EnvironmentType.CLOUD)
                .setEntityType(EntityType.SERVICE_PROVIDER_VALUE)
                .setOid(AWS_SP)
                .setDisplayName(CategoryPathConstants.AWS.toUpperCase())
                .addConnectedEntityList(ConnectedEntity.newBuilder()
                        .setConnectionType(ConnectionType.OWNS_CONNECTION)
                        .setConnectedEntityId(REGION_AWS)
                        .setConnectedEntityType(EntityType.REGION_VALUE))
                .build();
        TopologyEntityDTO azureSP = TopologyEntityDTO.newBuilder()
                .setEnvironmentType(EnvironmentType.CLOUD)
                .setEntityType(EntityType.SERVICE_PROVIDER_VALUE)
                .setOid(AZURE_SP)
                .setDisplayName(CategoryPathConstants.AZURE.toUpperCase())
                .addConnectedEntityList(ConnectedEntity.newBuilder()
                        .setConnectionType(ConnectionType.OWNS_CONNECTION)
                        .setConnectedEntityId(REGION_AZURE)
                        .setConnectedEntityType(EntityType.REGION_VALUE))
                .build();
        entityMap.put(AWS_SP, awsSP);
        entityMap.put(AZURE_SP, azureSP);

        TopologyEntityDTO az = TopologyEntityDTO.newBuilder()
                .setEnvironmentType(EnvironmentType.CLOUD)
                .setEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
                .setOid(ZONE_ID)
                .build();
        entityMap.put(ZONE_ID, az);
        TopologyEntityDTO region = TopologyEntityDTO.newBuilder()
                .setEnvironmentType(EnvironmentType.CLOUD)
                .setEntityType(EntityType.REGION_VALUE)
                .setOid(REGION_AWS)
                .addConnectedEntityList(ConnectedEntity.newBuilder()
                        .setConnectionType(ConnectionType.OWNS_CONNECTION)
                        .setConnectedEntityId(ZONE_ID)
                        .setConnectedEntityType(EntityType.AVAILABILITY_ZONE_VALUE))
                .build();

        TopologyEntityDTO regionAzure = TopologyEntityDTO.newBuilder()
                .setEnvironmentType(EnvironmentType.CLOUD)
                .setEntityType(EntityType.REGION_VALUE)
                .setOid(REGION_AZURE)
                .build();
        entityMap.put(REGION_AWS, region);
        entityMap.put(REGION_AZURE, regionAzure);


        TopologyEntityDTO ba = TopologyEntityDTO.newBuilder()
                .setEnvironmentType(EnvironmentType.CLOUD)
                .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
                .setOid(ACCOUNT_ID)
                .addConnectedEntityList(ConnectedEntity.newBuilder()
                        .setConnectedEntityId(VM_ID)
                        .setConnectedEntityType(EntityType.VIRTUAL_MACHINE_VALUE))
                .build();
        TopologyEntityDTO ma = TopologyEntityDTO.newBuilder()
                .setEnvironmentType(EnvironmentType.CLOUD)
                .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
                .setOid(PRIMARY_ACCOUNT_1_OID)
                .addConnectedEntityList(ConnectedEntity.newBuilder()
                        .setConnectedEntityId(PRIMARY_VM_ID)
                        .setConnectedEntityType(EntityType.VIRTUAL_MACHINE_VALUE))
                .build();

        entityMap.put(ACCOUNT_ID, ba);
        entityMap.put(PRIMARY_ACCOUNT_1_OID, ma);

        TopologyEntityDTO computeTier1 = TopologyEntityDTO.newBuilder()
                .setEnvironmentType(EnvironmentType.CLOUD)
                .setEntityType(EntityType.COMPUTE_TIER_VALUE)
                .setOid(AWS_TIER_ID)
                .setDisplayName("tier1")
                .addConnectedEntityList(ConnectedEntity.newBuilder()
                        .setConnectedEntityId(REGION_AWS)
                        .setConnectedEntityType(EntityType.REGION_VALUE))
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                        .setComputeTier(ComputeTierInfo.newBuilder()
                                .setNumCoupons(10).setFamily(T1)))
                .build();
        TopologyEntityDTO computeTier2 = TopologyEntityDTO.newBuilder()
                .setEnvironmentType(EnvironmentType.CLOUD)
                .setEntityType(EntityType.COMPUTE_TIER_VALUE)
                .setOid(AZURE_TIER_ID)
                .setDisplayName("tier2")
                .addConnectedEntityList(ConnectedEntity.newBuilder()
                        .setConnectedEntityId(REGION_AWS)
                        .setConnectedEntityType(EntityType.REGION_VALUE))
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                        .setComputeTier(ComputeTierInfo.newBuilder()
                                .setNumCoupons(10).setFamily(T2)))
                .build();
        entityMap.put(AWS_TIER_ID, computeTier1);
        entityMap.put(AZURE_TIER_ID, computeTier2);

        TopologyEntityDTO vm = TopologyEntityDTO.newBuilder()
                .setEnvironmentType(EnvironmentType.CLOUD)
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setOid(VM_ID)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .setProviderId(AWS_TIER_ID)
                        .setProviderEntityType(EntityType.COMPUTE_TIER_VALUE))
                .addConnectedEntityList(ConnectedEntity.newBuilder()
                        .setConnectedEntityId(ZONE_ID)
                        .setConnectedEntityType(EntityType.AVAILABILITY_ZONE_VALUE))
                .build();
        TopologyEntityDTO vm2 = TopologyEntityDTO.newBuilder()
                .setEnvironmentType(EnvironmentType.CLOUD)
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setOid(PRIMARY_VM_ID)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .setProviderId(AZURE_TIER_ID)
                        .setProviderEntityType(EntityType.COMPUTE_TIER_VALUE))
                .addConnectedEntityList(ConnectedEntity.newBuilder()
                        .setConnectedEntityId(REGION_AZURE)
                        .setConnectedEntityType(EntityType.REGION_VALUE))
                .build();
        entityMap.put(VM_ID, vm);
        entityMap.put(PRIMARY_VM_ID, vm2);
        return entityMap;
    }
}
