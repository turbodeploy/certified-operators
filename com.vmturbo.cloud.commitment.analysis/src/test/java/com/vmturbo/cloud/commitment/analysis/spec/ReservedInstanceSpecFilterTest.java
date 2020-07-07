package com.vmturbo.cloud.commitment.analysis.spec;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.components.common.setting.CategoryPathConstants;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopologyFactory;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopologyFactory.DefaultTopologyEntityCloudTopologyFactory;
import com.vmturbo.group.api.GroupMemberRetriever;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType.OfferingClass;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType.PaymentOption;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

/**
 * Class for testing the CloudCommitmentSpecFilter.
 */
public class ReservedInstanceSpecFilterTest {

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

    private static final TopologyEntityDTO computeTier1 = TopologyEntityDTO.newBuilder()
            .setEnvironmentType(EnvironmentType.CLOUD)
            .setEntityType(EntityType.COMPUTE_TIER_VALUE)
            .setOid(AWS_TIER_ID)
            .setDisplayName("tier1")
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                    .setConnectedEntityId(REGION_AWS)
                    .setConnectedEntityType(EntityType.REGION_VALUE))
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                    .setComputeTier(ComputeTierInfo.newBuilder()
                            .setNumCoupons(10).setFamily("T1")))
            .build();

    private static final TopologyEntityDTO computeTier2 = TopologyEntityDTO.newBuilder()
            .setEnvironmentType(EnvironmentType.CLOUD)
            .setEntityType(EntityType.COMPUTE_TIER_VALUE)
            .setOid(AZURE_TIER_ID)
            .setDisplayName("tier2")
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                    .setConnectedEntityId(REGION_AWS)
                    .setConnectedEntityType(EntityType.REGION_VALUE))
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                    .setComputeTier(ComputeTierInfo.newBuilder()
                            .setNumCoupons(10).setFamily("T2")))
            .build();
    private static final Map<Long, TopologyEntityDTO> entityMap = getEntityMap();
    private final TopologyEntityCloudTopologyFactory cloudTopologyFactory =
            new DefaultTopologyEntityCloudTopologyFactory(mock(GroupMemberRetriever.class));

    private ImmutableMap<Long, ReservedInstanceType> purchaseConstraints;
    private ReservedInstanceSpec awsSpec;
    private ReservedInstanceSpec azureSpec;

    private final CloudCommitmentSpecResolver cloudCommitmentSpecResolver = mock(CloudCommitmentSpecResolver.class);

    private CloudTopology cloudTopology;

    /**
     * Setup for the test.
     */
    @Before
    public void setup() {
        ReservedInstanceType awsConstraints = ReservedInstanceType.newBuilder().setOfferingClass(OfferingClass.STANDARD)
                .setPaymentOption(PaymentOption.PARTIAL_UPFRONT).setTermYears(1).build();

        ReservedInstanceType azureConstraints = ReservedInstanceType.newBuilder().setOfferingClass(OfferingClass.CONVERTIBLE)
                .setPaymentOption(PaymentOption.ALL_UPFRONT).setTermYears(3).build();

        purchaseConstraints =
                ImmutableMap.of(REGION_AWS, awsConstraints,
                        REGION_AZURE, azureConstraints);

        cloudTopology = cloudTopologyFactory.newCloudTopology(entityMap.values().stream());
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
        when(cloudCommitmentSpecResolver.getRISpecsForRegion(any(Long.class))).thenReturn(riSpecs);
        ReservedInstanceSpecFilter reservedInstanceSpecFilter = new ReservedInstanceSpecFilter(cloudTopology, purchaseConstraints, cloudCommitmentSpecResolver);
        Map<ReservedInstanceSpecKey, ReservedInstanceSpecData> reservedInstanceSpecKeyReservedInstanceSpecDataMap
                = reservedInstanceSpecFilter.filterRISpecs(123454L);

        //Test aws
        ReservedInstanceSpecKey awsSpecKey = ImmutableReservedInstanceSpecKey.builder().regionOid(111L)
                .family("T1").tenancy(Tenancy.DEFAULT).build();
        ReservedInstanceSpecData awsReservedInstanceSpecData = reservedInstanceSpecKeyReservedInstanceSpecDataMap.get(awsSpecKey);
        assert (awsReservedInstanceSpecData.computeTier().equals(computeTier1));
        assert (awsReservedInstanceSpecData.reservedInstanceSpec().equals(awsSpec));

        //Test azure
        ReservedInstanceSpecKey azureSpecKey = ImmutableReservedInstanceSpecKey.builder().regionOid(11L)
                .family("T2").tenancy(Tenancy.DEFAULT).build();
        ReservedInstanceSpecData azureReservedInstanceSpecData = reservedInstanceSpecKeyReservedInstanceSpecDataMap
                .get(azureSpecKey);
        assert (azureReservedInstanceSpecData.computeTier().equals(computeTier2));
        assert (azureReservedInstanceSpecData.reservedInstanceSpec().equals(azureSpec));
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

