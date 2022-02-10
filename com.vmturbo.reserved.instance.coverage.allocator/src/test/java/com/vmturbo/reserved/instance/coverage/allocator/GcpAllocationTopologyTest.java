package com.vmturbo.reserved.instance.coverage.allocator;

import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.ImmutableSet;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.OS;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.BusinessAccountInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.CloudCommitmentInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.group.api.GroupAndMembers;
import com.vmturbo.group.api.ImmutableGroupAndMembers;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CloudCommitmentData.CloudCommitmentScope;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CloudCommitmentData.CloudCommitmentStatus;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CloudCommitmentData.CommittedCommoditiesBought;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CloudCommitmentData.CommittedCommodityBought;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CloudCommitmentData.FamilyRestricted;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

// package-private
class GcpAllocationTopologyTest {

    private GcpAllocationTopologyTest() {}

    protected static final AtomicLong OID_PROVIDER = new AtomicLong();

    private static final long COMMITMENT_A_SCOPED_COMMITMENT_OID = OID_PROVIDER.incrementAndGet();

    private static final long COMMITMENT_B_BF_SCOPED_OID = OID_PROVIDER.incrementAndGet();

    protected static final TopologyEntityDTO GCP_SERVICE_PROVIDER_TEST = TopologyEntityDTO.newBuilder()
            .setOid(OID_PROVIDER.incrementAndGet())
            .setDisplayName("GCP")
            .setEntityType(EntityType.SERVICE_PROVIDER_VALUE)
            .setEnvironmentType(EnvironmentType.CLOUD)
            .build();

    protected static final TopologyEntityDTO COMPUTE_TIER_SMALL = TopologyEntityDTO.newBuilder()
            .setOid(OID_PROVIDER.incrementAndGet())
            .setDisplayName("COMPUTE_TIER_SMALL")
            .setEntityType(EntityType.COMPUTE_TIER_VALUE)
            .setEnvironmentType(EnvironmentType.CLOUD)
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                    .setComputeTier(ComputeTierInfo.newBuilder()
                            .setFamily("familyA")
                            .setNumCoupons(1)))
            .build();

    protected static final TopologyEntityDTO AVAILABILITY_ZONE_A = TopologyEntityDTO.newBuilder()
            .setOid(OID_PROVIDER.incrementAndGet())
            .setDisplayName("availability_zone_a")
            .setEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
            .setEnvironmentType(EnvironmentType.CLOUD)
            .build();

    protected static final TopologyEntityDTO AVAILABILITY_ZONE_B = TopologyEntityDTO.newBuilder()
            .setOid(OID_PROVIDER.incrementAndGet())
            .setDisplayName("availability_zone_b")
            .setEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
            .setEnvironmentType(EnvironmentType.CLOUD)
            .build();

    protected static final TopologyEntityDTO REGION = TopologyEntityDTO.newBuilder()
            .setOid(OID_PROVIDER.incrementAndGet())
            .setDisplayName("region")
            .setEntityType(EntityType.REGION_VALUE)
            .setEnvironmentType(EnvironmentType.CLOUD)
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                    .setConnectedEntityId(AVAILABILITY_ZONE_A.getOid())
                    .setConnectedEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
                    .setConnectionType(ConnectionType.OWNS_CONNECTION))
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                    .setConnectedEntityId(AVAILABILITY_ZONE_B.getOid())
                    .setConnectedEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
                    .setConnectionType(ConnectionType.OWNS_CONNECTION))
            .build();

    protected static final TopologyEntityDTO VIRTUAL_MACHINE_SMALL_A = TopologyEntityDTO.newBuilder()
            .setOid(OID_PROVIDER.incrementAndGet())
            .setDisplayName("virtual_machine_small_a")
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .setEnvironmentType(EnvironmentType.CLOUD)
            .setEntityState(EntityState.POWERED_ON)
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                    .setVirtualMachine(VirtualMachineInfo.newBuilder()
                            .setGuestOsInfo(OS.newBuilder()
                                    .setGuestOsType(OSType.LINUX))
                            .setTenancy(Tenancy.DEFAULT)))
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                    .setProviderEntityType(EntityType.COMPUTE_TIER_VALUE)
                    .setProviderId(COMPUTE_TIER_SMALL.getOid())
                    .addCommodityBought(CommodityBoughtDTO.newBuilder()
                            .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                                    .setType(CommodityType.NUM_VCORE_VALUE))
                            .setUsed(1.0)
                            .build())
                    .addCommodityBought(CommodityBoughtDTO.newBuilder()
                            .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                                    .setType(CommodityType.MEM_PROVISIONED_VALUE))
                            .setUsed(2.0)
                            .build()))
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                    .setConnectedEntityId(AVAILABILITY_ZONE_A.getOid())
                    .setConnectedEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
                    .setConnectionType(ConnectionType.AGGREGATED_BY_CONNECTION))
            .build();


    protected static final TopologyEntityDTO BUSINESS_ACCOUNT_A = TopologyEntityDTO.newBuilder()
            .setOid(OID_PROVIDER.incrementAndGet())
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                    .setBusinessAccount(BusinessAccountInfo.newBuilder()
                            .build()))
            .setDisplayName("business_account_a")
            .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
            .setEnvironmentType(EnvironmentType.CLOUD)
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                    .setConnectedEntityId(VIRTUAL_MACHINE_SMALL_A.getOid())
                    .setConnectedEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                    .setConnectionType(ConnectionType.OWNS_CONNECTION))
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                    .setConnectedEntityId(COMMITMENT_A_SCOPED_COMMITMENT_OID)
                    .setConnectedEntityType(EntityType.CLOUD_COMMITMENT_VALUE)
                    .setConnectionType(ConnectionType.OWNS_CONNECTION))
            .build();

    protected static final TopologyEntityDTO BUSINESS_ACCOUNT_B = BUSINESS_ACCOUNT_A.toBuilder()
            .setOid(OID_PROVIDER.incrementAndGet())
            .setDisplayName("business_account_b")
            .clearConnectedEntityList()
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                    .setConnectedEntityId(COMMITMENT_B_BF_SCOPED_OID)
                    .setConnectedEntityType(EntityType.CLOUD_COMMITMENT_VALUE)
                    .setConnectionType(ConnectionType.OWNS_CONNECTION))
            .build();

    protected static final Long BILLING_FAMILY_ID = OID_PROVIDER.incrementAndGet();

    protected static final GroupAndMembers BILLING_FAMILY_GROUPS =
            ImmutableGroupAndMembers.builder()
                    .group(GroupDTO.Grouping.newBuilder()
                            .setId(BILLING_FAMILY_ID)
                            .build())
                    .entities(ImmutableSet.of(BUSINESS_ACCOUNT_A.getOid(), BUSINESS_ACCOUNT_B.getOid()))
                    .members(ImmutableSet.of(BUSINESS_ACCOUNT_A.getOid(), BUSINESS_ACCOUNT_B.getOid()))
                    .build();

    protected static final TopologyEntityDTO COMPUTE_CLOUD_SERVICE = TopologyEntityDTO.newBuilder()
            .setOid(OID_PROVIDER.incrementAndGet())
            .setDisplayName("compute_cloud_service")
            .setEntityType(EntityType.CLOUD_SERVICE_VALUE)
            .setEnvironmentType(EnvironmentType.CLOUD)
            .build();

    protected static final TopologyEntityDTO ACCOUNT_A_SCOPED_COMMITMENT = TopologyEntityDTO.newBuilder()
            .setOid(COMMITMENT_A_SCOPED_COMMITMENT_OID)
            .setDisplayName("account_a_commitment")
            .setEntityType(EntityType.CLOUD_COMMITMENT_VALUE)
            .setEnvironmentType(EnvironmentType.CLOUD)
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                    .setCloudCommitmentData(CloudCommitmentInfo.newBuilder()
                            .setFamilyRestricted(FamilyRestricted.newBuilder()
                                    .setInstanceFamily("familyA")
                                    .build())
                            .setCommoditiesBought(CommittedCommoditiesBought.newBuilder()
                                    .addCommodity(CommittedCommodityBought.newBuilder()
                                            .setCapacity(1.0)
                                            .setCommodityType(CommodityType.NUM_VCORE))
                                    .addCommodity(CommittedCommodityBought.newBuilder()
                                            .setCapacity(4.0)
                                            .setCommodityType(CommodityType.MEM_PROVISIONED))
                                    .build())
                            .setCommitmentStatus(CloudCommitmentStatus.CLOUD_COMMITMENT_STATUS_ACTIVE)
                            .setCommitmentScope(CloudCommitmentScope.CLOUD_COMMITMENT_SCOPE_ACCOUNT)
                    .build()))
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                    .setConnectedEntityId(REGION.getOid())
                    .setConnectedEntityType(EntityType.REGION_VALUE)
                    .setConnectionType(ConnectionType.AGGREGATED_BY_CONNECTION))
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                    // add account scope
                    .setConnectedEntityId(BUSINESS_ACCOUNT_A.getOid())
                    .setConnectedEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
                    .setConnectionType(ConnectionType.NORMAL_CONNECTION))
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                    // add cloud service scope
                    .setConnectedEntityId(COMPUTE_CLOUD_SERVICE.getOid())
                    .setConnectedEntityType(EntityType.CLOUD_SERVICE_VALUE)
                    .setConnectionType(ConnectionType.NORMAL_CONNECTION))
            .build();

    protected static final TopologyEntityDTO ACCOUNT_B_BF_SCOPED_COMMITMENT = TopologyEntityDTO.newBuilder()
            .setOid(COMMITMENT_B_BF_SCOPED_OID)
            .setDisplayName("account_b_bf_commitment")
            .setEntityType(EntityType.CLOUD_COMMITMENT_VALUE)
            .setEnvironmentType(EnvironmentType.CLOUD)
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                    .setCloudCommitmentData(CloudCommitmentInfo.newBuilder()
                            .setFamilyRestricted(FamilyRestricted.newBuilder()
                                    .setInstanceFamily("familyA")
                                    .build())
                            .setCommoditiesBought(CommittedCommoditiesBought.newBuilder()
                                    .addCommodity(CommittedCommodityBought.newBuilder()
                                            .setCapacity(1.0)
                                            .setCommodityType(CommodityType.NUM_VCORE))
                                    .addCommodity(CommittedCommodityBought.newBuilder()
                                            .setCapacity(1.0)
                                            .setCommodityType(CommodityType.MEM_PROVISIONED))
                                    .build())
                            .setCommitmentStatus(CloudCommitmentStatus.CLOUD_COMMITMENT_STATUS_ACTIVE)
                            .setCommitmentScope(CloudCommitmentScope.CLOUD_COMMITMENT_SCOPE_BILLING_FAMILY_GROUP)
                            .build()))
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                    .setConnectedEntityId(REGION.getOid())
                    .setConnectedEntityType(EntityType.REGION_VALUE)
                    .setConnectionType(ConnectionType.AGGREGATED_BY_CONNECTION))
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                    // add cloud service scope
                    .setConnectedEntityId(COMPUTE_CLOUD_SERVICE.getOid())
                    .setConnectedEntityType(EntityType.CLOUD_SERVICE_VALUE)
                    .setConnectionType(ConnectionType.NORMAL_CONNECTION))
            .build();
}
