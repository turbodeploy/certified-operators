package com.vmturbo.reserved.instance.coverage.allocator;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceBoughtCoupons;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceScopeInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.OS;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PerTargetEntityInformation;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.DiscoveryOrigin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Origin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.BusinessAccountInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

// package-private
class AzureAllocationTopologyTest {

    protected static final AtomicLong OID_PROVIDER = new AtomicLong();

    protected static final TopologyEntityDTO COMPUTE_TIER_SMALL = TopologyEntityDTO.newBuilder()
            .setOid(OID_PROVIDER.incrementAndGet())
            .setDisplayName("COMPUTE_TIER_SMALL")
            .setEntityType(EntityType.COMPUTE_TIER_VALUE)
            .setEnvironmentType(EnvironmentType.CLOUD)
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                    .setComputeTier(ComputeTierInfo.newBuilder()
                            .setFamily("familyA")
                            .setNumCoupons(1)))
            .setOrigin(Origin.newBuilder()
                    .setDiscoveryOrigin(DiscoveryOrigin.newBuilder()
                            .putDiscoveredTargetData(OID_PROVIDER.incrementAndGet(),
                                    PerTargetEntityInformation.newBuilder().build())
                            .build())
                    .build())
            .build();

    protected static final TopologyEntityDTO COMPUTER_TIER_MEDIUM = TopologyEntityDTO.newBuilder()
            .setOid(OID_PROVIDER.incrementAndGet())
            .setDisplayName("COMPUTER_TIER_MEDIUM")
            .setEntityType(EntityType.COMPUTE_TIER_VALUE)
            .setEnvironmentType(EnvironmentType.CLOUD)
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                    .setComputeTier(ComputeTierInfo.newBuilder()
                            .setFamily("familyA")
                            .setNumCoupons(2)))
            .setOrigin(Origin.newBuilder()
                    .setDiscoveryOrigin(DiscoveryOrigin.newBuilder()
                            .putDiscoveredTargetData(OID_PROVIDER.incrementAndGet(),
                                    PerTargetEntityInformation.newBuilder().build())
                            .build())
                    .build())
            .build();

    protected static final TopologyEntityDTO REGION = TopologyEntityDTO.newBuilder()
            .setOid(OID_PROVIDER.incrementAndGet())
            .setDisplayName("region")
            .setEntityType(EntityType.REGION_VALUE)
            .setEnvironmentType(EnvironmentType.CLOUD)
            .setOrigin(Origin.newBuilder()
                    .setDiscoveryOrigin(DiscoveryOrigin.newBuilder()
                            .putDiscoveredTargetData(OID_PROVIDER.incrementAndGet(),
                                    PerTargetEntityInformation.newBuilder().build())
                            .build())
                    .build())
            .build();

    protected static final TopologyEntityDTO VIRTUAL_MACHINE_SMALL_A = TopologyEntityDTO.newBuilder()
            .setOid(OID_PROVIDER.incrementAndGet())
            .setDisplayName("VIRTUAL_MACHINE_SMALL_A")
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .setEnvironmentType(EnvironmentType.CLOUD)
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                    .setVirtualMachine(VirtualMachineInfo.newBuilder()
                            .setGuestOsInfo(OS.newBuilder()
                                    .setGuestOsType(OSType.LINUX))
                            .setTenancy(Tenancy.DEFAULT)))
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                    .setProviderEntityType(EntityType.COMPUTE_TIER_VALUE)
                    .setProviderId(COMPUTE_TIER_SMALL.getOid()))
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                    .setConnectedEntityId(REGION.getOid())
                    .setConnectedEntityType(EntityType.REGION_VALUE)
                    .setConnectionType(ConnectionType.AGGREGATED_BY_CONNECTION))
            .setOrigin(Origin.newBuilder()
                    .setDiscoveryOrigin(DiscoveryOrigin.newBuilder()
                            .putDiscoveredTargetData(OID_PROVIDER.incrementAndGet(),
                                    PerTargetEntityInformation.newBuilder().build())
                            .build())
                    .build())
            .build();


    protected static final TopologyEntityDTO BUSINESS_ACCOUNT = TopologyEntityDTO.newBuilder()
            .setOid(OID_PROVIDER.incrementAndGet())
            .setDisplayName("bussiness_account")
            .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
            .setEnvironmentType(EnvironmentType.CLOUD)
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                    .setConnectedEntityId(VIRTUAL_MACHINE_SMALL_A.getOid())
                    .setConnectedEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                    .setConnectionType(ConnectionType.OWNS_CONNECTION))
            .setOrigin(Origin.newBuilder()
                    .setDiscoveryOrigin(DiscoveryOrigin.newBuilder()
                            .putDiscoveredTargetData(OID_PROVIDER.incrementAndGet(),
                                    PerTargetEntityInformation.newBuilder().build())
                            .build())
                    .build())
            .build();

    protected static final ReservedInstanceSpec RI_SPEC_SMALL = ReservedInstanceSpec.newBuilder()
            .setId(OID_PROVIDER.incrementAndGet())
            .setReservedInstanceSpecInfo(ReservedInstanceSpecInfo.newBuilder()
                    .setTenancy(Tenancy.DEFAULT)
                    .setOs(OSType.LINUX)
                    .setTierId(COMPUTE_TIER_SMALL.getOid())
                    .setRegionId(REGION.getOid())
                    .setPlatformFlexible(true)
                    .setSizeFlexible(false)
                    .setType(ReservedInstanceType.newBuilder()
                            .setTermYears(1)))
            .build();

    protected static final ReservedInstanceBought RI_BOUGHT_SMALL = ReservedInstanceBought.newBuilder()
            .setId(OID_PROVIDER.incrementAndGet())
            .setReservedInstanceBoughtInfo(ReservedInstanceBoughtInfo.newBuilder()
                    .setBusinessAccountId(BUSINESS_ACCOUNT.getOid())
                    .setStartTime(Instant.now().toEpochMilli())
                    .setReservedInstanceSpec(RI_SPEC_SMALL.getId())
                    .setReservedInstanceScopeInfo(ReservedInstanceScopeInfo.newBuilder()
                            .setShared(true))
                    .setReservedInstanceBoughtCoupons(ReservedInstanceBoughtCoupons.newBuilder()
                            .setNumberOfCoupons(1)
                            .build()))
            .build();

}
