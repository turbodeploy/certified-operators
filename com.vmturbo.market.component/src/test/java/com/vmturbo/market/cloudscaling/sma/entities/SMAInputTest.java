package com.vmturbo.market.cloudscaling.sma.entities;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceBoughtCoupons;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.OS;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.AnalysisSettings;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.ReservedInstanceData;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.pricing.CloudRateExtractor;
import com.vmturbo.cost.calculation.pricing.CloudRateExtractor.ComputePriceBundle;
import com.vmturbo.cost.calculation.pricing.CloudRateExtractor.ComputePriceBundle.ComputePrice;
import com.vmturbo.cost.calculation.pricing.CloudRateExtractor.CoreBasedLicensePriceBundle;
import com.vmturbo.cost.calculation.pricing.ImmutableCoreBasedLicensePriceBundle;
import com.vmturbo.cost.calculation.topology.AccountPricingData;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopologyFactory.DefaultTopologyEntityCloudTopologyFactory;
import com.vmturbo.group.api.GroupAndMembers;
import com.vmturbo.group.api.ImmutableGroupAndMembers;
import com.vmturbo.market.topology.conversions.ConsistentScalingHelper;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualMachineData.VMBillingType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

/**
 * Class to test SMAInput functionality.
 */
public class SMAInputTest {

    private static final long vm1Id = 1L;
    private static final long vm2Id = 2L;
    private static final long ct1Id = 101L;
    private static final long ct2Id = 102L;
    private static final long regionId = 1001L;
    private static final long zoneId = 1002L;
    private static final long accountId = 10001L;
    private static final long billingFamilyId = 1000001L;
    private static final long riBoughtId = 10000001L;
    private static final long riSpecId1 = 10000002L;
    private static final long riSpecId2 = 10000003L;

    private static final String vm1Name = "VM1";
    private static final String vm2Name = "VM2";
    private static final String ct1Name = "t2.micro";
    private static final String ctFamily = "t2";
    private static final String regionName = "aws-east-1";
    private static final String zoneName = "aws-east-1a";
    private static final String accountName = "Development";
    private static final OSType osType = OSType.LINUX;

    private static final int ct1Coupons = 4;

    /*
     * Create the topology entity DTOs
     */
    // create VM that is not eligible to scale and is covered by an RI
    TopologyEntityDTO vm1Dto = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .setEntityState(EntityState.POWERED_ON)
            .setOid(vm1Id)
            .setDisplayName(vm1Name)
            .setEnvironmentType(EnvironmentType.CLOUD)
            .setAnalysisSettings(AnalysisSettings.newBuilder().setIsEligibleForScale(false).build())
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                    .setVirtualMachine(VirtualMachineInfo.newBuilder()
                            .setBillingType(VMBillingType.ONDEMAND)
                            .setTenancy(Tenancy.DEFAULT)
                            .setGuestOsInfo(OS.newBuilder()
                                    .setGuestOsType(osType)
                                    .setGuestOsName(osType.name()))))

            .build();
    // create VM that is eligible to scale
    TopologyEntityDTO vm2Dto = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .setEntityState(EntityState.POWERED_ON)
            .setOid(vm2Id)
            .setDisplayName(vm2Name)
            .setEnvironmentType(EnvironmentType.CLOUD)
            .setAnalysisSettings(AnalysisSettings.newBuilder().setIsEligibleForScale(true).build())
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                    .setVirtualMachine(VirtualMachineInfo.newBuilder()
                            .setBillingType(VMBillingType.ONDEMAND)
                            .setTenancy(Tenancy.DEFAULT)
                            .setGuestOsInfo(OS.newBuilder()
                                    .setGuestOsType(osType)
                                    .setGuestOsName(osType.name()))))

            .build();

    // create a compute tier that is t2.micro
    TopologyEntityDTO ct1Dto = TopologyEntityDTO.newBuilder().setEntityType(
            EntityType.COMPUTE_TIER_VALUE).setOid(ct1Id).setDisplayName(ct1Name).setEnvironmentType(
            EnvironmentType.CLOUD).setAnalysisSettings(
            AnalysisSettings.newBuilder().setIsEligibleForScale(false).build()).setTypeSpecificInfo(
            TypeSpecificInfo.newBuilder()
                    .setComputeTier(ComputeTierInfo.newBuilder()
                            .setFamily(ctFamily)
                            .setNumCoupons(ct1Coupons))).addConnectedEntityList(
            ConnectedEntity.newBuilder()
                    .setConnectedEntityId(regionId)
                    .setConnectedEntityType(EntityType.REGION_VALUE)
                    .build()).addCommoditySoldList(
            CommoditySoldDTO.newBuilder()
                    .setCommodityType(CommodityType.newBuilder()
                            .setKey("Linux")
                            .setType(CommodityDTO.CommodityType.LICENSE_ACCESS_VALUE)
                            .build())
                    .build()).build();
    Optional<TopologyEntityDTO> computeTier1Optional = Optional.of(ct1Dto);

    // create a region
    TopologyEntityDTO regionDto = TopologyEntityDTO.newBuilder().setEntityType(
            EntityType.REGION_VALUE).setOid(regionId).setDisplayName(regionName).setEnvironmentType(
            EnvironmentType.CLOUD).build();
    Optional<TopologyEntityDTO> regionalOptional = Optional.of(regionDto);

    // create a zone
    TopologyEntityDTO zoneDto = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
            .setOid(zoneId)
            .setDisplayName(zoneName)
            .setEnvironmentType(EnvironmentType.CLOUD)
            .build();
    Optional<TopologyEntityDTO> zoneOptional = Optional.of(zoneDto);

    // create a business account
    TopologyEntityDTO accountDto = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
            .setOid(accountId)
            .setDisplayName(accountName)
            .setEnvironmentType(EnvironmentType.CLOUD)
            .build();
    Optional<TopologyEntityDTO> accountOptional = Optional.of(accountDto);

    // create group and members
    GroupDTO.Grouping group = Grouping.newBuilder().setId(billingFamilyId).build();
    ImmutableGroupAndMembers groupAndMembers = ImmutableGroupAndMembers.builder().members(
            Arrays.asList(accountId)).group(group).entities(Collections.singleton(11111L)).build();
    Optional<GroupAndMembers> gAndMOptional = Optional.of(groupAndMembers);

    // Create RI data structures for XL
    final ReservedInstanceBought riBought1 = ReservedInstanceBought.newBuilder()
            .setId(riBoughtId)
            .setReservedInstanceBoughtInfo(ReservedInstanceBoughtInfo.newBuilder()
                    .setAvailabilityZoneId(zoneId)
                    .setBusinessAccountId(accountId)
                    .setNumBought(1)
                    .setReservedInstanceBoughtCoupons(
                            ReservedInstanceBoughtCoupons.newBuilder().setNumberOfCoupons(
                                    ct1Coupons).setNumberOfCouponsUsed(ct1Coupons).build())
                    .build())
            .build();

    final ReservedInstanceBought riBought2 = ReservedInstanceBought.newBuilder()
            .setId(riBoughtId)
            .setReservedInstanceBoughtInfo(ReservedInstanceBoughtInfo.newBuilder()
                    .setAvailabilityZoneId(zoneId)
                    .setBusinessAccountId(accountId)
                    .setNumBought(2)
                    .setReservedInstanceBoughtCoupons(
                            ReservedInstanceBoughtCoupons.newBuilder().setNumberOfCoupons(
                                    2 * ct1Coupons).setNumberOfCouponsUsed(ct1Coupons).build())
                    .build())
            .build();

    final ReservedInstanceSpec riSpec1 = ReservedInstanceSpec.newBuilder()
            .setId(riSpecId1)
            .setReservedInstanceSpecInfo(ReservedInstanceSpecInfo.newBuilder()
                    .setOs(OSType.LINUX)
                    .setSizeFlexible(true)
                    .setRegionId(regionId)
                    .setTenancy(Tenancy.DEFAULT)
                    .setTierId(ct1Id)
                    .build())
            .build();

    final ReservedInstanceSpec riSpec2 = ReservedInstanceSpec.newBuilder()
            .setId(riSpecId2)
            .setReservedInstanceSpecInfo(ReservedInstanceSpecInfo.newBuilder()
                    .setOs(OSType.LINUX)
                    .setSizeFlexible(true)
                    .setRegionId(regionId)
                    .setTenancy(Tenancy.DEFAULT)
                    .setTierId(ct2Id)
                    .build())
            .build();

    // create cloud cost data
    EntityReservedInstanceCoverage coverage =
            EntityReservedInstanceCoverage.newBuilder().setEntityId(vm1Id).putCouponsCoveredByRi(
                    riBoughtId, 4.0).build();

    ConsistentScalingHelper consistentScalingHelper = new ConsistentScalingHelper(null);

    DefaultTopologyEntityCloudTopologyFactory defaultFactory = spy(
            new DefaultTopologyEntityCloudTopologyFactory(null));

    /**
     * Test cloud topology to SMAInputContext conversion.
     */
    @Test
    public void testSMAInput() {

        // create cloudTopology
        final List<TopologyEntityDTO> dtos = Arrays.asList(vm1Dto, vm2Dto, ct1Dto, regionDto,
                zoneDto, accountDto);
        final CloudTopology<TopologyEntityDTO> cloudTopology = spy(
                defaultFactory.newCloudTopology(dtos.stream()));
        when(cloudTopology.getConnectedRegion(anyLong())).thenReturn(regionalOptional);
        when(cloudTopology.getComputeTier(anyLong())).thenReturn(computeTier1Optional);
        when(cloudTopology.getConnectedAvailabilityZone(anyLong())).thenReturn(zoneOptional);
        when(cloudTopology.getOwner(anyLong())).thenReturn(accountOptional);
        doReturn(gAndMOptional).when(cloudTopology).getBillingFamilyForEntity(anyLong());

        //create compute price
        final ComputePrice computePrice = ComputePrice.builder().accountId(accountId)
                .osType(OSType.LINUX)
                .hourlyComputeRate(0.2f)
                .hourlyLicenseRate(0)
                .isBasePrice(true)
                .build();
        final AccountPricingData accountPricingData = mock(AccountPricingData.class);
        final ComputePriceBundle computePriceBundle1 = mock(ComputePriceBundle.class);
        when(computePriceBundle1.getPrices()).thenReturn(ImmutableList.of(computePrice));

        //create ri price
        final CoreBasedLicensePriceBundle riPrice = ImmutableCoreBasedLicensePriceBundle.builder()
                .osType(OSType.LINUX)
                .price(0.1f)
                .numCores(1)
                .isBurstableCPU(false)
                .licenseCommodityType(CommodityType.newBuilder()
                        .setType(CommodityDTO.CommodityType.LICENSE_ACCESS_VALUE)
                        .build())
                .build();

        // create MarketPriceTable
        final CloudRateExtractor marketCloudRateExtractor = mock(CloudRateExtractor.class);
        when(marketCloudRateExtractor.getComputePriceBundle(ct1Dto, regionId,
                accountPricingData)).thenReturn(computePriceBundle1);
        when(marketCloudRateExtractor.getReservedLicensePriceBundles(accountPricingData,
                ct1Dto)).thenReturn(ImmutableSet.of(riPrice));

        //create CloudCostData
        final CloudCostData<TopologyEntityDTO> cloudCostData = mock(CloudCostData.class);
        when(cloudCostData.getAccountPricingData(Mockito.anyLong())).thenReturn(
                Optional.of(accountPricingData));

        final ReservedInstanceData riData1 = new ReservedInstanceData(riBought1, riSpec1);
        final ReservedInstanceData riData2 = new ReservedInstanceData(riBought2, riSpec2);
        when(cloudCostData.getExistingRiBought()).thenReturn(ImmutableList.of(riData1, riData2));

        when(cloudCostData.getRiCoverageForEntity(vm1Id)).thenReturn(Optional.of(coverage));
        when(cloudCostData.getRiCoverageForEntity(vm2Id)).thenReturn(Optional.empty());

        // create VM to providers map, if there is no provider - vm is not movable
        Map<Long, Set<Long>> providers = new HashMap<>();
        providers.put(vm2Id, ImmutableSet.of(ct1Id));

        //create SMAInput
        final SMAInput smaInput = new SMAInput(cloudTopology, providers, cloudCostData,
                marketCloudRateExtractor, consistentScalingHelper, false, false);
        Assert.assertEquals(1, smaInput.getContexts().size());
        final SMAInputContext smaContext = smaInput.getContexts().iterator().next();
        Assert.assertEquals(2, smaContext.getVirtualMachines().size());

        //this VM is not movable, so it should remain on same template, and it is also covered by
        //bought RI
        final Optional<SMAVirtualMachine> vm1 = findVMByOid(smaContext, vm1Id);
        Assert.assertTrue(vm1.isPresent());
        // no provider templates, because VM is not movable
        Assert.assertEquals(0, vm1.get().getProviders().size());
        Assert.assertEquals(1, vm1.get().getGroupProviders().size());
        Assert.assertEquals(vm1.get().getCurrentTemplate(), vm1.get().getGroupProviders().iterator().next());
        //natural template == current template, because VM is not movable
        Assert.assertEquals(vm1.get().getCurrentTemplate(), vm1.get().getNaturalTemplate());
        Assert.assertEquals(vm1.get().getCurrentTemplate().getCoupons(),
                vm1.get().getCurrentRICoverage(), 0.001f);

        //this VM is movable and should have provider templates
        final Optional<SMAVirtualMachine> vm2 = findVMByOid(smaContext, vm2Id);
        Assert.assertTrue(vm2.isPresent());
        Assert.assertEquals(1, vm2.get().getProviders().size());
        Assert.assertEquals(ct1Name, vm2.get().getProviders().iterator().next().getName());

        Assert.assertEquals(1, smaContext.getTemplates().size());
        Assert.assertEquals(ct1Name, smaContext.getTemplates().iterator().next().getName());

        Assert.assertEquals(1, smaContext.getReservedInstances().size());
    }

    private static Optional<SMAVirtualMachine> findVMByOid(SMAInputContext smaContext, long oid) {
        return smaContext.getVirtualMachines()
                .stream()
                .filter(vm -> vm.getOid() == oid)
                .findFirst();
    }
}
