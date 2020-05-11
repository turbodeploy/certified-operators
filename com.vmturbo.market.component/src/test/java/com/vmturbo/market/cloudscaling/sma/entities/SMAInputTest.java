package com.vmturbo.market.cloudscaling.sma.entities;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Table;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceBoughtCoupons;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.OS;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.AnalysisSettings;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.ReservedInstanceData;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.topology.AccountPricingData;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopologyFactory.DefaultTopologyEntityCloudTopologyFactory;
import com.vmturbo.group.api.GroupAndMembers;
import com.vmturbo.group.api.ImmutableGroupAndMembers;
import com.vmturbo.market.cloudscaling.sma.analysis.StableMarriagePerContext;
import com.vmturbo.market.cloudscaling.sma.entities.SMAInput.CspFromRegion;
import com.vmturbo.market.cloudscaling.sma.entities.SMAInput.SMAReservedInstanceKeyIDGenerator;
import com.vmturbo.market.topology.conversions.ConsistentScalingHelper;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualMachineData.VMBillingType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

/**
 * Class to test SMAInput functionality.
 */
public class SMAInputTest {

    private static final Logger logger = LogManager.getLogger();

    private static final long vm1Id = 1L;
    private static final long vm2Id = 2L;
    private static final long vm3Id = 3L;
    private static final long ri1Id = 11L;
    private static final long ct1Id = 101L;
    private static final long ct2Id = 102L;
    private static final long regionId = 1001L;
    private static final long zoneId = 1002L;
    private static final long accountId = 10001L;
    private static final long billingFamilyId = 1000001L;
    private static final long riBoughtId = 10000001L;
    private static final long riSpecId   = 10000002L;
    private static final long coverageId = 2001L;

    private static final String vm1Name = "VM1";
    private static final String vm2Name = "VM2";
    private static final String vm3Name = "VM3";
    private static final String ri1Name = "RI1";
    private static final String ct1Name = "t2.micro";
    private static final String ct2Name = "t2.small";
    private static final String ctFamily = "t2";
    private static final String regionName = "aws-east-1";
    private static final String zoneName = "aws-east-1a";
    private static final String accountName = "Development";
    private static final OSType osType = OSType.LINUX;

    private static final int ct1Coupons = 4;
    private static final int ct2Coupons = 8;

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
        .setAnalysisSettings(
            AnalysisSettings.newBuilder()
                .setIsEligibleForScale(false)
                .build())
        .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
            .setVirtualMachine(
                VirtualMachineInfo.newBuilder()
                    .setBillingType(VMBillingType.ONDEMAND)
                    .setTenancy(Tenancy.DEFAULT)
                    .setGuestOsInfo(OS.newBuilder()
                        .setGuestOsType(osType)
                        .setGuestOsName(osType.name()))
            )
        )
        .build();
    // create VM that is eligible to scale
    TopologyEntityDTO vm2Dto = TopologyEntityDTO.newBuilder()
        .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
        .setEntityState(EntityState.POWERED_ON)
        .setOid(vm2Id)
        .setDisplayName(vm2Name)
        .setEnvironmentType(EnvironmentType.CLOUD)
        .setAnalysisSettings(
            AnalysisSettings.newBuilder()
                .setIsEligibleForScale(true)
                .build())
        .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
            .setVirtualMachine(
                VirtualMachineInfo.newBuilder()
                    .setBillingType(VMBillingType.ONDEMAND)
                    .setTenancy(Tenancy.DEFAULT)
                    .setGuestOsInfo(OS.newBuilder()
                        .setGuestOsType(osType)
                        .setGuestOsName(osType.name()))
            )
        )
        .build();

    // create VM that is not controllable
    TopologyEntityDTO vm3Dto = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .setEntityState(EntityState.POWERED_ON)
            .setOid(vm3Id)
            .setDisplayName(vm3Name)
            .setEnvironmentType(EnvironmentType.CLOUD)
            .setAnalysisSettings(
                    AnalysisSettings.newBuilder()
                            .setIsEligibleForScale(true)
                            .setControllable(false)
                            .build())
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                    .setVirtualMachine(
                            VirtualMachineInfo.newBuilder()
                                    .setBillingType(VMBillingType.ONDEMAND)
                                    .setTenancy(Tenancy.DEFAULT)
                                    .setGuestOsInfo(OS.newBuilder()
                                            .setGuestOsType(osType)
                                            .setGuestOsName(osType.name()))
                    )
            )
            .build();

    // create a compute tier that is t2.micro
    TopologyEntityDTO ct1Dto = TopologyEntityDTO.newBuilder()
        .setEntityType(EntityType.COMPUTE_TIER_VALUE)
        .setOid(ct1Id)
        .setDisplayName(ct1Name)
        .setEnvironmentType(EnvironmentType.CLOUD)
        .setAnalysisSettings(
            AnalysisSettings.newBuilder()
                .setIsEligibleForScale(false)
                .build())
        .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
            .setComputeTier(
                ComputeTierInfo.newBuilder()
                    .setFamily(ctFamily)
                    .setNumCoupons(ct1Coupons)
            )
        )
        .build();
    Optional<TopologyEntityDTO> computeTier1Optional = Optional.of(ct1Dto);

    // create a compute tier that is t2.small
    TopologyEntityDTO ct2Dto = TopologyEntityDTO.newBuilder()
        .setEntityType(EntityType.COMPUTE_TIER_VALUE)
        .setOid(ct2Id)
        .setDisplayName(ct2Name)
        .setEnvironmentType(EnvironmentType.CLOUD)
        .setAnalysisSettings(
            AnalysisSettings.newBuilder()
                .setIsEligibleForScale(false)
                .build())
        .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
            .setComputeTier(
                ComputeTierInfo.newBuilder()
                    .setFamily(ctFamily)
                    .setNumCoupons(ct2Coupons)
            )
        )
        .build();
    Optional<TopologyEntityDTO> computeTier2Optional = Optional.of(ct2Dto);

    // create a region
    TopologyEntityDTO regionDto = TopologyEntityDTO.newBuilder()
        .setEntityType(EntityType.REGION_VALUE)
        .setOid(regionId)
        .setDisplayName(regionName)
        .setEnvironmentType(EnvironmentType.CLOUD)
        .build();
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
    ImmutableGroupAndMembers groupAndMembers = ImmutableGroupAndMembers.builder()
        .members(Arrays.asList(accountId))
        .group(group)
        .entities(Collections.singleton(11111L))
        .build();
    Optional<GroupAndMembers> gAndMOptional = Optional.of(groupAndMembers);


    // Create RI data structures for XL
    final ReservedInstanceBought riBought1 = ReservedInstanceBought.newBuilder().setId(riBoughtId)
        .setReservedInstanceBoughtInfo(ReservedInstanceBoughtInfo.newBuilder().setAvailabilityZoneId(zoneId)
            .setBusinessAccountId(accountId)
            .setNumBought(1)
            .setReservedInstanceBoughtCoupons(ReservedInstanceBoughtCoupons.newBuilder()
                .setNumberOfCoupons(ct1Coupons)
                .setNumberOfCouponsUsed(ct1Coupons).build()).build()).build();

    final ReservedInstanceBought riBought2 = ReservedInstanceBought.newBuilder().setId(riBoughtId)
        .setReservedInstanceBoughtInfo(ReservedInstanceBoughtInfo.newBuilder().setAvailabilityZoneId(zoneId)
            .setBusinessAccountId(accountId)
            .setNumBought(2)
            .setReservedInstanceBoughtCoupons(ReservedInstanceBoughtCoupons.newBuilder()
                .setNumberOfCoupons(2 * ct1Coupons)
                .setNumberOfCouponsUsed(ct1Coupons).build()).build()).build();

    final ReservedInstanceSpec riSpec1 = ReservedInstanceSpec.newBuilder().setId(riSpecId)
        .setReservedInstanceSpecInfo(ReservedInstanceSpecInfo.newBuilder().setOs(OSType.LINUX)
            .setSizeFlexible(true)
            .setRegionId(regionId)
            .setTenancy(Tenancy.DEFAULT)
            .setTierId(ct1Id).build()).build();

    final ReservedInstanceBought riBought3 = ReservedInstanceBought.newBuilder().setId(riBoughtId)
        .setReservedInstanceBoughtInfo(ReservedInstanceBoughtInfo.newBuilder().setAvailabilityZoneId(zoneId)
            .setBusinessAccountId(accountId)
            .setNumBought(1)
            .setReservedInstanceBoughtCoupons(ReservedInstanceBoughtCoupons.newBuilder()
                .setNumberOfCoupons(ct2Coupons)
                .setNumberOfCouponsUsed(ct1Coupons).build()).build()).build();

    final ReservedInstanceSpec riSpec2 = ReservedInstanceSpec.newBuilder().setId(riSpecId)
        .setReservedInstanceSpecInfo(ReservedInstanceSpecInfo.newBuilder().setOs(OSType.LINUX)
            .setSizeFlexible(true)
            .setRegionId(regionId)
            .setTenancy(Tenancy.DEFAULT)
            .setTierId(ct2Id).build()).build();

    // create cloud cost data
    EntityReservedInstanceCoverage coverage = EntityReservedInstanceCoverage.newBuilder()
        .setEntityId(vm1Id)
        .putCouponsCoveredByRi(riBoughtId, 4.0)
        .build();
    Map<Long, EntityReservedInstanceCoverage> riCoverageByEntityId = ImmutableMap.of(vm1Id, coverage);
    Map<Long, ReservedInstanceBought> riBoughtById = ImmutableMap.of(riBoughtId, riBought1);
    Map<Long, ReservedInstanceSpec> riSpecById = ImmutableMap.of(riSpecId, riSpec1);
    Map<Long, ReservedInstanceBought> buyRIBoughtById = new HashMap<>();
    Map<Long, AccountPricingData<TopologyEntityDTO>> accountPricingDataByBusinessAccountOid =
        new HashMap<>();
    Map<Long, EntityReservedInstanceCoverage> filteredRiCoverageByEntityId = ImmutableMap.of(vm1Id, coverage);
    CloudCostData<TopologyEntityDTO> cloudCostData =
        new CloudCostData<TopologyEntityDTO>(riCoverageByEntityId, filteredRiCoverageByEntityId,
            riBoughtById, riSpecById, buyRIBoughtById, accountPricingDataByBusinessAccountOid);

    ConsistentScalingHelper consistentScalingHelper = new ConsistentScalingHelper(null);

    SMAReservedInstanceKeyIDGenerator reservedInstanceKeyIDGenerator =
        new SMAReservedInstanceKeyIDGenerator();


    DefaultTopologyEntityCloudTopologyFactory defaultFactory = spy(
        new DefaultTopologyEntityCloudTopologyFactory(null));

    /*
     * Test remove coupons from an RI that is covering a VM that is not eligible to scale.
     * Test that RIs that cover VMs that are not eligible to scale have those covered coupons
     * removed from the RI.  If a VM is not scalable, it is not added as a SMAVirtualMachine.
     *
     * <p>Call SMAInput.processVirtualMachines to create the RI bought ID to coupons map.
     * Call SMAInput.processReservedInstances to use the map.
     * Between calls, update the VM's template.
     * Call SMAInput.generateInputContexts to generate the input contexts.
     * Call StableMarriagePerContext.execute to generate the SMAOutput
     */
    /**
     * First test: 2 VMs, vm1 and vm2, and one RI, ri, with a count of 1.
     * All are the same template, t1.
     * vm1 is covered by ri and is not eligible to scale.
     * Outcome: vm2 scale to t1 and is not discounted.
     */
    @Test()
    public void testNonScalableVMCoveredByRI() {

        // create cloudTopology
        final List<TopologyEntityDTO> dtos = Arrays.asList(vm1Dto, vm2Dto, vm3Dto, ct1Dto, regionDto,
            zoneDto, accountDto);
        CloudTopology<TopologyEntityDTO> cloudTopology = spy(defaultFactory.newCloudTopology(dtos.stream()));

        when(cloudTopology.getConnectedRegion(anyLong())).thenReturn(regionalOptional);
        when(cloudTopology.getComputeTier(anyLong())).thenReturn(computeTier1Optional);
        when(cloudTopology.getConnectedAvailabilityZone(anyLong())).thenReturn(zoneOptional);
        // doReturn(zoneOptional).when(cloudTopology).getConnectedAvailabilityZone(anyLong());
        when(cloudTopology.getOwner(anyLong())).thenReturn(accountOptional);
        // doReturn(accountOptional).when(cloudTopology).getOwner(anyLong());
        // when(cloudTopology.getBillingFamilyForEntity(anyLong())).thenReturn(gAndMOptional);
        doReturn(gAndMOptional).when(cloudTopology).getBillingFamilyForEntity(anyLong());

        /*
         * create SMA data structures
         */
        // create SMAContext
        SMAContext context = new SMAContext(SMACSP.AWS, OSType.LINUX, regionId,
            billingFamilyId, Tenancy.DEFAULT);

        // create SMATemplate
        Table<Long, SMAContext, SMATemplate> computeTierOidToContextToTemplate = HashBasedTable.create();
        SMATemplate template = new SMATemplate(ct1Id, ct1Name, ctFamily, ct1Coupons, context, ct1Dto);
        Map<SMAContext, Set<SMATemplate>> smaContextToTemplates = new HashMap<>();
        smaContextToTemplates.put(context, Collections.singleton(template));
        computeTierOidToContextToTemplate.put(ct1Id, context, template);

        /*
         * Process VMs
         */
        CspFromRegion cspFromRegion = new CspFromRegion();
        Map<SMAContext, Set<SMAVirtualMachine>> smaContextToVMs = new HashMap<>();
        Map<Long, Float> riBoughtIdToCouponsUsed = new HashMap<>();
        // Map from region ID to OSType to context: used to restrict template and RI creation.
        Table<Long, OSType, Set<SMAContext>> regionIdToOsTypeToContexts = HashBasedTable.create();
        Map<SMAContext, Set<Long>> contextToBusinessAccountIds = new HashMap<>();
        Map<SMAContext, Set<OSType>> contextToOSTypes = new HashMap<>();

        SMAInput.processVirtualMachine(vm1Dto, cloudTopology, cloudCostData, consistentScalingHelper,
            cspFromRegion, smaContextToVMs, riBoughtIdToCouponsUsed, regionIdToOsTypeToContexts,
            contextToBusinessAccountIds, contextToOSTypes);

        SMAInput.processVirtualMachine(vm3Dto, cloudTopology, cloudCostData, consistentScalingHelper,
                cspFromRegion, smaContextToVMs, riBoughtIdToCouponsUsed, regionIdToOsTypeToContexts,
                contextToBusinessAccountIds, contextToOSTypes);

        Assert.assertTrue(smaContextToVMs.size() == 0);
        Assert.assertTrue(riBoughtIdToCouponsUsed.size() == 1);
        Assert.assertTrue(riBoughtIdToCouponsUsed.get(riBoughtId) != null);
        Assert.assertTrue(riBoughtIdToCouponsUsed.get(riBoughtId) == 4);
        Assert.assertTrue(regionIdToOsTypeToContexts.size() == 0);
        Assert.assertTrue(contextToBusinessAccountIds.size() == 0);
        Assert.assertTrue(contextToOSTypes.size() == 0);

        SMAInput.processVirtualMachine(vm2Dto, cloudTopology, cloudCostData, consistentScalingHelper,
            cspFromRegion, smaContextToVMs, riBoughtIdToCouponsUsed, regionIdToOsTypeToContexts,
            contextToBusinessAccountIds, contextToOSTypes);
        Assert.assertTrue(smaContextToVMs.size() == 1);
        Assert.assertTrue(riBoughtIdToCouponsUsed.size() == 1);
        Assert.assertTrue(riBoughtIdToCouponsUsed.get(riBoughtId) == 4);
        Assert.assertTrue(regionIdToOsTypeToContexts.size() == 1);
        Assert.assertTrue(contextToBusinessAccountIds.size() == 1);
        Assert.assertTrue(contextToOSTypes.size() == 1);

        Set<SMAVirtualMachine> vms = smaContextToVMs.get(context);
        Assert.assertTrue(vms.size() == 1);
        SMAVirtualMachine vm = vms.stream().findFirst().orElse(null);
        // updateVirtualMachines does: set currentTemplate, naturalTemplate, and Providers
        Assert.assertTrue(vm != null);
        vm.setCurrentTemplate(template);
        vm.setNaturalTemplate(template);
        vm.setProviders(Collections.singletonList(template));

        // process SMAReservedInstances
        Map<SMAContext, Set<SMAReservedInstance>> smaContextToRIs = new HashMap<>();
        Map<Long, SMAReservedInstance> riBoughtOidToRI = new HashMap<>();
        final ReservedInstanceData riData = new ReservedInstanceData(riBought1, riSpec1);

        boolean created = SMAInput.processReservedInstance(riData, cloudTopology,
            computeTierOidToContextToTemplate, regionIdToOsTypeToContexts, riBoughtIdToCouponsUsed,
            cspFromRegion, reservedInstanceKeyIDGenerator, smaContextToRIs, riBoughtOidToRI);
        Assert.assertTrue(smaContextToRIs.size() == 0);
        Assert.assertTrue(riBoughtOidToRI.size() == 0);
        Assert.assertTrue(smaContextToRIs.get(context) == null);
        Assert.assertTrue(riBoughtIdToCouponsUsed.size() == 1);
        Assert.assertTrue(riBoughtIdToCouponsUsed.get(riBoughtId) == 4);

        // Generate input context
        List<SMAInputContext> inputContexts = SMAInput.generateInputContexts(smaContextToVMs, smaContextToRIs,
            smaContextToTemplates);
        Assert.assertTrue(inputContexts.size() == 1);
        SMAInputContext inputContext = inputContexts.get(0);

        // Call Stable marriage
        SMAOutputContext outputContext = StableMarriagePerContext.execute(inputContext);
        Assert.assertTrue(outputContext != null);
        Assert.assertTrue(outputContext.getContext().equals(context));
        List<SMAMatch> matches = outputContext.getMatches();
        Assert.assertTrue(matches.size() == 1);
        SMAMatch match = matches.get(0);
        SMAVirtualMachine matchVM = match.getVirtualMachine();
        Assert.assertTrue(matchVM.equals(vm));
        Assert.assertTrue(match.getTemplate() != null);
        Assert.assertTrue(match.getTemplate().equals(template));
        Assert.assertTrue(match.getDiscountedCoupons() == 0);
        Assert.assertTrue(match.getReservedInstance() == null);
    }

    /**
     * Second test: 2 VMs, vm1 and vm2, and one RI, ri, with a count of 2.
     * All are the same template, t2.micro.
     * vm1 is covered by ri and is not eligible to scale.
     * Outcome: vm2 scale to t1 and is discounted by ri.
     */
    @Test()
    public void testNonScalableVMCoveredByRIWithCountOfTwo() {

        // create cloudTopology
        final List<TopologyEntityDTO> dtos = Arrays.asList(vm1Dto, vm2Dto, ct1Dto, regionDto,
            zoneDto, accountDto);
        CloudTopology<TopologyEntityDTO> cloudTopology = spy(defaultFactory.newCloudTopology(dtos.stream()));

        when(cloudTopology.getConnectedRegion(anyLong())).thenReturn(regionalOptional);
        when(cloudTopology.getComputeTier(anyLong())).thenReturn(computeTier1Optional);
        when(cloudTopology.getConnectedAvailabilityZone(anyLong())).thenReturn(zoneOptional);
        // doReturn(zoneOptional).when(cloudTopology).getConnectedAvailabilityZone(anyLong());
        when(cloudTopology.getOwner(anyLong())).thenReturn(accountOptional);
        // doReturn(accountOptional).when(cloudTopology).getOwner(anyLong());
        // when(cloudTopology.getBillingFamilyForEntity(anyLong())).thenReturn(gAndMOptional);
        doReturn(gAndMOptional).when(cloudTopology).getBillingFamilyForEntity(anyLong());

        /*
         * create SMA data structures
         */
        // create SMAContext
        SMAContext context = new SMAContext(SMACSP.AWS, OSType.LINUX, regionId,
            billingFamilyId, Tenancy.DEFAULT);

        /*
         * create SMATemplate
         */
        final Table<Long, SMAContext, SMATemplate> computeTierOidToContextToTemplate = HashBasedTable.create();
        SMATemplate template = new SMATemplate(ct1Id, ct1Name, ctFamily, ct1Coupons, context, ct1Dto);
        template.setOnDemandCost(accountId, osType, new SMACost(1.0f, 0.0f));
        template.setDiscountedCost(accountId, osType, new SMACost(0.0f, 0.0f));

        Map<SMAContext, Set<SMATemplate>> smaContextToTemplates = new HashMap<>();
        smaContextToTemplates.put(context, Collections.singleton(template));
        computeTierOidToContextToTemplate.put(ct1Id, context, template);

        /*
         * Process VMs
         */
        CspFromRegion cspFromRegion = new CspFromRegion();
        Map<SMAContext, Set<SMAVirtualMachine>> smaContextToVMs = new HashMap<>();
        Map<Long, Float> riBoughtIdToCouponsUsed = new HashMap<>();
        // Map from region ID to OSType to context: used to restrict template and RI creation.
        Table<Long, OSType, Set<SMAContext>> regionIdToOsTypeToContexts = HashBasedTable.create();
        Map<SMAContext, Set<Long>> contextToBusinessAccountIds = new HashMap<>();
        Map<SMAContext, Set<OSType>> contextToOSTypes = new HashMap<>();

        SMAInput.processVirtualMachine(vm1Dto, cloudTopology, cloudCostData, consistentScalingHelper,
            cspFromRegion, smaContextToVMs, riBoughtIdToCouponsUsed, regionIdToOsTypeToContexts,
            contextToBusinessAccountIds, contextToOSTypes);

        Assert.assertTrue(smaContextToVMs.size() == 0);
        Assert.assertTrue(riBoughtIdToCouponsUsed.size() == 1);
        Assert.assertTrue(riBoughtIdToCouponsUsed.get(riBoughtId) != null);
        Assert.assertTrue(riBoughtIdToCouponsUsed.get(riBoughtId) == 4);
        Assert.assertTrue(regionIdToOsTypeToContexts.size() == 0);
        Assert.assertTrue(contextToBusinessAccountIds.size() == 0);
        Assert.assertTrue(contextToOSTypes.size() == 0);

        SMAInput.processVirtualMachine(vm2Dto, cloudTopology, cloudCostData, consistentScalingHelper,
            cspFromRegion, smaContextToVMs, riBoughtIdToCouponsUsed, regionIdToOsTypeToContexts,
            contextToBusinessAccountIds, contextToOSTypes);
        Assert.assertTrue(smaContextToVMs.size() == 1);
        Assert.assertTrue(riBoughtIdToCouponsUsed.size() == 1);
        Assert.assertTrue(riBoughtIdToCouponsUsed.get(riBoughtId) == 4);
        Assert.assertTrue(regionIdToOsTypeToContexts.size() == 1);
        Assert.assertTrue(contextToBusinessAccountIds.size() == 1);
        Assert.assertTrue(contextToOSTypes.size() == 1);

        Set<SMAVirtualMachine> vms = smaContextToVMs.get(context);
        Assert.assertTrue(vms.size() == 1);
        SMAVirtualMachine vm = vms.stream().findFirst().orElse(null);
        // updateVirtualMachines does: set currentTemplate, naturalTemplate, and Providers
        Assert.assertTrue(vm != null);
        vm.setCurrentTemplate(template);
        vm.setNaturalTemplate(template);
        vm.setProviders(Collections.singletonList(template));

        // process SMAReservedInstances
        Map<SMAContext, Set<SMAReservedInstance>> smaContextToRIs = new HashMap<>();
        Map<Long, SMAReservedInstance> riBoughtOidToRI = new HashMap<>();
        final ReservedInstanceData riData = new ReservedInstanceData(riBought2, riSpec1);

        boolean created = SMAInput.processReservedInstance(riData, cloudTopology,
            computeTierOidToContextToTemplate, regionIdToOsTypeToContexts, riBoughtIdToCouponsUsed,
            cspFromRegion, reservedInstanceKeyIDGenerator, smaContextToRIs, riBoughtOidToRI);
        Assert.assertTrue(smaContextToRIs.size() == 1);
        Assert.assertTrue(riBoughtOidToRI.size() == 1);
        Assert.assertTrue(smaContextToRIs.get(context) != null);
        Assert.assertTrue(smaContextToRIs.get(context).size() == 1);
        Assert.assertTrue(riBoughtIdToCouponsUsed.size() == 1);
        Assert.assertTrue(riBoughtIdToCouponsUsed.get(riBoughtId) == 4);
        SMAReservedInstance ri = smaContextToRIs.get(context).stream().findFirst().orElse(null);
        Assert.assertTrue(ri != null);
        Assert.assertTrue(ri.getCount() == 1);
        // Generate input context
        List<SMAInputContext> inputContexts = SMAInput.generateInputContexts(smaContextToVMs, smaContextToRIs,
            smaContextToTemplates);
        Assert.assertTrue(inputContexts.size() == 1);
        SMAInputContext inputContext = inputContexts.get(0);

        // Call Stable marriage
        SMAOutputContext outputContext = StableMarriagePerContext.execute(inputContext);
        Assert.assertTrue(outputContext != null);
        Assert.assertTrue(outputContext.getContext().equals(context));
        List<SMAMatch> matches = outputContext.getMatches();
        Assert.assertTrue(matches.size() == 1);
        SMAMatch match = matches.get(0);
        SMAVirtualMachine matchVM = match.getVirtualMachine();
        Assert.assertTrue(matchVM.equals(vm));
        Assert.assertTrue(match.getTemplate() != null);
        Assert.assertTrue(match.getTemplate().equals(template));
        Assert.assertTrue("match.getDiscountedCoupons=" + match.getDiscountedCoupons() + " != 4",
            match.getDiscountedCoupons() == 4);
        Assert.assertTrue(match.getReservedInstance() != null);
        Assert.assertTrue(match.getReservedInstance().equals(ri));
    }

    /**
     * Third test: 2 VMs, vm1 and vm2, and one RI, ri, with a count of 1.
     * The VMs are t2.micro and the RI is t2.small, such that, vm1 is coverd by ri and is not
     * eligible to scale; therefore vm2 scales to t2.mciro and is covered by ri with 4 discounted
     * coupons.
     */
    @Test()
    public void testNonScalableVMCoveredByRITwiceTheSize() {

        // create cloudTopology
        final List<TopologyEntityDTO> dtos = Arrays.asList(vm1Dto, vm2Dto, ct1Dto, regionDto,
            zoneDto, accountDto);
        CloudTopology<TopologyEntityDTO> cloudTopology = spy(defaultFactory.newCloudTopology(dtos.stream()));

        when(cloudTopology.getConnectedRegion(anyLong())).thenReturn(regionalOptional);
        when(cloudTopology.getComputeTier(anyLong())).thenReturn(computeTier1Optional);
        when(cloudTopology.getConnectedAvailabilityZone(anyLong())).thenReturn(zoneOptional);
        // doReturn(zoneOptional).when(cloudTopology).getConnectedAvailabilityZone(anyLong());
        when(cloudTopology.getOwner(anyLong())).thenReturn(accountOptional);
        // doReturn(accountOptional).when(cloudTopology).getOwner(anyLong());
        // when(cloudTopology.getBillingFamilyForEntity(anyLong())).thenReturn(gAndMOptional);
        doReturn(gAndMOptional).when(cloudTopology).getBillingFamilyForEntity(anyLong());

        /*
         * create SMA data structures
         */
        // create SMAContext
        SMAContext context = new SMAContext(SMACSP.AWS, OSType.LINUX, regionId,
            billingFamilyId, Tenancy.DEFAULT);

        /*
         * create SMATemplates
         */
        Table<Long, SMAContext, SMATemplate> computeTierOidToContextToTemplate = HashBasedTable.create();
        SMATemplate template1 = new SMATemplate(ct1Id, ct1Name, ctFamily, ct1Coupons, context, ct1Dto);
        template1.setOnDemandCost(accountId, osType, new SMACost(1.0f, 0.0f));
        template1.setDiscountedCost(accountId, osType, new SMACost(0.0f, 0.0f));
        SMATemplate template2 = new SMATemplate(ct2Id, ct2Name, ctFamily, ct2Coupons, context, ct2Dto);
        template1.setOnDemandCost(accountId, osType, new SMACost(2.0f, 0.0f));
        template1.setDiscountedCost(accountId, osType, new SMACost(0.0f, 0.0f));
        Map<SMAContext, Set<SMATemplate>> smaContextToTemplates = new HashMap<>();
        smaContextToTemplates.put(context, new HashSet<>(Arrays.asList(template1, template2)));
        computeTierOidToContextToTemplate.put(ct1Id, context, template1);
        computeTierOidToContextToTemplate.put(ct2Id, context, template2);

        /*
         * Process VMs
         */
        CspFromRegion cspFromRegion = new CspFromRegion();
        Map<SMAContext, Set<SMAVirtualMachine>> smaContextToVMs = new HashMap<>();
        Map<Long, Float> riBoughtIdToCouponsUsed = new HashMap<>();
        // Map from region ID to OSType to context: used to restrict template and RI creation.
        Table<Long, OSType, Set<SMAContext>> regionIdToOsTypeToContexts = HashBasedTable.create();
        Map<SMAContext, Set<Long>> contextToBusinessAccountIds = new HashMap<>();
        Map<SMAContext, Set<OSType>> contextToOSTypes = new HashMap<>();

        SMAInput.processVirtualMachine(vm1Dto, cloudTopology, cloudCostData, consistentScalingHelper,
            cspFromRegion, smaContextToVMs, riBoughtIdToCouponsUsed, regionIdToOsTypeToContexts,
            contextToBusinessAccountIds, contextToOSTypes);

        Assert.assertTrue(smaContextToVMs.size() == 0);
        Assert.assertTrue(riBoughtIdToCouponsUsed.size() == 1);
        Assert.assertTrue(riBoughtIdToCouponsUsed.get(riBoughtId) != null);
        Assert.assertTrue(riBoughtIdToCouponsUsed.get(riBoughtId) == 4);
        Assert.assertTrue(regionIdToOsTypeToContexts.size() == 0);
        Assert.assertTrue(contextToBusinessAccountIds.size() == 0);
        Assert.assertTrue(contextToOSTypes.size() == 0);

        SMAInput.processVirtualMachine(vm2Dto, cloudTopology, cloudCostData, consistentScalingHelper,
            cspFromRegion, smaContextToVMs, riBoughtIdToCouponsUsed, regionIdToOsTypeToContexts,
            contextToBusinessAccountIds, contextToOSTypes);
        Assert.assertTrue(smaContextToVMs.size() == 1);
        Assert.assertTrue(riBoughtIdToCouponsUsed.size() == 1);
        Assert.assertTrue(riBoughtIdToCouponsUsed.get(riBoughtId) == 4);
        Assert.assertTrue(regionIdToOsTypeToContexts.size() == 1);
        Assert.assertTrue(contextToBusinessAccountIds.size() == 1);
        Assert.assertTrue(contextToOSTypes.size() == 1);

        Set<SMAVirtualMachine> vms = smaContextToVMs.get(context);
        Assert.assertTrue(vms.size() == 1);
        SMAVirtualMachine vm = vms.stream().findFirst().orElse(null);
        // updateVirtualMachines does: set currentTemplate, naturalTemplate, and Providers
        Assert.assertTrue(vm != null);
        vm.setCurrentTemplate(template1);
        vm.setNaturalTemplate(template1);
        vm.setProviders(Arrays.asList(template1, template2));

        // process SMAReservedInstances
        Map<SMAContext, Set<SMAReservedInstance>> smaContextToRIs = new HashMap<>();
        Map<Long, SMAReservedInstance> riBoughtOidToRI = new HashMap<>();
        // 1 t2.small with t2.mciro coupons used
        final ReservedInstanceData riData = new ReservedInstanceData(riBought3, riSpec2);

        boolean created = SMAInput.processReservedInstance(riData, cloudTopology,
            computeTierOidToContextToTemplate, regionIdToOsTypeToContexts, riBoughtIdToCouponsUsed,
            cspFromRegion, reservedInstanceKeyIDGenerator, smaContextToRIs, riBoughtOidToRI);
        Assert.assertTrue(smaContextToRIs.size() == 1);
        Assert.assertTrue(riBoughtOidToRI.size() == 1);
        Assert.assertTrue(smaContextToRIs.get(context) != null);
        Assert.assertTrue(smaContextToRIs.get(context).size() == 1);
        Assert.assertTrue(riBoughtIdToCouponsUsed.size() == 1);
        Assert.assertTrue(riBoughtIdToCouponsUsed.get(riBoughtId) == 4);
        SMAReservedInstance ri = smaContextToRIs.get(context).stream().findFirst().orElse(null);
        Assert.assertTrue(ri != null);

        // Generate input context
        List<SMAInputContext> inputContexts = SMAInput.generateInputContexts(smaContextToVMs, smaContextToRIs,
            smaContextToTemplates);
        Assert.assertTrue(inputContexts.size() == 1);
        SMAInputContext inputContext = inputContexts.get(0);

        // Call Stable marriage
        SMAOutputContext outputContext = StableMarriagePerContext.execute(inputContext);
        Assert.assertTrue(outputContext != null);
        Assert.assertTrue(outputContext.getContext().equals(context));
        List<SMAMatch> matches = outputContext.getMatches();
        Assert.assertTrue(matches.size() == 1);
        SMAMatch match = matches.get(0);
        SMAVirtualMachine matchVM = match.getVirtualMachine();
        Assert.assertTrue(matchVM.equals(vm));
        Assert.assertTrue(match.getTemplate() != null);
        Assert.assertTrue(match.getTemplate().equals(template1));
        Assert.assertTrue("match.getDiscountedCoupons=" + match.getDiscountedCoupons() + " != 4",
            match.getDiscountedCoupons() == 4);
        Assert.assertTrue(match.getReservedInstance() != null);
        Assert.assertTrue(match.getReservedInstance().equals(ri));
    }
}
