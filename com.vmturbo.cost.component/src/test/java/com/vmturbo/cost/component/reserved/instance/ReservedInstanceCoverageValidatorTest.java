package com.vmturbo.cost.component.reserved.instance;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import org.junit.Test;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceScopeInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest.EntityRICoverageUpload;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest.EntityRICoverageUpload.Coverage;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.OS;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopologyFactory;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopologyFactory.DefaultTopologyEntityCloudTopologyFactory;
import com.vmturbo.group.api.GroupAndMembers;
import com.vmturbo.group.api.GroupMemberRetriever;
import com.vmturbo.group.api.ImmutableGroupAndMembers;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

public class ReservedInstanceCoverageValidatorTest {

    private final AtomicLong oidProvider = new AtomicLong();

    private final ReservedInstanceBoughtStore mockReservedInstanceBoughtStore =
            mock(ReservedInstanceBoughtStore.class);

    private final ReservedInstanceSpecStore mockReservedInstanceSpecStore =
            mock(ReservedInstanceSpecStore.class);

    private final GroupMemberRetriever mockedGroupMemberRetriever = mock(GroupMemberRetriever.class);

    private final TopologyEntityCloudTopologyFactory cloudTopologyFactory =
            new DefaultTopologyEntityCloudTopologyFactory(mockedGroupMemberRetriever);

    private final TopologyEntityDTO computeTier = TopologyEntityDTO.newBuilder()
            .setOid(oidProvider.incrementAndGet())
            .setDisplayName("computeTier")
            .setEntityType(EntityType.COMPUTE_TIER_VALUE)
            .setEnvironmentType(EnvironmentType.CLOUD)
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                    .setComputeTier(ComputeTierInfo.newBuilder()
                            .setFamily("familyA")
                            .setNumCoupons(1)))
            .build();

    private final TopologyEntityDTO availabilityZone = TopologyEntityDTO.newBuilder()
            .setOid(oidProvider.incrementAndGet())
            .setDisplayName("availability_zone")
            .setEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
            .setEnvironmentType(EnvironmentType.CLOUD)
            .build();

    private final TopologyEntityDTO region = TopologyEntityDTO.newBuilder()
            .setOid(oidProvider.incrementAndGet())
            .setDisplayName("region")
            .setEntityType(EntityType.REGION_VALUE)
            .setEnvironmentType(EnvironmentType.CLOUD)
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                    .setConnectedEntityId(availabilityZone.getOid())
                    .setConnectedEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
                    .setConnectionType(ConnectionType.OWNS_CONNECTION))
            .build();

    private final TopologyEntityDTO virtualMachine = TopologyEntityDTO.newBuilder()
            .setOid(oidProvider.incrementAndGet())
            .setDisplayName("virtual_machine")
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .setEnvironmentType(EnvironmentType.CLOUD)
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                    .setVirtualMachine(VirtualMachineInfo.newBuilder()
                            .setGuestOsInfo(OS.newBuilder()
                                    .setGuestOsType(OSType.LINUX))))
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                    .setProviderEntityType(EntityType.COMPUTE_TIER_VALUE)
                    .setProviderId(computeTier.getOid()))
            .setEntityState(EntityState.POWERED_ON)
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                    .setConnectedEntityId(availabilityZone.getOid())
                    .setConnectedEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
                    .setConnectionType(ConnectionType.AGGREGATED_BY_CONNECTION))
            .build();


    private final long businessAccountOid = oidProvider.incrementAndGet();
    private final TopologyEntityDTO businessAccount = TopologyEntityDTO.newBuilder()
            .setOid(businessAccountOid)
            .setDisplayName("bussiness_account")
            .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
            .setEnvironmentType(EnvironmentType.CLOUD)
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                    .setConnectedEntityId(virtualMachine.getOid())
                    .setConnectedEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                    .setConnectionType(ConnectionType.OWNS_CONNECTION))
            .build();

    private final ReservedInstanceSpec reservedInstanceSpec = ReservedInstanceSpec.newBuilder()
            .setId(oidProvider.incrementAndGet())
            .setReservedInstanceSpecInfo(ReservedInstanceSpecInfo.newBuilder()
                    .setTenancy(Tenancy.DEFAULT)
                    .setOs(OSType.LINUX)
                    .setTierId(computeTier.getOid())
                    .setRegionId(region.getOid())
                    .setPlatformFlexible(false)
                    .setSizeFlexible(false)
                    .setType(ReservedInstanceType.newBuilder()
                        .setTermYears(1)))
            .build();

    private final ReservedInstanceBought reservedInstanceBought = ReservedInstanceBought.newBuilder()
            .setId(oidProvider.incrementAndGet())
            .setReservedInstanceBoughtInfo(ReservedInstanceBoughtInfo.newBuilder()
                    .setBusinessAccountId(businessAccount.getOid())
                    .setProbeReservedInstanceId("probe_reserved_instance_id")
                    .setStartTime(Instant.now().toEpochMilli())
                    .setAvailabilityZoneId(availabilityZone.getOid())
                    .setReservedInstanceSpec(reservedInstanceSpec.getId())
                    .setReservedInstanceScopeInfo(ReservedInstanceScopeInfo.newBuilder()
                            .setShared(false)
                            .addApplicableBusinessAccountId(businessAccount.getOid())))
            .build();

    private final ReservedInstanceCoverageValidatorFactory validatorFactory =
            new ReservedInstanceCoverageValidatorFactory(
                    mockReservedInstanceBoughtStore,
                    mockReservedInstanceSpecStore);

    private CloudTopology<TopologyEntityDTO> generateCloudTopology(
            TopologyEntityDTO... entityDtos) {
        return cloudTopologyFactory.newCloudTopology(Arrays.stream(entityDtos));
    }

    /**
     * Test a valid match with baseline config (strict match)
     */
    @Test
    public void testIsValidCoverage_validCoverage() {

        // setup mocks
        when(mockReservedInstanceBoughtStore.getReservedInstanceBoughtByFilter(any()))
                .thenReturn(Lists.newArrayList(reservedInstanceBought));
        when(mockReservedInstanceSpecStore.getReservedInstanceSpecByIds(any()))
                .thenReturn(Lists.newArrayList(reservedInstanceSpec));

        // generate CloudTopology
        final CloudTopology<TopologyEntityDTO> cloudTopology = generateCloudTopology(
                computeTier, region, availabilityZone, virtualMachine, businessAccount);

        // setup validator
        ReservedInstanceCoverageValidator validator = validatorFactory.newValidator(cloudTopology);

        final boolean isValidCoverage = validator.isCoverageValid(
                virtualMachine, reservedInstanceBought);

        assertTrue(isValidCoverage);
    }

    /**
     * Test a valid match with billing family match
     */
    @Test
    public void testIsValidCoverage_validCoverage_billingFamilyScope() {

        //setup input
        final long riAccountOid = oidProvider.incrementAndGet();
        final TopologyEntityDTO riBusinessAccount = TopologyEntityDTO.newBuilder(businessAccount)
                .setOid(riAccountOid)
                .clearConnectedEntityList()
                .build();
        final long masterAccountOid = oidProvider.incrementAndGet();
        final TopologyEntityDTO masterAccount = TopologyEntityDTO.newBuilder()
                .setOid(masterAccountOid)
                .setDisplayName("master_account")
                .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
                .setEnvironmentType(EnvironmentType.CLOUD)
                .addConnectedEntityList(ConnectedEntity.newBuilder()
                        .setConnectionType(ConnectionType.OWNS_CONNECTION)
                        .setConnectedEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
                        .setConnectedEntityId(businessAccount.getOid()))
                .addConnectedEntityList(ConnectedEntity.newBuilder()
                        .setConnectionType(ConnectionType.OWNS_CONNECTION)
                        .setConnectedEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
                        .setConnectedEntityId(riBusinessAccount.getOid()))
                .build();

        GroupAndMembers billingFamily = groupAndMembers(Arrays.asList(masterAccountOid, riAccountOid, businessAccountOid));
        final List<GroupAndMembers> collectionGroups = new ArrayList<>();
        collectionGroups.add(billingFamily);
        final ReservedInstanceBought.Builder reservedInstanceBuilder =
                ReservedInstanceBought.newBuilder(reservedInstanceBought);
        reservedInstanceBuilder.getReservedInstanceBoughtInfoBuilder()
                .setBusinessAccountId(riBusinessAccount.getOid())
                .setReservedInstanceScopeInfo(ReservedInstanceScopeInfo.newBuilder()
                        .setShared(true));
        final ReservedInstanceBought reservedInstanceBought = reservedInstanceBuilder.build();


        // setup mocks
        when(mockReservedInstanceBoughtStore.getReservedInstanceBoughtByFilter(any()))
                .thenReturn(Lists.newArrayList(reservedInstanceBought));
        when(mockReservedInstanceSpecStore.getReservedInstanceSpecByIds(any()))
                .thenReturn(Lists.newArrayList(reservedInstanceSpec));

        when(mockedGroupMemberRetriever.getGroupsWithMembers(GetGroupsRequest.newBuilder()
                .setGroupFilter(GroupFilter.newBuilder()
                        .setGroupType(GroupType.BILLING_FAMILY)
                        .build())
                .build())).thenReturn(collectionGroups);
        // generate CloudTopology
        final CloudTopology<TopologyEntityDTO> cloudTopology = generateCloudTopology(
                computeTier, region, availabilityZone, virtualMachine, businessAccount,
                riBusinessAccount, masterAccount);

        // setup validator
        ReservedInstanceCoverageValidator validator = validatorFactory.newValidator(cloudTopology);

        final boolean isValidCoverage = validator.isCoverageValid(
                virtualMachine, reservedInstanceBought);

        assertTrue(isValidCoverage);
    }

    /**
     * Test a valid match with a platform-flexible RI
     */
    @Test
    public void testIsValidCoverage_validCoverage_platformFlexible() {

        // setup platform-flexible RI spec
        final ReservedInstanceSpec.Builder riSpecBuilder =
                ReservedInstanceSpec.newBuilder(reservedInstanceSpec);
        riSpecBuilder.getReservedInstanceSpecInfoBuilder()
                .setOs(OSType.RHEL)
                .setPlatformFlexible(true);
        final ReservedInstanceSpec reservedInstanceSpec = riSpecBuilder.build();

        // setup mocks
        when(mockReservedInstanceBoughtStore.getReservedInstanceBoughtByFilter(any()))
                .thenReturn(Lists.newArrayList(reservedInstanceBought));
        when(mockReservedInstanceSpecStore.getReservedInstanceSpecByIds(any()))
                .thenReturn(Lists.newArrayList(reservedInstanceSpec));

        // generate CloudTopology
        final CloudTopology<TopologyEntityDTO> cloudTopology = generateCloudTopology(
                computeTier, region, availabilityZone, virtualMachine, businessAccount);

        // setup validator
        ReservedInstanceCoverageValidator validator = validatorFactory.newValidator(cloudTopology);

        final boolean isValidCoverage = validator.isCoverageValid(
                virtualMachine, reservedInstanceBought);

        assertTrue(isValidCoverage);
    }

    /**
     * Test a valid match with a size-flexible RI
     */
    @Test
    public void testIsValidCoverage_validCoverage_sizeFlexible() {

        // setup size-flexible RI spec
        final TopologyEntityDTO riComputeTier = TopologyEntityDTO.newBuilder(computeTier)
                .setOid(oidProvider.incrementAndGet())
                .build();
        final ReservedInstanceSpec.Builder riSpecBuilder =
                ReservedInstanceSpec.newBuilder(reservedInstanceSpec);
        riSpecBuilder.getReservedInstanceSpecInfoBuilder()
                .setSizeFlexible(true)
                .setTierId(riComputeTier.getOid());
        final ReservedInstanceSpec reservedInstanceSpec = riSpecBuilder.build();

        // setup mocks
        when(mockReservedInstanceBoughtStore.getReservedInstanceBoughtByFilter(any()))
                .thenReturn(Lists.newArrayList(reservedInstanceBought));
        when(mockReservedInstanceSpecStore.getReservedInstanceSpecByIds(any()))
                .thenReturn(Lists.newArrayList(reservedInstanceSpec));

        // generate CloudTopology
        final CloudTopology<TopologyEntityDTO> cloudTopology = generateCloudTopology(
                computeTier, riComputeTier, region, availabilityZone,
                virtualMachine, businessAccount);

        // setup validator
        ReservedInstanceCoverageValidator validator = validatorFactory.newValidator(cloudTopology);

        final boolean isValidCoverage = validator.isCoverageValid(
                virtualMachine, reservedInstanceBought);

        assertTrue(isValidCoverage);
    }

    /**
     * Test a valid match with a regional RI
     */
    @Test
    public void testIsValidCoverage_validCoverage_regional() {

        // setup regional RI
        final ReservedInstanceBought.Builder riBoughtBuilder =
                ReservedInstanceBought.newBuilder(reservedInstanceBought);
        riBoughtBuilder.getReservedInstanceBoughtInfoBuilder()
                .clearAvailabilityZoneId();
        final ReservedInstanceBought reservedInstanceBought = riBoughtBuilder.build();

        // setup mocks
        when(mockReservedInstanceBoughtStore.getReservedInstanceBoughtByFilter(any()))
                .thenReturn(Lists.newArrayList(reservedInstanceBought));
        when(mockReservedInstanceSpecStore.getReservedInstanceSpecByIds(any()))
                .thenReturn(Lists.newArrayList(reservedInstanceSpec));

        // generate CloudTopology
        final CloudTopology<TopologyEntityDTO> cloudTopology = generateCloudTopology(
                computeTier, region, availabilityZone, virtualMachine, businessAccount);

        // setup validator
        ReservedInstanceCoverageValidator validator = validatorFactory.newValidator(cloudTopology);

        final boolean isValidCoverage = validator.isCoverageValid(
                virtualMachine, reservedInstanceBought);

        assertTrue(isValidCoverage);
    }

    /**
     * Test a valid match with a regional RI
     */
    @Test
    public void testIsValidCoverage_invalidCoverage_expired() {

        // setup expired RI
        final ReservedInstanceBought.Builder riBoughtBuilder =
                ReservedInstanceBought.newBuilder(reservedInstanceBought);
        //
        riBoughtBuilder.getReservedInstanceBoughtInfoBuilder()
                .setStartTime(Instant.now()
                        .minus(1500, ChronoUnit.DAYS)
                        .toEpochMilli());
        final ReservedInstanceBought reservedInstanceBought = riBoughtBuilder.build();

        // setup mocks
        when(mockReservedInstanceBoughtStore.getReservedInstanceBoughtByFilter(any()))
                .thenReturn(Lists.newArrayList(reservedInstanceBought));
        when(mockReservedInstanceSpecStore.getReservedInstanceSpecByIds(any()))
                .thenReturn(Lists.newArrayList(reservedInstanceSpec));

        // generate CloudTopology
        final CloudTopology<TopologyEntityDTO> cloudTopology = generateCloudTopology(
                computeTier, region, availabilityZone, virtualMachine, businessAccount);

        // setup validator
        ReservedInstanceCoverageValidator validator = validatorFactory.newValidator(cloudTopology);

        final boolean isValidCoverage = validator.isCoverageValid(
                virtualMachine, reservedInstanceBought);

        assertFalse(isValidCoverage);
    }

    @Test
    public void testValidateCoverageUploads_validCoverage() {

        // setup input
        final EntityRICoverageUpload entityRICoverageUpload = EntityRICoverageUpload.newBuilder()
                .setEntityId(virtualMachine.getOid())
                .setTotalCouponsRequired(4)
                .addCoverage(Coverage.newBuilder()
                        .setCoveredCoupons(2)
                        .setReservedInstanceId(reservedInstanceBought.getId()))
                .build();

        // setup mocks
        when(mockReservedInstanceBoughtStore.getReservedInstanceBoughtByFilter(any()))
                .thenReturn(Lists.newArrayList(reservedInstanceBought));
        when(mockReservedInstanceSpecStore.getReservedInstanceSpecByIds(any()))
                .thenReturn(Lists.newArrayList(reservedInstanceSpec));

        // generate CloudTopology
        final CloudTopology<TopologyEntityDTO> cloudTopology = generateCloudTopology(
                computeTier, region, availabilityZone, virtualMachine, businessAccount);

        // setup validator
        ReservedInstanceCoverageValidator validator = validatorFactory.newValidator(cloudTopology);

        // invoke test method
        final List<EntityRICoverageUpload> validEntityRICoverageUploads =
                validator.validateCoverageUploads(Collections.singleton(entityRICoverageUpload));

        assertThat(validEntityRICoverageUploads.size(), equalTo(1));
        final EntityRICoverageUpload validEntityRICoverageUpload = validEntityRICoverageUploads.get(0);
        assertThat(validEntityRICoverageUpload.getTotalCouponsRequired(), equalTo(4.0));
        assertThat(validEntityRICoverageUpload.getCoverageCount(), equalTo(1));
        final Coverage validCoverage = validEntityRICoverageUpload.getCoverage(0);
        assertThat(validCoverage.getCoveredCoupons(), equalTo(2.0));
        assertThat(validCoverage.getReservedInstanceId(), equalTo(reservedInstanceBought.getId()));

    }


    @Test
    public void testValidateCoverageUploads_poweredOff() {

        // setup powered off VM
        TopologyEntityDTO virtualMachine = TopologyEntityDTO.newBuilder(this.virtualMachine)
                .setEntityState(EntityState.POWERED_OFF)
                .build();

        // setup input
        final EntityRICoverageUpload entityRICoverageUpload = EntityRICoverageUpload.newBuilder()
                .setEntityId(virtualMachine.getOid())
                .setTotalCouponsRequired(4)
                .addCoverage(Coverage.newBuilder()
                        .setCoveredCoupons(2)
                        .setReservedInstanceId(reservedInstanceBought.getId()))
                .build();

        // setup mocks
        when(mockReservedInstanceBoughtStore.getReservedInstanceBoughtByFilter(any()))
                .thenReturn(Lists.newArrayList(reservedInstanceBought));
        when(mockReservedInstanceSpecStore.getReservedInstanceSpecByIds(any()))
                .thenReturn(Lists.newArrayList(reservedInstanceSpec));

        // generate CloudTopology
        final CloudTopology<TopologyEntityDTO> cloudTopology = generateCloudTopology(
                computeTier, region, availabilityZone, virtualMachine, businessAccount);

        // setup validator
        ReservedInstanceCoverageValidator validator = validatorFactory.newValidator(cloudTopology);

        // invoke test method
        final List<EntityRICoverageUpload> validEntityRICoverageUploads =
                validator.validateCoverageUploads(Collections.singleton(entityRICoverageUpload));

        assertThat(validEntityRICoverageUploads.size(), equalTo(1));
        final EntityRICoverageUpload validEntityRICoverageUpload = validEntityRICoverageUploads.get(0);
        assertThat(validEntityRICoverageUpload.getTotalCouponsRequired(), equalTo(0.0));
        assertThat(validEntityRICoverageUpload.getCoverageCount(), equalTo(0));
    }

    private GroupAndMembers groupAndMembers(List<Long> entityOid) {
        final long groupId = 1L;
        final Set<Long> members = ImmutableSet.copyOf(entityOid);
        final GroupDefinition groupDefinition =
                GroupDefinition.newBuilder().setType(GroupType.BILLING_FAMILY).build();
        final Grouping group = Grouping.newBuilder()
                .setDefinition(groupDefinition)
                .setId(groupId)
                .addExpectedTypes(
                        MemberType.newBuilder().setEntity(EntityType.BUSINESS_ACCOUNT_VALUE))
                .build();
        return ImmutableGroupAndMembers.builder()
                .group(group)
                .members(members)
                .entities(members)
                .build();
    }
}
