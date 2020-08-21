package com.vmturbo.topology.processor.topology;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers.StaticMembersByType;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.PlanScope;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.PlanScopeEntry;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PlanTopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Origin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.PlanScenarioOrigin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.commons.analysis.InvertedIndex;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.group.ResolvedGroup;
import com.vmturbo.topology.processor.topology.PlanTopologyScopeEditor.FastLookupQueue;

/**
 * Unit tests for {@link PlanTopologyScopeEditor}.
 */
public class PlanTopologyScopeEditorTest {

    private static final TopologyDTO.CommodityType CPU = TopologyDTO.CommodityType.newBuilder().setType(CommonDTO.CommodityDTO.CommodityType.CPU_VALUE).build();
    private static final TopologyDTO.CommodityType DATASTORE = TopologyDTO.CommodityType.newBuilder().setType(CommodityType.DATASTORE_VALUE).build();
    private static final TopologyDTO.CommodityType VCPU = TopologyDTO.CommodityType.newBuilder().setType(CommonDTO.CommodityDTO.CommodityType.VCPU_VALUE).build();
    private static final TopologyDTO.CommodityType POWER = TopologyDTO.CommodityType.newBuilder().setType(CommonDTO.CommodityDTO.CommodityType.POWER_VALUE).build();
    private static final TopologyDTO.CommodityType EXTENT1 = TopologyDTO.CommodityType.newBuilder().setType(CommonDTO.CommodityDTO.CommodityType.EXTENT_VALUE)
            .setKey("DA1").build();
    private static final TopologyDTO.CommodityType DC1 = TopologyDTO.CommodityType.newBuilder().setType(CommonDTO.CommodityDTO.CommodityType.DATACENTER_VALUE)
            .setKey("DC1").build();
    private static final TopologyDTO.CommodityType DC2 = TopologyDTO.CommodityType.newBuilder().setType(CommonDTO.CommodityDTO.CommodityType.DATACENTER_VALUE)
            .setKey("DC2").build();
    private static final TopologyDTO.CommodityType SC1 = TopologyDTO.CommodityType.newBuilder().setType(CommonDTO.CommodityDTO.CommodityType.CLUSTER_VALUE)
            .setKey("SC1").build();
    private static final TopologyDTO.CommodityType SC2 = TopologyDTO.CommodityType.newBuilder().setType(CommonDTO.CommodityDTO.CommodityType.CLUSTER_VALUE)
            .setKey("SC2").build();
    private static final TopologyDTO.CommodityType APP1 = TopologyDTO.CommodityType.newBuilder().setType(CommonDTO.CommodityDTO.CommodityType.APPLICATION_VALUE)
            .setKey("APP1").build();
    private static final TopologyDTO.CommodityType APP2 = TopologyDTO.CommodityType.newBuilder().setType(CommonDTO.CommodityDTO.CommodityType.APPLICATION_VALUE)
            .setKey("APP2").build();
    private static final TopologyDTO.CommodityType BAPP1 = TopologyDTO.CommodityType.newBuilder().setType(CommonDTO.CommodityDTO.CommodityType.APPLICATION_VALUE)
            .setKey("BAPP1").build();
    private static final TopologyDTO.CommodityType BAPP2 = TopologyDTO.CommodityType.newBuilder().setType(CommonDTO.CommodityDTO.CommodityType.APPLICATION_VALUE)
            .setKey("BAPP2").build();
    private static final TopologyDTO.CommodityType DSPM1 = TopologyDTO.CommodityType.newBuilder().setType(CommonDTO.CommodityDTO.CommodityType.DSPM_ACCESS_VALUE)
            .setKey("DSPM1").build();
    private static final TopologyDTO.CommodityType DS1 = TopologyDTO.CommodityType.newBuilder().setType(CommonDTO.CommodityDTO.CommodityType.DATASTORE_VALUE)
            .setKey("DS1").build();
    private static final TopologyDTO.CommodityType DSPM2 = TopologyDTO.CommodityType.newBuilder().setType(CommonDTO.CommodityDTO.CommodityType.DSPM_ACCESS_VALUE)
            .setKey("DSPM2").build();
    private static final TopologyDTO.CommodityType DS2 = TopologyDTO.CommodityType.newBuilder().setType(CommonDTO.CommodityDTO.CommodityType.DATASTORE_VALUE)
            .setKey("DS2").build();
    private static final TopologyDTO.CommodityType ST_AMT = TopologyDTO.CommodityType.newBuilder().setType(CommonDTO.CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE).build();

    private static final List<TopologyDTO.CommodityType> basketSoldByPMinDC1 = Lists.newArrayList(DC1, CPU, DS1);
    private static final List<TopologyDTO.CommodityType> basketSoldByPMinDC2 = Lists.newArrayList(DC2, CPU, DS1, DS2);
    private static final List<TopologyDTO.CommodityType> basketSoldByDC1 = Lists.newArrayList(DC1, POWER);
    private static final List<TopologyDTO.CommodityType> basketSoldByDC2 = Lists.newArrayList(DC2, POWER);
    private static final List<TopologyDTO.CommodityType> basketSoldByVM1 = Lists.newArrayList(VCPU, APP1);
    private static final List<TopologyDTO.CommodityType> basketSoldByVM2 = Lists.newArrayList(VCPU, APP2);
    private static final List<TopologyDTO.CommodityType> basketSoldByDA = Lists.newArrayList(EXTENT1, ST_AMT);
    private static final List<TopologyDTO.CommodityType> basketSoldByDS1 = Lists.newArrayList(ST_AMT, SC1, DSPM1, DSPM2);
    private static final List<TopologyDTO.CommodityType> basketSoldByDS2 = Lists.newArrayList(ST_AMT, SC2, DSPM2);
    private static final List<TopologyDTO.CommodityType> basketSoldByAS1 = Lists.newArrayList(BAPP1);
    private static final List<TopologyDTO.CommodityType> basketSoldByAS2 = Lists.newArrayList(BAPP2);

    private static final Map<Long, List<TopologyDTO.CommodityType>> commBoughtByVMinDC1PM1DS1 = new HashMap<Long, List<TopologyDTO.CommodityType>>() {{
        put(20001L, basketSoldByPMinDC1);
        put(40001L, basketSoldByDS1);
    }};

    private static final Map<Long, List<TopologyDTO.CommodityType>> commBoughtByVMinDC1PM2DS1 = new HashMap<Long, List<TopologyDTO.CommodityType>>() {{
        put(20002L, basketSoldByPMinDC1);
        put(40001L, basketSoldByDS1);
    }};

    private static final Map<Long, List<TopologyDTO.CommodityType>> commBoughtByVMinDC2PMDS2 = new HashMap<Long, List<TopologyDTO.CommodityType>>() {{
        put(20003L, basketSoldByPMinDC2);
        put(40002L, basketSoldByDS2);
    }};

    private static final Map<Long, List<TopologyDTO.CommodityType>> commBoughtByPMinDC1  = new HashMap<Long, List<TopologyDTO.CommodityType>>() {{
        put(10001L, basketSoldByDC1);
    }};

    private static final Map<Long, List<TopologyDTO.CommodityType>> commBoughtByPMinDC2  = new HashMap<Long, List<TopologyDTO.CommodityType>>() {{
        put(10001L, basketSoldByDC2);
    }};

    private static final Map<Long, List<TopologyDTO.CommodityType>> commBoughtByApp1 = new HashMap<Long, List<TopologyDTO.CommodityType>>() {{
        put(30001L, basketSoldByVM1);
    }};

    private static final Map<Long, List<TopologyDTO.CommodityType>> commBoughtByApp2 = new HashMap<Long, List<TopologyDTO.CommodityType>>() {{
        put(30003L, basketSoldByVM2);
    }};

    private static final Map<Long, List<TopologyDTO.CommodityType>> commBoughtByDSinDA1 = new HashMap<Long, List<TopologyDTO.CommodityType>>() {{
        put(50001L, basketSoldByDA);
    }};

    private static final Map<Long, List<TopologyDTO.CommodityType>> commBoughtByBA = new HashMap<Long, List<TopologyDTO.CommodityType>>() {{
        put(70001L, basketSoldByAS1);
        put(70002L, basketSoldByAS2);
    }};

    private static final int HYPERVISOR_TARGET = 0;
    private static final int CLOUD_TARGET_1 = 1;
    private static final int CLOUD_TARGET_2 = 2;
    private final TopologyEntity.Builder da1 = createHypervisorTopologyEntity(50001L, "da1", EntityType.DISK_ARRAY, new HashMap<>(), basketSoldByDA);
    private final TopologyEntity.Builder st1 = createHypervisorTopologyEntity(40001L, "st1", EntityType.STORAGE, commBoughtByDSinDA1, basketSoldByDS1);
    private final TopologyEntity.Builder st2 = createHypervisorTopologyEntity(40002L, "st2", EntityType.STORAGE, commBoughtByDSinDA1, basketSoldByDS2);
    private final TopologyEntity.Builder dc1 = createHypervisorTopologyEntity(10001L, "dc1", EntityType.DATACENTER, new HashMap<>(), basketSoldByDC1);
    private final TopologyEntity.Builder dc2 = createHypervisorTopologyEntity(10002L, "dc2", EntityType.DATACENTER, new HashMap<>(), basketSoldByDC2);
    private final TopologyEntity.Builder pm1InDc1 = createHypervisorHost(20001L, "pm1InDc1", EntityType.PHYSICAL_MACHINE, commBoughtByPMinDC1, basketSoldByPMinDC1, Arrays.asList(40001L));
    private final TopologyEntity.Builder pm2InDc1 = createHypervisorHost(20002L, "pm2InDc1", EntityType.PHYSICAL_MACHINE, commBoughtByPMinDC1, basketSoldByPMinDC1, Arrays.asList(40001L));
    private final TopologyEntity.Builder pmInDc2 = createHypervisorTopologyEntity(20003L, "pmInDc2", EntityType.PHYSICAL_MACHINE, commBoughtByPMinDC2, basketSoldByPMinDC2);
    private final TopologyEntity.Builder virtualVolume = TopologyEntityUtils.connectedTopologyEntity(25001L, HYPERVISOR_TARGET, 0, "virtualVolume", EntityType.VIRTUAL_VOLUME, st1.getOid());
    private final TopologyEntity.Builder vm1InDc1 = createHypervisorTopologyEntity(30001L, "vm1InDc1", EntityType.VIRTUAL_MACHINE, commBoughtByVMinDC1PM1DS1, basketSoldByVM1, virtualVolume.getOid());
    private final TopologyEntity.Builder vm2InDc1 = createHypervisorTopologyEntity(30002L, "vm2InDc1", EntityType.VIRTUAL_MACHINE, commBoughtByVMinDC1PM2DS1, basketSoldByVM1);
    private final TopologyEntity.Builder vmInDc2 = createHypervisorTopologyEntity(30003L, "vmInDc2", EntityType.VIRTUAL_MACHINE, commBoughtByVMinDC2PMDS2, basketSoldByVM2);
    private final TopologyEntity.Builder appc1 = createHypervisorTopologyEntity(60001L, "appc1", EntityType.APPLICATION_COMPONENT, commBoughtByApp1, new ArrayList<>());
    private final TopologyEntity.Builder as1 = createHypervisorTopologyEntity(70001L, "as1", EntityType.APPLICATION_SERVER, commBoughtByApp1, basketSoldByAS1);
    private final TopologyEntity.Builder as2 = createHypervisorTopologyEntity(70002L, "as2", EntityType.APPLICATION_SERVER, commBoughtByApp2, basketSoldByAS2);
    private final TopologyEntity.Builder bapp1 = createHypervisorTopologyEntity(80001L, "bapp1", EntityType.BUSINESS_APPLICATION, commBoughtByBA, new ArrayList<>());

    private static final long VIRTUAL_VOLUME_IN_OHIO_ID = 6001L;
    private static final long VIRTUAL_VOLUME_IN_LONDON_ID = 6002L;
    private static final long VIRTUAL_VOLUME_IN_HONG_KONG_ID = 6003L;
    private static final long VIRTUAL_VOLUME_IN_CENTRAL_US_ID = 6004L;
    private static final long VIRTUAL_VOLUME_IN_CANADA_ID = 6005L;
    private static final long UNATTACHED_VV_IN_CENTRAL_US_ID = 6006L;
    private static final long UNATTACHED_VV_IN_LONDON_ID = 6007L;
    private static final long VIRTUAL_VOLUME_2_IN_CANADA_ID = 6008L;

    private final TopologyEntity.Builder computeTier = createCloudConnectedTopologyEntity(
            CLOUD_TARGET_1, 3001L, "Compute tier", EntityType.COMPUTE_TIER);
    private final TopologyEntity.Builder storageTier = createCloudConnectedTopologyEntity(
            CLOUD_TARGET_1, 7001L, "Storage tier", EntityType.STORAGE_TIER);

    private final TopologyEntity.Builder vm1InLondon = createCloudVm(
            CLOUD_TARGET_1, 4001L, "VM1 in London", VIRTUAL_VOLUME_IN_LONDON_ID);
    private final TopologyEntity.Builder vm2InLondon = createCloudVm(
            CLOUD_TARGET_1, 4002L, "VM2 in London");
    private final TopologyEntity.Builder vmInOhio = createCloudVm(
            CLOUD_TARGET_1, 4003L, "VM in Ohio", VIRTUAL_VOLUME_IN_OHIO_ID);
    private final TopologyEntity.Builder vmInHongKong = createCloudVm(
            CLOUD_TARGET_1, 4004L, "VM in Hong Kong", VIRTUAL_VOLUME_IN_HONG_KONG_ID);

    private final TopologyEntity.Builder appAws =
            TopologyEntityUtils.topologyEntity(900000L, CLOUD_TARGET_1, 0,
                                               EntityType.APPLICATION_COMPONENT, vm1InLondon.getOid());

    private final TopologyEntity.Builder dbLondon = createCloudConnectedTopologyEntity(
            CLOUD_TARGET_1, 8001L, "DB in London", EntityType.DATABASE);
    private final TopologyEntity.Builder dbsLondon = createCloudConnectedTopologyEntity(
            CLOUD_TARGET_1, 9001L, "DBS in London", EntityType.DATABASE_SERVER);
    private final TopologyEntity.Builder dbsHongKong = createCloudConnectedTopologyEntity(
            CLOUD_TARGET_1, 9002L, "DBS in Hong Kong", EntityType.DATABASE_SERVER);

    private final TopologyEntity.Builder virtualVolumeInLondon = createCloudVolume(
            CLOUD_TARGET_1, VIRTUAL_VOLUME_IN_LONDON_ID, "Virtual Volume in London",
            storageTier.getOid());
    private final TopologyEntity.Builder virtualVolumeInOhio = createCloudVolume(
            CLOUD_TARGET_1, VIRTUAL_VOLUME_IN_OHIO_ID, "Virtual Volume in Ohio",
            storageTier.getOid());
    private final TopologyEntity.Builder virtualVolumeInHongKong = createCloudVolume(
            CLOUD_TARGET_1, VIRTUAL_VOLUME_IN_HONG_KONG_ID, "Virtual Volume in Hong Kong",
            storageTier.getOid());
    private final TopologyEntity.Builder unattachedVirtualVolumeInLondon = createCloudVolume(
            CLOUD_TARGET_1, UNATTACHED_VV_IN_LONDON_ID, "Unattached Virtual Volume in London",
            storageTier.getOid());

    private final TopologyEntity.Builder az1London = createCloudTopologyAvailabilityZone(
            CLOUD_TARGET_1, 1001L, "AZ1 London",
            dbsLondon, vm1InLondon, virtualVolumeInLondon, unattachedVirtualVolumeInLondon);
    private final TopologyEntity.Builder az2London = createCloudTopologyAvailabilityZone(
            CLOUD_TARGET_1, 1002L, "AZ2 London",
            dbLondon, vm2InLondon);
    private final TopologyEntity.Builder azOhio = createCloudTopologyAvailabilityZone(
            CLOUD_TARGET_1, 1003L, "AZ Ohio",
            vmInOhio, virtualVolumeInOhio);
    private final TopologyEntity.Builder az1HongKong = createCloudTopologyAvailabilityZone(
            CLOUD_TARGET_1, 1004L, "AZ1 Hong Kong",
            vmInHongKong, virtualVolumeInHongKong);
    private final TopologyEntity.Builder az2HongKong = createCloudTopologyAvailabilityZone(
            CLOUD_TARGET_1, 1005L, "AZ2 Hong Kong",
            dbsHongKong);

    private final TopologyEntity.Builder regionLondon = createRegion(
            CLOUD_TARGET_1, 2001L, "London",
            ImmutableList.of(az1London, az2London),
            ImmutableList.of(computeTier, storageTier));
    private final TopologyEntity.Builder regionOhio = createRegion(
            CLOUD_TARGET_1, 2002L, "Ohio",
            Collections.singleton(azOhio),
            ImmutableList.of(computeTier, storageTier));
    private final TopologyEntity.Builder regionHongKong = createRegion(
            CLOUD_TARGET_1, 2003L, "Hong Kong",
            ImmutableList.of(az1HongKong, az2HongKong),
            Collections.singleton(storageTier));

    private final TopologyEntity.Builder computeTier2 = createCloudConnectedTopologyEntity(
            CLOUD_TARGET_2, 3002L, "Compute tier 2", EntityType.COMPUTE_TIER);
    private final TopologyEntity.Builder storageTier2 = createCloudConnectedTopologyEntity(
            CLOUD_TARGET_2, 7002L, "Storage tier 2", EntityType.STORAGE_TIER);

    private final TopologyEntity.Builder vmInCentralUs = createCloudVm(
            CLOUD_TARGET_2, 4005L, "VM in Central US", VIRTUAL_VOLUME_IN_CENTRAL_US_ID);
    private final TopologyEntity.Builder vmInCanada = createCloudVm(
            CLOUD_TARGET_2, 4006L, "VM in Canada", VIRTUAL_VOLUME_IN_CANADA_ID,
            VIRTUAL_VOLUME_2_IN_CANADA_ID);
    private final TopologyEntity.Builder dbCentralUs = createCloudConnectedTopologyEntity(
            CLOUD_TARGET_2, 8002L, "DB in Central US", EntityType.DATABASE);
    private final TopologyEntity.Builder dbsCentralUs = createCloudConnectedTopologyEntity(
            CLOUD_TARGET_2, 9003L, "DBS in Central US", EntityType.DATABASE_SERVER);

    private final TopologyEntity.Builder unattachedVirtualVolumeInCentralUs = createCloudVolume(
            CLOUD_TARGET_2, UNATTACHED_VV_IN_CENTRAL_US_ID,
            "Unattached Virtual Volume in Central US", storageTier2.getOid());
    private final TopologyEntity.Builder virtualVolumeInCentralUs = createCloudVolume(
            CLOUD_TARGET_2, VIRTUAL_VOLUME_IN_CENTRAL_US_ID, "Virtual Volume in Central US",
            storageTier2.getOid());
    private final TopologyEntity.Builder virtualVolumeInCanada = createCloudVolume(CLOUD_TARGET_2,
            VIRTUAL_VOLUME_IN_CANADA_ID, "Virtual Volume in Canada", storageTier2.getOid());
    private final TopologyEntity.Builder virtualVolume2InCanada = createCloudVolume(CLOUD_TARGET_2,
            VIRTUAL_VOLUME_2_IN_CANADA_ID, "Virtual Volume 2 in Canada", storageTier2.getOid());

    private final TopologyEntity.Builder businessAcc2 = createOwner(
            CLOUD_TARGET_1, 5002L, "Business account 2", EntityType.BUSINESS_ACCOUNT,
            vmInOhio, virtualVolumeInOhio);
    private final TopologyEntity.Builder businessAcc3 = createOwner(
            CLOUD_TARGET_1, 5003L, "Business account 3", EntityType.BUSINESS_ACCOUNT,
            vmInHongKong, virtualVolumeInHongKong);
    private final TopologyEntity.Builder businessAcc1 = createOwner(
            CLOUD_TARGET_1, 5001L, "Business account 1", EntityType.BUSINESS_ACCOUNT,
            businessAcc3, vm1InLondon, virtualVolumeInLondon, appAws);

    private final TopologyEntity.Builder appAzure =
            TopologyEntityUtils.topologyEntity(900001L, CLOUD_TARGET_2, 0,
                                               EntityType.APPLICATION_COMPONENT, vmInCanada.getOid());

    private final TopologyEntity.Builder regionCentralUs = createRegion(
            CLOUD_TARGET_2, 2004L, "Central US",
            Collections.emptySet(),
            ImmutableList.of(dbCentralUs, dbsCentralUs, vmInCentralUs, virtualVolumeInCentralUs,
                             computeTier2, storageTier2, unattachedVirtualVolumeInCentralUs));
    private final TopologyEntity.Builder regionCanada = createRegion(
            CLOUD_TARGET_2, 2005L, "Canada",
            Collections.emptySet(),
            ImmutableList.of(computeTier2, storageTier2, vmInCanada, virtualVolumeInCanada, virtualVolume2InCanada));

    private final TopologyEntity.Builder businessAcc4 = createOwner(
            CLOUD_TARGET_2, 5004L, "Business account 4", EntityType.BUSINESS_ACCOUNT,
            vmInCanada, virtualVolumeInCanada, appAzure);
    private final TopologyEntity.Builder cloudService =
            createOwner(CLOUD_TARGET_2, 10000L, "Cloud service 1",
                        EntityType.CLOUD_SERVICE, computeTier2);

    /* Creating an on prem topology.

                       ba
                    /     \
                   /       \
                  /         \
        appc1   as1         as2
           \  /              |
           vm1               vm3
           / \               / \
        pm1  vv            pm3  st2
        /     \            |
     dc1     st1-da1      dc2
      |       /
     pm2    /
       \   /
        vm2

     */
    private final TopologyGraph<TopologyEntity> emptyClusterGraph = TopologyEntityUtils
        .topologyGraphOf(TopologyEntity.newBuilder(pm1InDc1.getEntityBuilder()),
            TopologyEntity.newBuilder(pm2InDc1.getEntityBuilder()),
            TopologyEntity.newBuilder(pmInDc2.getEntityBuilder()),
            TopologyEntity.newBuilder(dc1.getEntityBuilder()),
            TopologyEntity.newBuilder(dc2.getEntityBuilder()),
            TopologyEntity.newBuilder(st1.getEntityBuilder()),
            TopologyEntity.newBuilder(st2.getEntityBuilder()),
            TopologyEntity.newBuilder(da1.getEntityBuilder()));

    private final TopologyGraph<TopologyEntity> graph = TopologyEntityUtils
        .topologyGraphOf(bapp1, appc1, vm1InDc1, vm2InDc1, vmInDc2, virtualVolume, pm1InDc1,
                pm2InDc1, pmInDc2, dc1, dc2, st1, st2, da1, as1,
                as2, az1London, az2London, azOhio,
                az1HongKong, az2HongKong, regionLondon, regionOhio,
                regionHongKong, computeTier, vm1InLondon,
                vm2InLondon, dbLondon, dbsLondon, dbsHongKong,
                vmInOhio, vmInHongKong, businessAcc1, businessAcc2, businessAcc3,
                virtualVolumeInOhio, virtualVolumeInLondon, virtualVolumeInHongKong,
                storageTier, regionCentralUs, regionCanada, dbCentralUs, dbsCentralUs,
                computeTier2, storageTier2, virtualVolumeInCentralUs,
                vmInCentralUs, virtualVolumeInCanada, vmInCanada, businessAcc4, cloudService,
                appAws, appAzure, unattachedVirtualVolumeInCentralUs,
                unattachedVirtualVolumeInLondon, virtualVolume2InCanada);

    private final Set<TopologyEntity.Builder> expectedEntitiesForAwsRegion = Stream
        .of(az1London, az2London, regionLondon, computeTier,
                vm1InLondon, vm2InLondon, dbLondon, dbsLondon, businessAcc1,
                virtualVolumeInLondon, storageTier, appAws,
                unattachedVirtualVolumeInLondon)
        .collect(Collectors.collectingAndThen(Collectors.toSet(),
            Collections::unmodifiableSet));

    private final Set<TopologyEntity.Builder> expectedEntitiesForAzureRegion = Stream
                    .of(regionCentralUs, dbCentralUs, dbsCentralUs, computeTier2,
                            storageTier2, virtualVolumeInCentralUs, vmInCentralUs, cloudService,
                            unattachedVirtualVolumeInCentralUs)
                    .collect(Collectors.collectingAndThen(Collectors.toSet(),
                        Collections::unmodifiableSet));

    private final Set<TopologyEntity.Builder> expectedEntitiesForRegionsList = Stream
        .of(az1London, az2London, azOhio, regionLondon, regionOhio, computeTier,
                vm1InLondon, vm2InLondon, vmInOhio, dbLondon, dbsLondon, businessAcc1,
                businessAcc2, storageTier, virtualVolumeInLondon, virtualVolumeInOhio, appAws,
                unattachedVirtualVolumeInLondon)
        .collect(Collectors.collectingAndThen(Collectors.toSet(),
            Collections::unmodifiableSet));

    private final Set<TopologyEntity.Builder> expectedEntitiesForBusinessAccount = Stream
                    .of(az1HongKong, vmInHongKong, storageTier, regionHongKong,
                            businessAcc1, businessAcc3, virtualVolumeInHongKong)
                    .collect(Collectors.collectingAndThen(Collectors.toSet(),
                                                          Collections::unmodifiableSet));

    private final Set<TopologyEntity.Builder> expectedEntitiesForBusinessAccountsList = Stream
                    .of(azOhio, vmInOhio, businessAcc1, computeTier, storageTier,
                            businessAcc2, businessAcc3, az1HongKong, vmInHongKong,
                            regionOhio, regionHongKong, virtualVolumeInHongKong, virtualVolumeInOhio)
                    .collect(Collectors.collectingAndThen(Collectors.toSet(),
                                                          Collections::unmodifiableSet));

    private final Set<TopologyEntity.Builder> expectedEntitiesForBillingFamily = Stream
                    .of(az1London, vm1InLondon, computeTier, storageTier,
                            businessAcc1, businessAcc3, az1HongKong, vmInHongKong,
                            regionLondon, regionHongKong, virtualVolumeInHongKong,
                            virtualVolumeInLondon, appAws)
                    .collect(Collectors.collectingAndThen(Collectors.toSet(),
                                                          Collections::unmodifiableSet));

    private final Set<TopologyEntity.Builder> expectedEntitiesForAwsVm = Stream
                    .of(az1London, regionLondon, vm1InLondon, businessAcc1,
                            computeTier, virtualVolumeInLondon, storageTier, appAws)
                    .collect(Collectors.collectingAndThen(Collectors.toSet(),
                                                          Collections::unmodifiableSet));

    private final Set<TopologyEntity.Builder> expectedEntitiesForAzureDb = Stream
                    .of(dbCentralUs, regionCentralUs, computeTier2, storageTier2, cloudService)
                    .collect(Collectors.collectingAndThen(Collectors.toSet(),
                                                          Collections::unmodifiableSet));

    private final Set<TopologyEntity.Builder> expectedEntitiesForAwsDbsGroup = Stream
                    .of(az1London, regionLondon, dbsLondon, computeTier, storageTier,
                            dbsHongKong, az2HongKong, regionHongKong)
                    .collect(Collectors.collectingAndThen(Collectors.toSet(),
                                                          Collections::unmodifiableSet));

    private final Set<TopologyEntity.Builder> expectedEntitiesForResourceGroup = Stream
                    .of(dbCentralUs, regionCentralUs, computeTier2, storageTier2,
                            virtualVolumeInCanada, regionCanada, vmInCanada, businessAcc4,
                            cloudService, appAzure)
                    .collect(Collectors.collectingAndThen(Collectors.toSet(),
                                                          Collections::unmodifiableSet));

    private final Set<TopologyEntity.Builder> expectedEntitiesForVolumesGroup = Stream
            .of(regionCentralUs, computeTier2, storageTier2, unattachedVirtualVolumeInCentralUs,
                    virtualVolume2InCanada, regionCanada, vmInCanada, businessAcc4,
                    cloudService, appAzure)
            .collect(Collectors.collectingAndThen(Collectors.toSet(),
                    Collections::unmodifiableSet));

    private final GroupResolver groupResolver = mock(GroupResolver.class);
    private PlanTopologyScopeEditor planTopologyScopeEditor;
    private final GroupServiceMole groupServiceClient = spy(new GroupServiceMole());

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(groupServiceClient);

    @Before
    public void setup() {
        planTopologyScopeEditor = new PlanTopologyScopeEditor(GroupServiceGrpc
            .newBlockingStub(grpcServer.getChannel()));
    }

    /**
     * Tests scope cloud topology for the plan scope with resource group of DB and virtual volume.
     * Topology graph contains entities for 3 targets: hypervisor and 2 clouds.
     * expectedEntitiesForResourceGroup - set of cloud entities expected as result of applying plan scope to the topology.
     */
    @Test
    public void testScopeCloudTopologyForVolumesGroup() {
        // Unattached Virtual Volume in Canada
        final List<Long> oidsList = Arrays.asList(6006L, 6008L);
        testScopeCloudTopology(oidsList, expectedEntitiesForVolumesGroup);
    }

    /**
     * Tests scope cloud topology for the plan scope with single region.
     * Topology graph contains entities for 3 targets: hypervisor and 2 clouds.
     * AWS target has Availability Zone entities.
     * EXPECTED_ENTITIES_FOR_REGION - set of cloud entities expected as result of applying plan scope to the topology.
     */
    @Test
    public void testScopeCloudTopologyForAWSRegion() {
        // Region London
        final List<Long> oidsList = Arrays.asList(2001L);
        testScopeCloudTopology(oidsList, expectedEntitiesForAwsRegion);
    }

    /**
     * Tests scope cloud topology for the plan scope with single region.
     * Topology graph contains entities for 3 targets: hypervisor and 2 clouds.
     * Azure target doesn't have Availability Zone entities.
     * EXPECTED_ENTITIES_FOR_REGION - set of cloud entities expected as result of applying plan scope to the topology.
     */
    @Test
    public void testScopeCloudTopologyForAzureRegion() {
        // Region Central US
        final List<Long> oidsList = Arrays.asList(2004L);
        testScopeCloudTopology(oidsList, expectedEntitiesForAzureRegion);
    }

    /**
     * Tests scope cloud topology for the plan scope with 2 regions.
     * Topology graph contains entities for 3 targets: hypervisor and 2 clouds.
     * expectedEntitiesForRegionsList - set of cloud entities expected as result of applying plan scope to the topology.
     */
    @Test
    public void testScopeCloudTopologyForRegionsList() {
        // Regions London and Ohio
        final List<Long> oidsList = Arrays.asList(2001L, 2002L);
        testScopeCloudTopology(oidsList, expectedEntitiesForRegionsList);
    }

    /**
     * Tests scope cloud topology for the plan scope with 2 regions.
     * Topology graph contains entities for 3 targets: hypervisor and 2 clouds.
     * expectedEntitiesForBusinessAccount - set of cloud entities expected as result of applying plan scope to the topology.
     */
    @Test
    public void testScopeCloudTopologyForBusinessAccount() {
        // Business account 3
        final List<Long> oidsList = Arrays.asList(5003L);
        testScopeCloudTopology(oidsList, expectedEntitiesForBusinessAccount);
    }

    /**
     * Tests scope cloud topology for the plan scope with 2 regions.
     * Topology graph contains entities for 3 targets: hypervisor and 2 clouds.
     * expectedEntitiesForBusinessAccount - set of cloud entities expected as result of applying plan scope to the topology.
     */
    @Test
    public void testScopeCloudTopologyForBusinessAccountsList() {
        // Business account 2 and Business account 3
        final List<Long> oidsList = Arrays.asList(5002L, 5003L);
        testScopeCloudTopology(oidsList, expectedEntitiesForBusinessAccountsList);
    }

    /**
     * Tests scope cloud topology for the plan scope with 2 regions.
     * Topology graph contains entities for 3 targets: hypervisor and 2 clouds.
     * expectedEntitiesForBusinessAccount - set of cloud entities expected as result of applying plan scope to the topology.
     */
    @Test
    public void testScopeCloudTopologyForBillingFamily() {
        // Billing family
        final List<Long> oidsList = Arrays.asList(5001L);
        testScopeCloudTopology(oidsList, expectedEntitiesForBillingFamily);
    }

    /**
     * Tests scope cloud topology for the plan scope with 1 VM.
     * Topology graph contains entities for 3 targets: hypervisor and 2 clouds.
     * expectedEntitiesForAwsVm - set of cloud entities expected as result of applying plan scope to the topology.
     */
    @Test
    public void testScopeCloudTopologyForVM() {
        // VM1 in London
        final List<Long> oidsList = Arrays.asList(4001L);
        testScopeCloudTopology(oidsList, expectedEntitiesForAwsVm);
    }

    /**
     * Tests scope cloud topology for the plan scope with 1 DB.
     * Topology graph contains entities for 3 targets: hypervisor and 2 clouds.
     * expectedEntitiesForAwsVm - set of cloud entities expected as result of applying plan scope to the topology.
     */
    @Test
    public void testScopeCloudTopologyForDB() {
        // DB in Central US
        final List<Long> oidsList = Arrays.asList(8002L);
        testScopeCloudTopology(oidsList, expectedEntitiesForAzureDb);
    }

    /**
     * Tests scope cloud topology for the plan scope with group of 2 DBS.
     * Topology graph contains entities for 3 targets: hypervisor and 2 clouds.
     * expectedEntitiesForAwsDbsGroup - set of cloud entities expected as result of applying plan scope to the topology.
     */
    @Test
    public void testScopeCloudTopologyForDBSGroup() {
        // DBS in London and DBS in Hong Kong
        final List<Long> oidsList = Arrays.asList(9001L, 9002L);
        testScopeCloudTopology(oidsList, expectedEntitiesForAwsDbsGroup);
    }

    /**
     * Tests scope cloud topology for the plan scope with resource group of DB and virtual volume.
     * Topology graph contains entities for 3 targets: hypervisor and 2 clouds.
     * expectedEntitiesForResourceGroup - set of cloud entities expected as result of applying plan scope to the topology.
     */
    @Test
    public void testScopeCloudTopologyForResourceGroup() {
        // DB in Central US and Virtual Volume in Canada
        final List<Long> oidsList = Arrays.asList(8002L, 6005L);
        testScopeCloudTopology(oidsList, expectedEntitiesForResourceGroup);
    }

    private void testScopeCloudTopology(List<Long> oidsList, Set<TopologyEntity.Builder> expectedEntities) {
        final TopologyInfo cloudTopologyInfo = TopologyInfo.newBuilder()
                        .setTopologyContextId(1)
                        .setTopologyId(1)
                        .setTopologyType(TopologyType.PLAN)
                        .setPlanInfo(PlanTopologyInfo.newBuilder().setPlanType("OPTIMIZE_CLOUD").build())
                        .addAllScopeSeedOids(oidsList)
                        .build();
        final TopologyGraph<TopologyEntity> result = planTopologyScopeEditor.scopeCloudTopology(cloudTopologyInfo, graph);
        Assert.assertEquals(expectedEntities.size(), result.size());
        expectedEntities.forEach(entity -> assertTrue(entity.getOid() + " is missing", result.getEntity(entity.getOid())
                        .isPresent()));
    }

    /**
     * Scenario: scope on pm1 and pm2 which consumes on dc1.
     * Expected: the entities in scope should be vm1, vm2, pm1, pm2, dc1, vv, st1, da1, appc1, as1, ba
     *
     * @throws Exception An exception thrown when a stage of the pipeline fails.
     */
    @Test
    public void testScopeOnpremTopologyOnCluster() throws Exception {
        Grouping g = Grouping.newBuilder()
                        .addExpectedTypes(MemberType.newBuilder().setEntity(ApiEntityType.PHYSICAL_MACHINE.typeNumber()))
                        .setDefinition(GroupDefinition.newBuilder()
                        .setStaticGroupMembers(StaticMembers.newBuilder()
                                        .addMembersByType(StaticMembersByType.newBuilder()
                                                        .setType(MemberType.newBuilder()
                                                                        .setEntity(ApiEntityType.PHYSICAL_MACHINE.typeNumber()))
                                                        .addMembers(pm1InDc1.getOid())
                                                        .addMembers(pm2InDc1.getOid())
                                                        )))
                        .build();

        List<Grouping> groups = Arrays.asList(g);
        when(groupServiceClient.getGroups(GetGroupsRequest.newBuilder()
            .setGroupFilter(GroupFilter.newBuilder().addId(25001L))
            .setReplaceGroupPropertyWithGroupMembershipFilter(true)
            .build())).thenReturn(groups);
        when(groupResolver.resolve(eq(g), eq(graph))).thenReturn(
            new ResolvedGroup(g, Collections.singletonMap(ApiEntityType.PHYSICAL_MACHINE,
                Sets.newHashSet(pm1InDc1.getOid(), pm2InDc1.getOid()))));

        final PlanScope planScope = PlanScope.newBuilder()
                        .addScopeEntries(PlanScopeEntry.newBuilder().setClassName("Cluster")
                                .setScopeObjectOid(25001L).setDisplayName("PM cluster/dc1").build()).build();
        // populate InvertedIndex
        InvertedIndex<TopologyEntity, TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider>
                index = planTopologyScopeEditor.createInvertedIndex();
        graph.entities().forEach(entity -> index.add(entity));
        // scope using inverted index
        TopologyGraph<TopologyEntity> result = planTopologyScopeEditor
                .indexBasedScoping(index, graph, groupResolver, planScope, PlanProjectType.USER);
        assertEquals(11, result.size());
    }

    /**
     * Scenario: scope on pm1 and pm2 which consumes on dc1.
     * Expected: the entities in scope should be vm1, vm2, pm1, pm2, dc1, vv, st1, da1, appc1, as1, ba
     * Because ds1 is shared across pm1, pm2, and (pm3 which is not in the cluster) via dspm, datastore
     * Accesses relationship, we still should not pull in pm3 and its related entities like st2 into
     * the scope.
     *
     * @throws Exception An exception thrown when a stage of the pipeline fails.
     */
    @Test
    public void testScopeOnpremTopologyOnClusterAccessesRelationship() throws Exception {
        Grouping g = Grouping.newBuilder()
                        .addExpectedTypes(MemberType.newBuilder().setEntity(ApiEntityType.PHYSICAL_MACHINE.typeNumber()))
                        .setDefinition(GroupDefinition.newBuilder()
                        .setStaticGroupMembers(StaticMembers.newBuilder()
                                        .addMembersByType(StaticMembersByType.newBuilder()
                                                        .setType(MemberType.newBuilder()
                                                                        .setEntity(ApiEntityType.PHYSICAL_MACHINE.typeNumber()))
                                                        .addMembers(pm1InDc1.getOid())
                                                        .addMembers(pm2InDc1.getOid())
                                                        )))
                        .build();

        // st1 connected to PM1, PM2, and PM3. PM3 is same entity type as seed members PM1 and PM2
        // and should not be brought into scope and we should not pull in related entities of it
        // into scope.
        pm1InDc1.getEntityBuilder().getCommoditySoldListBuilder(2).setAccesses(st1.getOid());
        pm2InDc1.getEntityBuilder().getCommoditySoldListBuilder(2).setAccesses(st1.getOid());
        pmInDc2.getEntityBuilder().getCommoditySoldListBuilder(2).setAccesses(st1.getOid());
        pmInDc2.getEntityBuilder().getCommoditySoldListBuilder(3).setAccesses(st2.getOid());

        // st2 connected only to PM3 and should not be brought into scope.
        st1.getEntityBuilder().getCommoditySoldListBuilder(2).setAccesses(pm1InDc1.getOid());
        st1.getEntityBuilder().getCommoditySoldListBuilder(2).setAccesses(pm2InDc1.getOid());
        st1.getEntityBuilder().getCommoditySoldListBuilder(3).setAccesses(pmInDc2.getOid());
        st2.getEntityBuilder().getCommoditySoldListBuilder(2).setAccesses(pmInDc2.getOid());

        List<Grouping> groups = Arrays.asList(g);
        when(groupServiceClient.getGroups(GetGroupsRequest.newBuilder()
            .setGroupFilter(GroupFilter.newBuilder().addId(25001L))
            .setReplaceGroupPropertyWithGroupMembershipFilter(true)
            .build())).thenReturn(groups);
        when(groupResolver.resolve(eq(g), eq(graph))).thenReturn(
            new ResolvedGroup(g, Collections.singletonMap(ApiEntityType.PHYSICAL_MACHINE,
                Sets.newHashSet(pm1InDc1.getOid(), pm2InDc1.getOid()))));

        final PlanScope planScope = PlanScope.newBuilder()
                        .addScopeEntries(PlanScopeEntry.newBuilder().setClassName("Cluster")
                                .setScopeObjectOid(25001L).setDisplayName("PM cluster/dc1").build()).build();
        // populate InvertedIndex
        InvertedIndex<TopologyEntity, TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider>
                index = planTopologyScopeEditor.createInvertedIndex();
        graph.entities().forEach(entity -> index.add(entity));
        // scope using inverted index
        TopologyGraph<TopologyEntity> result = planTopologyScopeEditor
                .indexBasedScoping(index, graph, groupResolver, planScope, PlanProjectType.USER);
        assertEquals(11, result.size());
    }

    private static TopologyEntity.Builder createHypervisorHost(long oid,
                                                               String displayName,
                                                               EntityType entityType,
                                                               Map<Long, List<TopologyDTO.CommodityType>> producers,
                                                               List<TopologyDTO.CommodityType> soldComms,
                                                               List<Long> connectedStorages,
                                                               long... connectedEntities) {
        TopologyEntity.Builder entity = createHypervisorTopologyEntity(oid, displayName,
            entityType, producers, soldComms, connectedEntities);
        connectedStorages.forEach(st -> entity.getEntityBuilder().addCommoditySoldList(
            CommoditySoldDTO.newBuilder().setCommodityType(DATASTORE).setAccesses(st)));
        return entity;
    }

    /**
     * Scenario: scope on ba which consumes as1 and as2.
     * Expected: the entities in scope should be ba, as1, vm1, vm2, pm1, pm2, dc1, vv, st1,
     * da1, as2, vm3, pm3, dc2, st2
     *
     * @throws Exception An exception thrown when a stage of the pipeline fails.
     */
    @Test
    public void testScopeOnpremTopologyOnBA() throws Exception {
        final PlanScope planScope = PlanScope.newBuilder()
                        .addScopeEntries(PlanScopeEntry.newBuilder().setClassName("BusinessApplication")
                                .setScopeObjectOid(80001L).setDisplayName("BusinessApplication1").build()).build();
        // populate InvertedIndex
        InvertedIndex<TopologyEntity, TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider>
                index = planTopologyScopeEditor.createInvertedIndex();
        graph.entities().forEach(entity -> index.add(entity));
        // scope using inverted index
        // The app doesnt get pulled in because it isnt connected to the BApp. We just pull in the AS's.
        // When processing the VMs as buyersToSatisfy we just go down to its providers and not up to the apps.
        TopologyGraph<TopologyEntity> result = planTopologyScopeEditor
                .indexBasedScoping(index, graph, groupResolver, planScope, PlanProjectType.USER);
        // THE APP THAT IS NOT CONNECTED TO BAPP WILL NOT GET PULLED INTO THE SCOPE
        assertEquals(15, result.size());
    }

    /**
     * Scenario: scope on st2 which hosts vm on dc2.
     * Expected: the entities in scope should be ba, as2, vm3, pm3, st2, dc2, da1
     *
     * @throws Exception An exception thrown when a stage of the pipeline fails.
     */
    @Test
    public void testScopeOnpremTopologyOnStorage() throws Exception {
        final PlanScope planScope = PlanScope.newBuilder()
                        .addScopeEntries(PlanScopeEntry.newBuilder().setClassName("Storage")
                                .setScopeObjectOid(40002L).setDisplayName("Storage2").build()).build();
        // populate InvertedIndex
        InvertedIndex<TopologyEntity, TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider>
                index = planTopologyScopeEditor.createInvertedIndex();
        graph.entities().forEach(entity -> index.add(entity));
        // scope using inverted index
        TopologyGraph<TopologyEntity> result = planTopologyScopeEditor
                .indexBasedScoping(index, graph, groupResolver, planScope, PlanProjectType.USER);
        result.entities().forEach(e -> System.out.println(e.getOid() + " "));
        assertEquals(7, result.size());
    }

    /**
     * Scenario: scope on vm2 which consumes pm2 on dc1, st1 on da1. The vm2 hosts no application at all.
     * Expected: the entities in scope should be dc1, da1, pm1, pm2, st1, vm2, and virtualVolume
     *           who st1 is providing.
     *
     * @throws Exception An exception thrown when a stage of the pipeline fails.
     */
    @Test
    public void testScopeOnpremTopologyOnVM() throws Exception {
        final PlanScope planScope = PlanScope.newBuilder()
                        .addScopeEntries(PlanScopeEntry.newBuilder().setClassName("VirtualMachine")
                                .setScopeObjectOid(30002L).setDisplayName("VM2").build()).build();
        // populate InvertedIndex
        InvertedIndex<TopologyEntity, TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider>
                index = planTopologyScopeEditor.createInvertedIndex();
        graph.entities().forEach(entity -> index.add(entity));
        // scope using inverted index
        TopologyGraph<TopologyEntity> result = planTopologyScopeEditor
                .indexBasedScoping(index, graph, groupResolver, planScope, PlanProjectType.USER);
        result.entities().forEach(e -> System.out.println(e.getOid() + " "));
        assertEquals(7, result.size());

        // Ensure virtual volume is included in scope due to storage.
        assertEquals(1, result.entities()
                .filter(entity -> entity.getEntityType() == EntityType.VIRTUAL_VOLUME_VALUE)
                .count());
    }

    /**
     * Scenario: scope on vm2 and a clone added of this VM via plan scenario.
     * Expected: the entities in scope should be vm2 and its clone.
     *
     * @throws Exception An exception thrown when a stage of the pipeline fails.
     */
    @Test
    public void testScopeOnpremTopologyOnVMWithCloneInScope() throws Exception {
        long originalVMOid = 30002L;
        final PlanScope planScope = PlanScope.newBuilder()
                .addScopeEntries(PlanScopeEntry.newBuilder().setClassName("VirtualMachine")
                        .setScopeObjectOid(originalVMOid).setDisplayName("VM2").build()).build();
        long cloneOid = 3456L;
        TopologyEntity.Builder cloneOfVM1 =  TopologyEntityUtils.topologyEntityBuilder(TopologyEntityDTO.newBuilder()
            .setOid(cloneOid)
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .setOrigin(Origin.newBuilder()
                    .setPlanScenarioOrigin(PlanScenarioOrigin.newBuilder().setPlanId(99))
                .build()))
            .setClonedFromEntity(vm2InDc1.getEntityBuilder());
        TopologyGraph<TopologyEntity> graphWithClone = TopologyEntityUtils.topologyGraphOf(vm2InDc1, cloneOfVM1);
        // populate InvertedIndex
        InvertedIndex<TopologyEntity, TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider>
                index = planTopologyScopeEditor.createInvertedIndex();
        graphWithClone.entities().forEach(entity -> index.add(entity));
        // scope using inverted index
        TopologyGraph<TopologyEntity> result = planTopologyScopeEditor
                .indexBasedScoping(index, graphWithClone, groupResolver, planScope, PlanProjectType.USER);

        result.entities().forEach(e -> System.out.println(e.getOid() + " "));
        assertEquals(2, result.size());
        assertTrue(result.getEntity(cloneOid).isPresent());
        // Make sure clone has original entity Oid
        assertTrue(result.getEntity(cloneOid).isPresent());
        assertTrue(result.getEntity(cloneOid).get().getClonedFromEntity().isPresent());
        assertEquals(originalVMOid, result.getEntity(cloneOid).get().getClonedFromEntity().get().getOid());
}

    /**
     * Scenario: scope on VM2 and a clone added of out of scope VM1_clone (VM1 is out of scope) via plan scenario.
     * Expected: the entities in scope should be the clone and VM2.
     *
     * @throws Exception An exception thrown when a stage of the pipeline fails.
     */
    @Test
    public void testScopeOnpremTopologyOnClonedVMFromOutOfScope() throws Exception {
        long originalVMOid = 30002L;
        final PlanScope planScope = PlanScope.newBuilder()
                .addScopeEntries(PlanScopeEntry.newBuilder().setClassName("VirtualMachine")
                        .setScopeObjectOid(originalVMOid).setDisplayName("VM2").build()).build();
        long cloneOid = 3456L;
        TopologyEntity.Builder cloneOfVM1 =  TopologyEntityUtils.topologyEntityBuilder(TopologyEntityDTO.newBuilder()
                .setOid(cloneOid)
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setOrigin(Origin.newBuilder()
                        .setPlanScenarioOrigin(PlanScenarioOrigin.newBuilder().setPlanId(99))
                        .build()))
                .setClonedFromEntity(vm1InDc1.getEntityBuilder());
        TopologyGraph<TopologyEntity> graphWithClone = TopologyEntityUtils.topologyGraphOf(vm2InDc1, cloneOfVM1, vm1InDc1);
        // populate InvertedIndex
        InvertedIndex<TopologyEntity, TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider>
                index = planTopologyScopeEditor.createInvertedIndex();
        graphWithClone.entities().forEach(entity -> index.add(entity));
        // scope using inverted index
        TopologyGraph<TopologyEntity> result = planTopologyScopeEditor
                .indexBasedScoping(index, graphWithClone, groupResolver, planScope, PlanProjectType.USER);

        result.entities().forEach(e -> System.out.println(e.getOid() + " "));
        assertEquals(2, result.size());
        // Make sure clone is in scope and it still references original OID.
        assertTrue(result.getEntity(cloneOid).isPresent());
        assertTrue(result.getEntity(cloneOid).get().getClonedFromEntity().isPresent());
        assertEquals(vm1InDc1.getOid(), result.getEntity(cloneOid).get().getClonedFromEntity().get().getOid());
    }

    /**
     * Test operations on the {@link FastLookupQueue} used internally in the editor.
     */
    @Test
    public void testLookupQueue() {
        FastLookupQueue lookupQueue = new FastLookupQueue();
        assertTrue(lookupQueue.isEmpty());

        lookupQueue.tryAdd(1L);
        assertFalse(lookupQueue.isEmpty());
        lookupQueue.tryAdd(2L);
        assertThat(lookupQueue.size(), is(2));

        // Second addition shouldn't increase the size of the queue.
        lookupQueue.tryAdd(1L);
        assertThat(lookupQueue.size(), is(2));

        // Check contains.
        assertTrue(lookupQueue.contains(1L));
        assertTrue(lookupQueue.contains(2L));
        assertFalse(lookupQueue.contains(3L));

        // Start removing stuff, and verify that it gets removed in the right order.
        assertThat(lookupQueue.remove(), is(1L));
        assertFalse(lookupQueue.contains(1L));
        lookupQueue.tryAdd(3L);
        assertThat(lookupQueue.remove(), is(2L));
        assertFalse(lookupQueue.contains(2L));
        assertThat(lookupQueue.remove(), is(3L));
        assertTrue(lookupQueue.isEmpty());
    }

    private static TopologyEntity.Builder createHypervisorTopologyEntity(long oid,
                                                                         String displayName,
                                                                         EntityType entityType,
                                                                         Map<Long, List<TopologyDTO.CommodityType>> producers,
                                                                         List<TopologyDTO.CommodityType> soldComms,
                                                                         long... connectedEntities) {
        TopologyEntity.Builder entity = TopologyEntityUtils.topologyEntity(oid, HYPERVISOR_TARGET, 0, displayName,
                entityType, producers, soldComms);
        Arrays.stream(connectedEntities).forEach(e ->
            entity.getEntityBuilder()
                .addConnectedEntityList(ConnectedEntity.newBuilder()
                        .setConnectedEntityId(e)
                        .setConnectionType(ConnectionType.NORMAL_CONNECTION)
                        .build()));
        return entity;
    }

    /**
     * Scenario: scope on emptyClusterGraph's pm1 and pm2 which consumes on dc1.
     * Expected: the entities in scope should be pm1, pm2, dc1, vv, st1, da1, app1, as1, ba
     *
     * @throws Exception An exception thrown when a stage of the pipeline fails.
     */
    @Test
    public void testScopeOnpremTopologyOnEmptyCluster() throws Exception {
        Grouping g = Grouping.newBuilder()
            .addExpectedTypes(MemberType.newBuilder().setEntity(ApiEntityType.PHYSICAL_MACHINE.typeNumber()))
            .setDefinition(GroupDefinition.newBuilder()
                .setStaticGroupMembers(StaticMembers.newBuilder()
                    .addMembersByType(StaticMembersByType.newBuilder()
                        .setType(MemberType.newBuilder()
                            .setEntity(ApiEntityType.PHYSICAL_MACHINE.typeNumber()))
                        .addMembers(pm1InDc1.getOid())
                        .addMembers(pm2InDc1.getOid())
                    )))
            .build();

        List<Grouping> groups = Arrays.asList(g);
        when(groupServiceClient.getGroups(GetGroupsRequest.newBuilder()
            .setGroupFilter(GroupFilter.newBuilder().addId(25001L))
            .setReplaceGroupPropertyWithGroupMembershipFilter(true)
            .build())).thenReturn(groups);
        when(groupResolver.resolve(eq(g), eq(emptyClusterGraph))).thenReturn(
            new ResolvedGroup(g, Collections.singletonMap(ApiEntityType.PHYSICAL_MACHINE,
                Sets.newHashSet(pm1InDc1.getOid(), pm2InDc1.getOid()))));

        final PlanScope planScope = PlanScope.newBuilder()
            .addScopeEntries(PlanScopeEntry.newBuilder().setClassName("Cluster")
                .setScopeObjectOid(25001L).setDisplayName("PM cluster/dc1").build()).build();
        // populate InvertedIndex
        InvertedIndex<TopologyEntity, TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider>
            index = planTopologyScopeEditor.createInvertedIndex();
        emptyClusterGraph.entities().forEach(entity -> index.add(entity));
        // scope using inverted index
        TopologyGraph<TopologyEntity> result = planTopologyScopeEditor
            .indexBasedScoping(index, emptyClusterGraph, groupResolver, planScope, PlanProjectType.USER);
        assertEquals(5, result.size());
        assertEquals(ImmutableSet.of(pm1InDc1.getOid(), pm2InDc1.getOid(), dc1.getOid(), st1.getOid(), da1.getOid()),
            result.entities().map(TopologyEntity::getOid).collect(Collectors.toSet()));
    }

    private static TopologyEntity.Builder createCloudTopologyAvailabilityZone(
            long targetId, long oid, String displayName,
            TopologyEntity.Builder... aggregatedEntities) {
        Arrays.stream(aggregatedEntities).forEach(e ->
                e.getEntityBuilder().addConnectedEntityList(
                        ConnectedEntity.newBuilder()
                                .setConnectionType(ConnectionType.AGGREGATED_BY_CONNECTION)
                                .setConnectedEntityId(oid)
                                .setConnectedEntityType(EntityType.AVAILABILITY_ZONE_VALUE)));
        return TopologyEntityUtils.connectedTopologyEntity(oid, targetId, 0,
                                                           displayName, EntityType.AVAILABILITY_ZONE,
                                                           Collections.emptySet());
    }

    private static TopologyEntity.Builder createOwner(
            long targetId, long oid, String displayName, EntityType entityType,
            TopologyEntity.Builder... ownedEntities) {
        final Collection<ConnectedEntity> connectedEntities =
                Arrays.stream(ownedEntities)
                        .map(e -> ConnectedEntity.newBuilder()
                                .setConnectedEntityId(e.getOid())
                                .setConnectedEntityType(e.getEntityType())
                                .setConnectionType(ConnectionType.OWNS_CONNECTION)
                                .build())
                        .collect(Collectors.toList());
        return TopologyEntityUtils.connectedTopologyEntity(oid, targetId, 0,
                displayName, entityType, connectedEntities);
    }

    private static TopologyEntity.Builder createCloudConnectedTopologyEntity(
            long targetId, long oid, String displayName, EntityType entityType,
            long... connectedToEntities) {
        return TopologyEntityUtils.connectedTopologyEntity(oid, targetId, 0, displayName,
                                                           entityType, connectedToEntities);
    }

    private static TopologyEntity.Builder createRegion(
            long targetId, long oid, String displayName,
            Collection<TopologyEntity.Builder> availabilityZones,
            Collection<TopologyEntity.Builder> aggregatedEntities) {

        final Collection<ConnectedEntity> connectedAvailabilityZones =
                availabilityZones.stream()
                    .map(e -> ConnectedEntity.newBuilder()
                                    .setConnectedEntityId(e.getOid())
                                    .setConnectedEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
                                    .setConnectionType(ConnectionType.OWNS_CONNECTION)
                                    .build())
                    .collect(Collectors.toList());

        aggregatedEntities.forEach(e ->
                e.getEntityBuilder().addConnectedEntityList(
                    ConnectedEntity.newBuilder()
                        .setConnectionType(ConnectionType.AGGREGATED_BY_CONNECTION)
                        .setConnectedEntityId(oid)
                        .setConnectedEntityType(EntityType.REGION_VALUE)));

        return TopologyEntityUtils.connectedTopologyEntity(oid, targetId, 0,
                                                           displayName, EntityType.REGION,
                                                           connectedAvailabilityZones);
    }

    private static TopologyEntity.Builder createCloudVm(
            final long targetId,
            final long oid,
            final String displayName,
            final long... volumeOids) {
        return TopologyEntityUtils.topologyEntity(
                oid, targetId, 0, displayName, EntityType.VIRTUAL_MACHINE, volumeOids);
    }

    private static TopologyEntity.Builder createCloudVolume(
            final long targetId,
            final long oid,
            final String displayName,
            final long storageTierOid) {
        return TopologyEntityUtils.topologyEntity(
                oid, targetId, 0, displayName, EntityType.VIRTUAL_VOLUME, storageTierOid);
    }
}
