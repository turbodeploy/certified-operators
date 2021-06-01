package com.vmturbo.topology.processor.topology;

import static org.hamcrest.Matchers.containsInAnyOrder;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
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
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
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
import com.vmturbo.platform.sdk.common.util.Pair;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.TopologyEntity.Builder;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.group.ResolvedGroup;
import com.vmturbo.topology.processor.topology.PlanTopologyScopeEditor.FastLookupQueue;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipelineContext;

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
    private static final TopologyDTO.CommodityType CPU_ALLOC_SOLD = TopologyDTO.CommodityType.newBuilder().setType(
            CommodityType.CPU_ALLOCATION_VALUE).setKey("VDCinDC1").build();
    private static final TopologyDTO.CommodityType CPU_ALLOC_PM1 = TopologyDTO.CommodityType.newBuilder().setType(
            CommodityType.CPU_ALLOCATION_VALUE).setKey("PM1").build();
    private static final TopologyDTO.CommodityType CPU_ALLOC_PM2 = TopologyDTO.CommodityType.newBuilder().setType(
            CommodityType.CPU_ALLOCATION_VALUE).setKey("PM2").build();
    private static final TopologyDTO.CommodityType DC1 = TopologyDTO.CommodityType.newBuilder().setType(CommonDTO.CommodityDTO.CommodityType.DATACENTER_VALUE)
            .setKey("DC::DC1").build();
    private static final TopologyDTO.CommodityType DC1_HOSTS = TopologyDTO.CommodityType.newBuilder().setType(CommonDTO.CommodityDTO.CommodityType.DATACENTER_VALUE)
        .setKey("DC1").build();
    private static final TopologyDTO.CommodityType DC2 = TopologyDTO.CommodityType.newBuilder().setType(CommonDTO.CommodityDTO.CommodityType.DATACENTER_VALUE)
            .setKey("DC::DC2").build();
    private static final TopologyDTO.CommodityType DC2_HOSTS = TopologyDTO.CommodityType.newBuilder().setType(CommonDTO.CommodityDTO.CommodityType.DATACENTER_VALUE)
        .setKey("DC2").build();
    private static final TopologyDTO.CommodityType SC1 = TopologyDTO.CommodityType.newBuilder().setType(CommonDTO.CommodityDTO.CommodityType.CLUSTER_VALUE)
            .setKey("SC1").build();
    private static final TopologyDTO.CommodityType SC2 = TopologyDTO.CommodityType.newBuilder().setType(CommonDTO.CommodityDTO.CommodityType.CLUSTER_VALUE)
            .setKey("SC2").build();
    private static final TopologyDTO.CommodityType APP = TopologyDTO.CommodityType.newBuilder().setType(CommonDTO.CommodityDTO.CommodityType.APPLICATION_VALUE)
            .setKey("APP").build();
    private static final TopologyDTO.CommodityType APP1 = TopologyDTO.CommodityType.newBuilder().setType(CommonDTO.CommodityDTO.CommodityType.APPLICATION_VALUE)
            .setKey("APP1").build();
    private static final TopologyDTO.CommodityType APP2 = TopologyDTO.CommodityType.newBuilder().setType(CommonDTO.CommodityDTO.CommodityType.APPLICATION_VALUE)
            .setKey("APP2").build();
    private static final TopologyDTO.CommodityType APP3 = TopologyDTO.CommodityType.newBuilder().setType(CommonDTO.CommodityDTO.CommodityType.APPLICATION_VALUE)
            .setKey("APP3").build();
    private static final TopologyDTO.CommodityType APP4 = TopologyDTO.CommodityType.newBuilder().setType(CommonDTO.CommodityDTO.CommodityType.APPLICATION_VALUE)
        .setKey("APP4").build();
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
    private static final TopologyDTO.CommodityType VMPM_ACCESS = TopologyDTO.CommodityType.newBuilder().setType(CommonDTO.CommodityDTO.CommodityType.VMPM_ACCESS_VALUE).setKey("foo").build();
    private static final TopologyDTO.CommodityType VMPM_ACCESS_KUBEPOD = TopologyDTO.CommodityType.newBuilder().setType(CommonDTO.CommodityDTO.CommodityType.VMPM_ACCESS_VALUE).setKey("kube-pod-aws").build();
    private static final TopologyDTO.CommodityType CLUSTER_COMM_AWS = TopologyDTO.CommodityType.newBuilder().setType(CommodityType.CLUSTER_VALUE).setKey("kube-cluster-aws").build();
    private static final TopologyDTO.CommodityType VCPUREQ_QUOTA = TopologyDTO.CommodityType.newBuilder().setType(CommodityType.VCPU_REQUEST_QUOTA_VALUE).build();

    private static final TopologyDTO.CommodityType CLUSTER_COMM_AZURE = TopologyDTO.CommodityType.newBuilder().setType(CommodityType.CLUSTER_VALUE).setKey("kube-cluster-azure").build();
    private static final TopologyDTO.CommodityType VCPUREQ_QUOTA_AZURE = TopologyDTO.CommodityType.newBuilder().setType(CommodityType.VCPU_REQUEST_QUOTA_VALUE).build();
    private static final TopologyDTO.CommodityType VMPM_ACCESS_KUBEPOD_AZURE = TopologyDTO.CommodityType.newBuilder().setType(CommonDTO.CommodityDTO.CommodityType.VMPM_ACCESS_VALUE).setKey("kube-pod-azure").build();

    private static final List<TopologyDTO.CommodityType> basketSoldByPMToVMinDC1 = Lists.newArrayList(DC1_HOSTS, CPU, DS1);
    private static final List<TopologyDTO.CommodityType> basketSoldByPMToVMinDC2 = Lists.newArrayList(DC2_HOSTS, CPU, DS1, DS2);
    private static final List<TopologyDTO.CommodityType> basketSoldByPM1inDC1 = Lists.newArrayList(DC1_HOSTS, CPU, DS1, CPU_ALLOC_PM1);
    private static final List<TopologyDTO.CommodityType> basketSoldByPM2inDC1 = Lists.newArrayList(DC1_HOSTS, CPU, DS1, CPU_ALLOC_PM2);
    private static final List<TopologyDTO.CommodityType> basketSoldByVDCinDC1 = Lists.newArrayList(CPU_ALLOC_SOLD);
    private static final List<TopologyDTO.CommodityType> basketSoldByPM1toVDC = Lists.newArrayList(CPU_ALLOC_PM1);
    private static final List<TopologyDTO.CommodityType> basketSoldByPM2toVDC = Lists.newArrayList(CPU_ALLOC_PM2);
    private static final List<TopologyDTO.CommodityType> basketSoldByDC1 = Lists.newArrayList(DC1, POWER);
    private static final List<TopologyDTO.CommodityType> basketSoldByDC2 = Lists.newArrayList(DC2, POWER);
    private static final List<TopologyDTO.CommodityType> basketSoldByVM1 = Lists.newArrayList(VCPU, APP1);
    private static final List<TopologyDTO.CommodityType> basketSoldByVM2 = Lists.newArrayList(VCPU, APP2);
    private static final List<TopologyDTO.CommodityType> basketSoldByVM4 = Lists.newArrayList(VCPU, APP4);
    private static final List<TopologyDTO.CommodityType> basketSoldByVM = Lists.newArrayList(VCPU, APP);
    private static final List<TopologyDTO.CommodityType> basketSoldByVV = Lists.newArrayList(ST_AMT);
    private static final List<TopologyDTO.CommodityType> basketSoldByDA = Lists.newArrayList(EXTENT1, ST_AMT);
    private static final List<TopologyDTO.CommodityType> basketSoldByDS1 = Lists.newArrayList(ST_AMT, SC1, DSPM1, DSPM2);
    private static final List<TopologyDTO.CommodityType> basketSoldByLocalDS1 = Lists.newArrayList(ST_AMT, SC1, DSPM1);
    private static final List<TopologyDTO.CommodityType> basketSoldByDS2 = Lists.newArrayList(ST_AMT, SC2, DSPM2);
    private static final List<TopologyDTO.CommodityType> basketSoldByLocalDS2 = Lists.newArrayList(ST_AMT, SC1, DSPM2);
    private static final List<TopologyDTO.CommodityType> basketSoldByAS1 = Lists.newArrayList(BAPP1);
    private static final List<TopologyDTO.CommodityType> basketSoldByAS2 = Lists.newArrayList(BAPP2);
    private static final List<TopologyDTO.CommodityType> basketSoldByVMInUSEast = Lists.newArrayList(APP3);
    private static final List<TopologyDTO.CommodityType> basketSoldByKubeVM1 = Lists.newArrayList(VCPU, CLUSTER_COMM_AWS);
    private static final List<TopologyDTO.CommodityType> basketSoldByKubeVM2 = Lists.newArrayList(VCPU, CLUSTER_COMM_AWS);
    private static final List<TopologyDTO.CommodityType> basketSoldByKubeVMinAzure = Lists.newArrayList(VCPU, CLUSTER_COMM_AZURE);

    private static final Map<Long, Pair<Integer, List<TopologyDTO.CommodityType>>> commBoughtByVMinDC1PM1DS1VDC = ImmutableMap.of(
        20001L, new Pair<>(EntityType.PHYSICAL_MACHINE_VALUE, basketSoldByPMToVMinDC1),
        40001L, new Pair<>(EntityType.STORAGE_VALUE, basketSoldByDS1),
        100001L, new Pair<>(EntityType.VIRTUAL_DATACENTER_VALUE, basketSoldByVDCinDC1)
    );

    private static final Map<Long, Pair<Integer, List<TopologyDTO.CommodityType>>> commBoughtByVMinDC1PM2DS1VDC = ImmutableMap.of(
        20002L,new Pair<>(EntityType.PHYSICAL_MACHINE_VALUE,  basketSoldByPMToVMinDC1),
        40001L, new Pair<>(EntityType.STORAGE_VALUE, basketSoldByDS1),
        100001L, new Pair<>(EntityType.VIRTUAL_DATACENTER_VALUE, basketSoldByVDCinDC1)
    );

    private static final Map<Long, Pair<Integer, List<TopologyDTO.CommodityType>>> commBoughtByVMinDC1PM2DS1VDCVV = ImmutableMap.of(
        20002L, new Pair<>(EntityType.PHYSICAL_MACHINE_VALUE, basketSoldByPMToVMinDC1),
        40001L, new Pair<>(EntityType.STORAGE_VALUE, basketSoldByDS1),
        100001L, new Pair<>(EntityType.VIRTUAL_DATACENTER_VALUE, basketSoldByVDCinDC1),
        2502L, new Pair<>(EntityType.VIRTUAL_VOLUME_VALUE, basketSoldByVV)
    );

    private static final Map<Long, Pair<Integer, List<TopologyDTO.CommodityType>>> commBoughtByVVDS1 = ImmutableMap.of(
        40001L, new Pair<>(EntityType.STORAGE_VALUE, Lists.newArrayList(ST_AMT))
    );

    private static final Map<Long, Pair<Integer, List<TopologyDTO.CommodityType>>> commBoughtByPod = ImmutableMap.of(
        30003L, new Pair<>(EntityType.VIRTUAL_MACHINE_VALUE, Lists.newArrayList(VCPU)),
        200004L, new Pair<>(EntityType.VIRTUAL_VOLUME_VALUE, Lists.newArrayList(ST_AMT))
    );

    private static final Map<Long, Pair<Integer, List<TopologyDTO.CommodityType>>> commBoughtByCntFromPod = ImmutableMap.of(
        200001L, new Pair<>(EntityType.CONTAINER_POD_VALUE, Lists.newArrayList(VCPU, VMPM_ACCESS))
    );

    private static final Map<Long, Pair<Integer, List<TopologyDTO.CommodityType>>> commBoughtByVMinDC2PMDS2 = ImmutableMap.of(
        20003L, new Pair<>(EntityType.PHYSICAL_MACHINE_VALUE, basketSoldByPMToVMinDC2),
        40002L, new Pair<>(EntityType.STORAGE_VALUE, basketSoldByDS2)
    );

    private static final Map<Long, Pair<Integer, List<TopologyDTO.CommodityType>>> commBoughtByPMinDC1  = ImmutableMap.of(
        10001L, new Pair<>(EntityType.DATACENTER_VALUE, basketSoldByDC1)
    );

    private static final Map<Long, Pair<Integer, List<TopologyDTO.CommodityType>>> commBoughtByPMinDC2  = ImmutableMap.of(
        10002L, new Pair<>(EntityType.DATACENTER_VALUE, basketSoldByDC2)
    );

    private static final Map<Long, Pair<Integer, List<TopologyDTO.CommodityType>>> commBoughtByApp1Comp1 = ImmutableMap.of(
        30001L, new Pair<>(EntityType.VIRTUAL_MACHINE_VALUE, basketSoldByVM1),
        30003L, new Pair<>(EntityType.VIRTUAL_MACHINE_VALUE, basketSoldByVM2)
    );

    private static final Map<Long, Pair<Integer, List<TopologyDTO.CommodityType>>> commBoughtByVDCinDC1 = ImmutableMap.of(
        20001L, new Pair<>(EntityType.PHYSICAL_MACHINE_VALUE, basketSoldByPM1toVDC),
        20002L, new Pair<>(EntityType.PHYSICAL_MACHINE_VALUE, basketSoldByPM2toVDC)
    );

    private static final Map<Long, Pair<Integer, List<TopologyDTO.CommodityType>>> commBoughtByApp1 = ImmutableMap.of(
        30001L, new Pair<>(EntityType.VIRTUAL_MACHINE_VALUE, basketSoldByVM1)
    );

    private static final Map<Long, Pair<Integer, List<TopologyDTO.CommodityType>>> commBoughtByApp2 = ImmutableMap.of(
        30003L, new Pair<>(EntityType.VIRTUAL_MACHINE_VALUE, basketSoldByVM)
    );

    private static final Map<Long, Pair<Integer, List<TopologyDTO.CommodityType>>> commBoughtByDSinDA1 = ImmutableMap.of(
        50001L, new Pair<>(EntityType.DISK_ARRAY_VALUE, basketSoldByDA)
    );

    private static final Map<Long, Pair<Integer, List<TopologyDTO.CommodityType>>> commBoughtByBA = ImmutableMap.of(
        70001L, new Pair<>(EntityType.APPLICATION_SERVER_VALUE, basketSoldByAS1),
        70002L,  new Pair<>(EntityType.APPLICATION_SERVER_VALUE, basketSoldByAS2)
    );

    private static final Map<Long, Pair<Integer, List<TopologyDTO.CommodityType>>> commBoughtByBT = ImmutableMap.of(
        90002L, new Pair<>(EntityType.SERVICE_VALUE, basketSoldByVMInUSEast),
        90003L, new Pair<>(EntityType.SERVICE_VALUE, Collections.singletonList(APP1))
    );

    private static final Map<Long, Pair<Integer, List<TopologyDTO.CommodityType>>> commBoughtByService1 = ImmutableMap.of(
        4007L, new Pair<>(EntityType.VIRTUAL_MACHINE_VALUE, basketSoldByVMInUSEast)
    );

    private static final Map<Long, Pair<Integer, List<TopologyDTO.CommodityType>>> commBoughtByService2 = ImmutableMap.of(
        30001L, new Pair<>(EntityType.VIRTUAL_MACHINE_VALUE, Collections.singletonList(APP1))
    );

    private static final int HYPERVISOR_TARGET = 0;
    private static final int CLOUD_TARGET_1 = 1;
    private static final int CLOUD_TARGET_2 = 2;
    private static final int CLOUD_NATIVE_TARGET = 3;
    private final TopologyEntity.Builder da1 = createHypervisorTopologyEntity(50001L, "da1", EntityType.DISK_ARRAY, new HashMap<>(), basketSoldByDA);
    private final TopologyEntity.Builder st1 = createHypervisorTopologyEntity(40001L, "st1", EntityType.STORAGE, commBoughtByDSinDA1, basketSoldByDS1);
    private final TopologyEntity.Builder localSt1 = createHypervisorTopologyEntity(40003L, "local_st1", EntityType.STORAGE, commBoughtByDSinDA1, basketSoldByDS1);
    private final TopologyEntity.Builder st2 = createHypervisorTopologyEntity(40002L, "st2", EntityType.STORAGE, commBoughtByDSinDA1, basketSoldByDS2);
    private final TopologyEntity.Builder localSt2 = createHypervisorTopologyEntity(40004L, "local_st2", EntityType.STORAGE, commBoughtByDSinDA1, basketSoldByDS1);
    private final TopologyEntity.Builder dc1 = createHypervisorTopologyEntity(10001L, "dc1", EntityType.DATACENTER, new HashMap<>(), basketSoldByDC1);
    private final TopologyEntity.Builder dc2 = createHypervisorTopologyEntity(10002L, "dc2", EntityType.DATACENTER, new HashMap<>(), basketSoldByDC2);
    private final TopologyEntity.Builder pm1InDc1 = createHypervisorHost(20001L, "pm1InDc1", EntityType.PHYSICAL_MACHINE, commBoughtByPMinDC1,
            basketSoldByPM1inDC1, Arrays.asList(40001L));
    private final TopologyEntity.Builder pm2InDc1 = createHypervisorHost(20002L, "pm2InDc1", EntityType.PHYSICAL_MACHINE, commBoughtByPMinDC1,
            basketSoldByPM2inDC1, Arrays.asList(40001L));
    private final TopologyEntity.Builder pmInDc2 = createHypervisorTopologyEntity(20004L, "pmInDc2", EntityType.PHYSICAL_MACHINE, commBoughtByPMinDC2,
            basketSoldByPMToVMinDC2);
    private final TopologyEntity.Builder virtualVolume = createHypervisorConnectedTopologyEntity(25001L, HYPERVISOR_TARGET, 0, "virtualVolume", EntityType.VIRTUAL_VOLUME, st1.getOid());
    private final TopologyEntity.Builder virtualVolume1 = createHypervisorTopologyEntity(2502L, "virtualVolume1", EntityType.VIRTUAL_VOLUME,
        commBoughtByVVDS1, basketSoldByVV);
    private final TopologyEntity.Builder vm1InDc1 = createHypervisorTopologyEntity(30001L, "vm1InDc1", EntityType.VIRTUAL_MACHINE,
            commBoughtByVMinDC1PM1DS1VDC, basketSoldByVM1, virtualVolume.getOid());
    private final TopologyEntity.Builder vm2InDc1 = createHypervisorTopologyEntity(30002L, "vm2InDc1", EntityType.VIRTUAL_MACHINE,
            commBoughtByVMinDC1PM2DS1VDC, basketSoldByVM2);
    private final TopologyEntity.Builder vm4InDc1 = createHypervisorTopologyEntity(30004L, "vm4InDc1", EntityType.VIRTUAL_MACHINE,
            commBoughtByVMinDC1PM2DS1VDCVV, basketSoldByVM4, st1.getOid());
    private final TopologyEntity.Builder vmInDc2 = createHypervisorTopologyEntity(30003L, "vmInDc2", EntityType.VIRTUAL_MACHINE, commBoughtByVMinDC2PMDS2, basketSoldByVM);
    private final TopologyEntity.Builder appc1 = createHypervisorTopologyEntity(60001L, "appc1", EntityType.APPLICATION_COMPONENT, commBoughtByApp1, new ArrayList<>());
    private final TopologyEntity.Builder as1 = createHypervisorTopologyEntity(70001L, "as1", EntityType.APPLICATION_SERVER, commBoughtByApp1, basketSoldByAS1);
    private final TopologyEntity.Builder as2 = createHypervisorTopologyEntity(70002L, "as2", EntityType.APPLICATION_SERVER, commBoughtByApp2, basketSoldByAS2);
    private final TopologyEntity.Builder bapp1 = createHypervisorTopologyEntity(80001L, "bapp1", EntityType.BUSINESS_APPLICATION, commBoughtByBA, new ArrayList<>());
    private final TopologyEntity.Builder vdcInDc1 = createHypervisorTopologyEntity(100001L, "vdcInDc1", EntityType.VIRTUAL_DATACENTER, commBoughtByVDCinDC1, basketSoldByVDCinDC1);
    private final TopologyEntity.Builder pod1 = createHypervisorTopologyEntity(200001L, "pod1", EntityType.CONTAINER_POD, commBoughtByPod, Arrays.asList(VCPU, VMPM_ACCESS));
    private final TopologyEntity.Builder cntSpec1 = createHypervisorTopologyEntity(200003L, "cntSpec1", EntityType.CONTAINER_SPEC, Collections.emptyMap(), Collections.emptyList());
    private final TopologyEntity.Builder cnt1 = addControlledByConnection(
        createHypervisorTopologyEntity(200002L, "cnt1", EntityType.CONTAINER, commBoughtByCntFromPod, Collections.singletonList(VCPU)),
        cntSpec1.getOid());
    private final TopologyEntity.Builder podVV = createHypervisorTopologyEntity(200004L, "podVV", EntityType.VIRTUAL_VOLUME, Collections.emptyMap(), Collections.singletonList(ST_AMT));

    private static final long VIRTUAL_VOLUME_IN_OHIO_ID = 6001L;
    private static final long VIRTUAL_VOLUME_IN_LONDON_ID = 6002L;
    private static final long VIRTUAL_VOLUME_IN_HONG_KONG_ID = 6003L;
    private static final long VIRTUAL_VOLUME_IN_CENTRAL_US_ID = 6004L;
    private static final long VIRTUAL_VOLUME_IN_CANADA_ID = 6005L;
    private static final long UNATTACHED_VV_IN_CENTRAL_US_ID = 6006L;
    private static final long UNATTACHED_VV_IN_LONDON_ID = 6007L;
    private static final long VIRTUAL_VOLUME_2_IN_CANADA_ID = 6008L;
    private static final long VIRTUAL_VOLUME_IN_US_EAST_ID = 6009L;

    private static final TopologyDTO.CommodityType ZONE_AZ1 = TopologyDTO.CommodityType.newBuilder().setType(CommodityType.ZONE_VALUE).setKey("AZ1 London").build();
    private static final TopologyDTO.CommodityType ZONE_AZ2 = TopologyDTO.CommodityType.newBuilder().setType(CommodityType.ZONE_VALUE).setKey("AZ2 London").build();

    private static final Map<Long, List<TopologyDTO.CommodityType>> commBoughtByKubeVM1inAZ1
            = ImmutableMap.of(
            1001L, Lists.newArrayList(ZONE_AZ1),
            3001L,  Lists.newArrayList(CPU),    //compute tier1
            200004L, Lists.newArrayList(ST_AMT) //VV podVV
    );

    private static final Map<Long, List<TopologyDTO.CommodityType>> commBoughtByKubeVM2inAZ2
            = ImmutableMap.of(
            1002L, Lists.newArrayList(ZONE_AZ2),
            3001L,Lists.newArrayList(CPU)   //compute tier1
    );

    private static final Map<Long, List<TopologyDTO.CommodityType>> commBoughtByKubeVMinAzure
            = ImmutableMap.of(
            3002L,  Lists.newArrayList(CPU),    //compute tier2
            6005L, Lists.newArrayList(ST_AMT)   //VV VIRTUAL_VOLUME_IN_CANADA
    );

    private final TopologyEntity.Builder computeTier
            = createComputeTier(CLOUD_TARGET_1, 3001L, "Compute tier");
    private final TopologyEntity.Builder storageTier = createCloudConnectedTopologyEntity(
            CLOUD_TARGET_1, 7001L, "Storage tier", EntityType.STORAGE_TIER);

    private final TopologyEntity.Builder vm1InLondon = createCloudVm(
            CLOUD_TARGET_1, 4001L, "VM1 in London", VIRTUAL_VOLUME_IN_LONDON_ID);
    private final TopologyEntity.Builder vm2InLondon = createCloudVm(
            CLOUD_TARGET_1, 4002L, "VM2 in London");
    private final TopologyEntity.Builder kubeVm1InLondon = createKubeCloudVm(
            CLOUD_TARGET_1, 40011L, "Kubernetes VM1 in London",
            basketSoldByKubeVM1, commBoughtByKubeVM1inAZ1, VIRTUAL_VOLUME_IN_LONDON_ID);
    private final TopologyEntity.Builder kubeVm2InLondon = createKubeCloudVm(
            CLOUD_TARGET_1, 40012L, "Kubernetes VM2 in London",
            basketSoldByKubeVM2, commBoughtByKubeVM2inAZ2);

    private final TopologyEntity.Builder vmInOhio = createCloudVm(
            CLOUD_TARGET_1, 4003L, "VM in Ohio", VIRTUAL_VOLUME_IN_OHIO_ID);
    private final TopologyEntity.Builder vmInHongKong = createCloudVm(
            CLOUD_TARGET_1, 4004L, "VM in Hong Kong", VIRTUAL_VOLUME_IN_HONG_KONG_ID);
    private final TopologyEntity.Builder vmInUSEast = createCloudVm(
            CLOUD_TARGET_1, 4007L, "VM in US East", basketSoldByVMInUSEast, VIRTUAL_VOLUME_IN_US_EAST_ID);

    private final TopologyEntity.Builder appAws =
            createCloudTopologyEntity(900000L, CLOUD_TARGET_1, 0,
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
    private final TopologyEntity.Builder vvInUSEast = createCloudVolume(
            CLOUD_TARGET_1, VIRTUAL_VOLUME_IN_US_EAST_ID, "Virtual Volume in US East",
            storageTier.getOid());

    private final TopologyEntity.Builder az1London = createCloudTopologyAvailabilityZone(
            CLOUD_TARGET_1, 1001L, "AZ1 London",
            dbsLondon, vm1InLondon, kubeVm1InLondon, virtualVolumeInLondon, unattachedVirtualVolumeInLondon);
    private final TopologyEntity.Builder az2London = createCloudTopologyAvailabilityZone(
            CLOUD_TARGET_1, 1002L, "AZ2 London",
            dbLondon, vm2InLondon, kubeVm2InLondon);
    private final TopologyEntity.Builder azOhio = createCloudTopologyAvailabilityZone(
            CLOUD_TARGET_1, 1003L, "AZ Ohio",
            vmInOhio, virtualVolumeInOhio);
    private final TopologyEntity.Builder az1HongKong = createCloudTopologyAvailabilityZone(
            CLOUD_TARGET_1, 1004L, "AZ1 Hong Kong",
            vmInHongKong, virtualVolumeInHongKong);
    private final TopologyEntity.Builder az2HongKong = createCloudTopologyAvailabilityZone(
            CLOUD_TARGET_1, 1005L, "AZ2 Hong Kong",
            dbsHongKong);
    private final TopologyEntity.Builder azUSEast = createCloudTopologyAvailabilityZone(
            CLOUD_TARGET_1, 1006L, "AZ US East",
            vmInUSEast, vvInUSEast);

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
    private final TopologyEntity.Builder regionUSEast = createRegion(
            CLOUD_TARGET_1, 2006L, "US East",
            Collections.singleton(azUSEast),
            ImmutableList.of(computeTier, storageTier));

    private final TopologyEntity.Builder computeTier2 = createCloudConnectedTopologyEntity(
            CLOUD_TARGET_2, 3002L, "Compute tier 2", EntityType.COMPUTE_TIER);
    private final TopologyEntity.Builder storageTier2 = createCloudConnectedTopologyEntity(
            CLOUD_TARGET_2, 7002L, "Storage tier 2", EntityType.STORAGE_TIER);

    private final TopologyEntity.Builder vmInCentralUs = createCloudVm(
            CLOUD_TARGET_2, 4005L, "VM in Central US", VIRTUAL_VOLUME_IN_CENTRAL_US_ID);
    private final TopologyEntity.Builder vmInCanada = createCloudVm(
            CLOUD_TARGET_2, 4006L, "VM in Canada", VIRTUAL_VOLUME_IN_CANADA_ID,
            VIRTUAL_VOLUME_2_IN_CANADA_ID);
    private final TopologyEntity.Builder kubeVmInCanada = createKubeCloudVm(
            CLOUD_TARGET_1, 40061L, "Kubernetes VM in Canada",
            basketSoldByKubeVMinAzure, commBoughtByKubeVMinAzure, VIRTUAL_VOLUME_IN_CANADA_ID);
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
            businessAcc3, vm1InLondon, kubeVm1InLondon, kubeVm2InLondon, virtualVolumeInLondon, appAws);

    private final TopologyEntity.Builder appAzure =
            createCloudTopologyEntity(900001L, CLOUD_TARGET_2, 0,
                                               EntityType.APPLICATION_COMPONENT, vmInCanada.getOid());

    private final TopologyEntity.Builder regionCentralUs = createRegion(
            CLOUD_TARGET_2, 2004L, "Central US",
            Collections.emptySet(),
            ImmutableList.of(dbCentralUs, dbsCentralUs, vmInCentralUs, virtualVolumeInCentralUs,
                             computeTier2, storageTier2, unattachedVirtualVolumeInCentralUs));
    private final TopologyEntity.Builder regionCanada = createRegion(
            CLOUD_TARGET_2, 2005L, "Canada",
            Collections.emptySet(),
            ImmutableList.of(computeTier2, storageTier2, vmInCanada, kubeVmInCanada,
                    virtualVolumeInCanada, virtualVolume2InCanada));

    private final TopologyEntity.Builder businessAcc4 = createOwner(
            CLOUD_TARGET_2, 5004L, "Business account 4", EntityType.BUSINESS_ACCOUNT,
            vmInCanada, kubeVmInCanada, virtualVolumeInCanada, appAzure);
    private final TopologyEntity.Builder cloudService =
            createOwner(CLOUD_TARGET_2, 10000L, "Cloud service 1",
                        EntityType.CLOUD_SERVICE, computeTier2);

    // --- Container Platform entities - Kubernetes cluster in AWS
    private static final TopologyDTO.CommodityType VCPUREQ_QUOTA_CNTRLLER_SOLD
            = TopologyDTO.CommodityType.newBuilder().setType(CommodityType.VCPU_REQUEST_QUOTA_VALUE).setKey("wrkContrller1").build();
    private static final TopologyDTO.CommodityType VCPUREQ_QUOTA_CNTRLLER_BOUGHT
            = TopologyDTO.CommodityType.newBuilder().setType(CommodityType.VCPU_REQUEST_QUOTA_VALUE).setKey("ns1").build();
    private static final TopologyDTO.CommodityType VCPUREQ_QUOTA_NAMESPACE_SOLD
            = TopologyDTO.CommodityType.newBuilder().setType(CommodityType.VCPU_REQUEST_QUOTA_VALUE).setKey("ns1").build();

    private static final Map<Long, List<TopologyDTO.CommodityType>> commBoughtByWorkloadController
            = ImmutableMap.of(
            22L, Lists.newArrayList(VCPUREQ_QUOTA_CNTRLLER_BOUGHT) // namespace
            );

    private static final Map<Long, List<TopologyDTO.CommodityType>> commBoughtByNamespace
            = ImmutableMap.of(
            21L, Lists.newArrayList(CLUSTER_COMM_AWS) // container cluster
    );

    private static final Map<Long, List<TopologyDTO.CommodityType>> commBoughtByKubePod
            = ImmutableMap.of(
            40011L, Lists.newArrayList(VCPU, CLUSTER_COMM_AWS), //VM
            23L, Lists.newArrayList(VCPUREQ_QUOTA_CNTRLLER_SOLD), // Workload controller
            27L, Lists.newArrayList(ST_AMT) //Volume
    );

    private static final Map<Long, List<TopologyDTO.CommodityType>> commBoughtByCnt
            = ImmutableMap.of(
            25L, Lists.newArrayList(VCPU, VCPUREQ_QUOTA, VMPM_ACCESS_KUBEPOD)   // pod
    );

    private final TopologyEntity.Builder kubeCntSpec1
            = createCloudNativeTopologyEntity(24L, "kubeCntSpec1", EntityType.CONTAINER_SPEC,
                                                Collections.emptyMap(), Collections.emptyList());

    private final TopologyEntity.Builder wrkContrller1
            = createWorkloadControllers(CLOUD_NATIVE_TARGET, 23L, "wrkContrller1",
                                            commBoughtByWorkloadController,
                                                Arrays.asList(VCPU, VCPUREQ_QUOTA_CNTRLLER_SOLD),
                                                Arrays.asList(kubeCntSpec1));

    private final TopologyEntity.Builder namespace1
            = createNamespace(CLOUD_NATIVE_TARGET, 22L, "ns1",
                                        commBoughtByNamespace,
                                        Arrays.asList(VCPU, VCPUREQ_QUOTA_NAMESPACE_SOLD),
                                        Arrays.asList(wrkContrller1));

    private final TopologyEntity.Builder cntCluster1
            = createContainerCluster(CLOUD_NATIVE_TARGET, 21L, "cntCluster1",
                                        Arrays.asList(CLUSTER_COMM_AWS),
                                        Arrays.asList(namespace1),
                                        Arrays.asList(kubeVm1InLondon, kubeVm2InLondon));

    private final TopologyEntity.Builder kubePod1
            = createCloudNativeTopologyEntity(25L, "kubePod1", EntityType.CONTAINER_POD,
                                                commBoughtByKubePod, Arrays.asList(VCPU, VCPUREQ_QUOTA, VMPM_ACCESS_KUBEPOD));

    private final TopologyEntity.Builder kubeCnt1 = addControlledByConnection(
            createCloudNativeTopologyEntity(26L, "kubeCnt1", EntityType.CONTAINER,
                                            commBoughtByCnt, Collections.singletonList(VCPU)),
                                            kubeCntSpec1.getOid());
    private final TopologyEntity.Builder kubePodVV1
            = createCloudNativeTopologyEntity(27L, "kubePodVV1", EntityType.VIRTUAL_VOLUME,
                                                Collections.emptyMap(),
                                                Collections.singletonList(ST_AMT));

    // --- Container Platform entities - Kubernetes cluster in Azure
    private static final TopologyDTO.CommodityType VCPUREQ_QUOTA_CNTRLLER_SOLD_Azure
            = TopologyDTO.CommodityType.newBuilder().setType(CommodityType.VCPU_REQUEST_QUOTA_VALUE).setKey("wrkContrller-azuure").build();
    private static final TopologyDTO.CommodityType VCPUREQ_QUOTA_CNTRLLER_BOUGHT_Azure
            = TopologyDTO.CommodityType.newBuilder().setType(CommodityType.VCPU_REQUEST_QUOTA_VALUE).setKey("ns-azure").build();
    private static final TopologyDTO.CommodityType VCPUREQ_QUOTA_NAMESPACE_SOLD_Azure
            = TopologyDTO.CommodityType.newBuilder().setType(CommodityType.VCPU_REQUEST_QUOTA_VALUE).setKey("nsa-azure").build();

    private static final Map<Long, List<TopologyDTO.CommodityType>> commBoughtByWorkloadControllerInAzure
            = ImmutableMap.of(
            32L, Lists.newArrayList(VCPUREQ_QUOTA_CNTRLLER_BOUGHT_Azure) // namespace
    );

    private static final Map<Long, List<TopologyDTO.CommodityType>> commBoughtByNamespaceInAzure
            = ImmutableMap.of(
            31L, Lists.newArrayList(CLUSTER_COMM_AZURE) // container cluster
    );

    private static final Map<Long, List<TopologyDTO.CommodityType>> commBoughtByKubePodInAzure
            = ImmutableMap.of(
            40061L, Lists.newArrayList(VCPU, CLUSTER_COMM_AZURE), //VM - KubeVMInAzure
            33L, Lists.newArrayList(VCPUREQ_QUOTA_CNTRLLER_SOLD_Azure), // Workload controller Azure
            37L, Lists.newArrayList(ST_AMT) //Volume
    );

    private static final Map<Long, List<TopologyDTO.CommodityType>> commBoughtByCntInAzure
            = ImmutableMap.of(
            35L, Lists.newArrayList(VCPU, VCPUREQ_QUOTA_AZURE, VMPM_ACCESS_KUBEPOD_AZURE)   // pod
    );

    private final TopologyEntity.Builder kubeCntSpecInAzure
            = createCloudNativeTopologyEntity(34L, "kubeCntSpecInAzure", EntityType.CONTAINER_SPEC,
            Collections.emptyMap(), Collections.emptyList());

    private final TopologyEntity.Builder wrkContrllerInAzure
            = createWorkloadControllers(CLOUD_NATIVE_TARGET, 33L, "wrkContrllerInAzure",
            commBoughtByWorkloadControllerInAzure,
            Arrays.asList(VCPU, VCPUREQ_QUOTA_CNTRLLER_SOLD_Azure),
            Arrays.asList(kubeCntSpecInAzure));

    private final TopologyEntity.Builder namespaceInAzure
            = createNamespace(CLOUD_NATIVE_TARGET, 32L, "nsInAzure",
            commBoughtByNamespaceInAzure,
            Arrays.asList(VCPU, VCPUREQ_QUOTA_NAMESPACE_SOLD_Azure),
            Arrays.asList(wrkContrllerInAzure));

    private final TopologyEntity.Builder cntClusterInAzure
            = createContainerCluster(CLOUD_NATIVE_TARGET, 31L, "cntClusterInAzure",
            Arrays.asList(CLUSTER_COMM_AZURE),
            Arrays.asList(namespaceInAzure),
            Arrays.asList(kubeVmInCanada));

    private final TopologyEntity.Builder kubePodInAzure
            = createCloudNativeTopologyEntity(35L, "kubePodInAzure", EntityType.CONTAINER_POD,
            commBoughtByKubePodInAzure, Arrays.asList(VCPU, VCPUREQ_QUOTA_AZURE, VMPM_ACCESS_KUBEPOD_AZURE));

    private final TopologyEntity.Builder kubeCntInAzure = addAggregatedByConnection(
            createCloudNativeTopologyEntity(36L, "kubeCntInAzure", EntityType.CONTAINER,
                    commBoughtByCntInAzure, Collections.singletonList(VCPU)),
            kubeCntSpec1.getOid());
    private final TopologyEntity.Builder kubePodVVInAzure
            = createCloudNativeTopologyEntity(37L, "kubePodVVInAzure", EntityType.VIRTUAL_VOLUME,
            Collections.emptyMap(),
            Collections.singletonList(ST_AMT));


    private final TopologyEntity.Builder bt = createHypervisorTopologyEntity(90001L, "bt", EntityType.BUSINESS_TRANSACTION, commBoughtByBT, new ArrayList<>());
    private final TopologyEntity.Builder s1 = createHypervisorTopologyEntity(90002L, "s1", EntityType.SERVICE, commBoughtByService1, basketSoldByVMInUSEast);
    private final TopologyEntity.Builder s2 = createHypervisorTopologyEntity(90003L, "s2", EntityType.SERVICE, commBoughtByService2, Collections.singletonList(APP1));

    /* Creating container topology in Kubernetes cluster in AWS
                                                       kubeCnt1 --- kubeCntSpec1 --- wrkController1
                                                        /                                |
                                           kubePodVV1 -- kubePod1                   namespace1
                                                        /                                |
                                      businessAcc1     /                                 |
                                    /             \   /                                  |
                                kubeVM2          kubeVM1                           cntCluster1
                                     \              /
                                    AZ2           AZ1
                                       \         /
                                       Region-London --- [computeTier, storageTier]
     */

     /* Creating container topology in Kubernetes cluster in Azure
                                                       kubeCnt --- kubeCntSpec --- wrkController
                                                        /                                |
                                           kubePodVV -- kubePod                   namespace
                                                        /                                |
                                      businessAcc4     /                                 |
                                              \       /                                  |
                                                kubeVMInCanada                       cntCluster

                                                    |
                                             Region-Canada --- [computeTier, storageTier]
     */

    /* Creating an on prem topology.

                      bt                   ba
                     /  \               /     \
                    s1   s2            /       \       cnt1--cntSpec1
                   /      \ appc1     /         \      /
                  /        \  \    as1         as2   pod1
              vmInUSEast    \  \  /              |  /    \
              /      \         vm1               vm3     podVV
       azUSEast   vvInUSEas   / \               / \
                            pm1  vv            pm3  st2
                            /     \            |      |
                         dc1     st1-da1      dc2     |
                          |       /    \             /
                         pm2    /        -----------
                           \   /
                            vm2
     NOTE: there is a VDC(not shown in graph) buying cpu allocation from pm1 and pm2, selling to vm1 and vm2.

     */
    private final TopologyGraph<TopologyEntity> emptyClusterGraph = TopologyEntityUtils
        .topologyGraphOf(
            TopologyEntity.newBuilder(copyAndAddLocalStorageAccesses(pm1InDc1, localSt1.getOid()).getEntityBuilder()),
            TopologyEntity.newBuilder(copyAndAddLocalStorageAccesses(pm2InDc1, localSt2.getOid()).getEntityBuilder()),
            TopologyEntity.newBuilder(pmInDc2.getEntityBuilder()),
            TopologyEntity.newBuilder(dc1.getEntityBuilder()),
            TopologyEntity.newBuilder(dc2.getEntityBuilder()),
            TopologyEntity.newBuilder(st1.getEntityBuilder()),
            TopologyEntity.newBuilder(st2.getEntityBuilder()),
            TopologyEntity.newBuilder(da1.getEntityBuilder()),
            TopologyEntity.newBuilder(vdcInDc1.getEntityBuilder()),
            TopologyEntity.newBuilder(localSt1.getEntityBuilder()),
            TopologyEntity.newBuilder(localSt2.getEntityBuilder()));

    private TopologyEntity.Builder copyAndAddLocalStorageAccesses(TopologyEntity.Builder entity, long accesses) {
        TopologyEntityDTO.Builder copy = entity.getEntityBuilder().build().toBuilder();
        copy.addCommoditySoldList(CommoditySoldDTO.newBuilder().setCommodityType(DS1).setAccesses(accesses));
        return TopologyEntity.newBuilder(copy);
    }

    private TopologyGraph<TopologyEntity> graph;

    private final Set<TopologyEntity.Builder> expectedEntitiesForAwsRegion = Stream
        .of(az1London, az2London, regionLondon, computeTier,
                vm1InLondon, vm2InLondon, kubeVm1InLondon, kubeVm2InLondon, dbLondon, dbsLondon, businessAcc1,
                virtualVolumeInLondon, storageTier, appAws,
                unattachedVirtualVolumeInLondon, businessAcc2, businessAcc3, businessAcc4)
        .collect(Collectors.collectingAndThen(Collectors.toSet(),
            Collections::unmodifiableSet));

    private final Set<TopologyEntity.Builder> expectedEntitiesForAzureRegion = Stream
                    .of(regionCentralUs, dbCentralUs, dbsCentralUs, computeTier2,
                            storageTier2, virtualVolumeInCentralUs, vmInCentralUs, cloudService,
                            unattachedVirtualVolumeInCentralUs, businessAcc1, businessAcc2,
                            businessAcc3, businessAcc4)
                    .collect(Collectors.collectingAndThen(Collectors.toSet(),
                        Collections::unmodifiableSet));

    private final Set<TopologyEntity.Builder> expectedEntitiesForRegionsList = Stream
        .of(az1London, az2London, azOhio, regionLondon, regionOhio, computeTier,
                vm1InLondon, vm2InLondon, kubeVm1InLondon, kubeVm2InLondon, vmInOhio, dbLondon, dbsLondon, businessAcc1,
                businessAcc2, storageTier, virtualVolumeInLondon, virtualVolumeInOhio, appAws,
                unattachedVirtualVolumeInLondon, businessAcc3, businessAcc4)
        .collect(Collectors.collectingAndThen(Collectors.toSet(),
            Collections::unmodifiableSet));

    private final Set<TopologyEntity.Builder> expectedEntitiesForBusinessAccount = Stream
                    .of(az1HongKong, vmInHongKong, storageTier, regionHongKong,
                            businessAcc1, businessAcc3, virtualVolumeInHongKong, businessAcc2,
                            businessAcc4)
                    .collect(Collectors.collectingAndThen(Collectors.toSet(),
                                                          Collections::unmodifiableSet));

    private final Set<TopologyEntity.Builder> expectedEntitiesForBusinessAccountsList = Stream
                    .of(azOhio, vmInOhio, businessAcc1, computeTier, storageTier,
                            businessAcc2, businessAcc3, az1HongKong, vmInHongKong,
                            regionOhio, regionHongKong, virtualVolumeInHongKong, virtualVolumeInOhio,
                            businessAcc4)
                    .collect(Collectors.collectingAndThen(Collectors.toSet(),
                                                          Collections::unmodifiableSet));

    private final Set<TopologyEntity.Builder> expectedEntitiesForBillingFamily = Stream
                    .of(az1London, vm1InLondon,  computeTier, storageTier,
                            businessAcc1, businessAcc3, az1HongKong, vmInHongKong,
                            regionLondon, regionHongKong, virtualVolumeInHongKong,
                            virtualVolumeInLondon, appAws, businessAcc2, businessAcc4,
                            kubeVm1InLondon, kubeVm2InLondon, az2London)
                    .collect(Collectors.collectingAndThen(Collectors.toSet(),
                                                          Collections::unmodifiableSet));

    private final Set<TopologyEntity.Builder> expectedEntitiesForAwsVm = Stream
                    .of(az1London, regionLondon, vm1InLondon, kubeVm1InLondon, businessAcc1,
                            computeTier, virtualVolumeInLondon, storageTier, appAws, businessAcc2,
                            businessAcc3, businessAcc4)
                    .collect(Collectors.collectingAndThen(Collectors.toSet(),
                                                          Collections::unmodifiableSet));

    private final Set<TopologyEntity.Builder> expectedEntitiesForAzureDb = Stream
                    .of(dbCentralUs, regionCentralUs, computeTier2, storageTier2, cloudService,
                            businessAcc1, businessAcc2, businessAcc3, businessAcc4)
                    .collect(Collectors.collectingAndThen(Collectors.toSet(),
                                                          Collections::unmodifiableSet));

    private final Set<TopologyEntity.Builder> expectedEntitiesForAwsDbsGroup = Stream
                    .of(az1London, regionLondon, dbsLondon, computeTier, storageTier,
                            dbsHongKong, az2HongKong, regionHongKong, businessAcc1, businessAcc2,
                            businessAcc3, businessAcc4)
                    .collect(Collectors.collectingAndThen(Collectors.toSet(),
                                                          Collections::unmodifiableSet));

    private final Set<TopologyEntity.Builder> expectedEntitiesForResourceGroup = Stream
                    .of(dbCentralUs, regionCentralUs, computeTier2, storageTier2,
                            virtualVolumeInCanada, regionCanada, vmInCanada, kubeVmInCanada, businessAcc4,
                            cloudService, appAzure, businessAcc1, businessAcc2, businessAcc3)
                    .collect(Collectors.collectingAndThen(Collectors.toSet(),
                                                          Collections::unmodifiableSet));

    private final Set<TopologyEntity.Builder> expectedEntitiesForVolumesGroup = Stream
            .of(regionCentralUs, computeTier2, storageTier2, unattachedVirtualVolumeInCentralUs,
                    virtualVolume2InCanada, regionCanada, vmInCanada, businessAcc4,
                    cloudService, appAzure, businessAcc1, businessAcc2, businessAcc3)
            .collect(Collectors.collectingAndThen(Collectors.toSet(),
                    Collections::unmodifiableSet));

    private final GroupResolver groupResolver = mock(GroupResolver.class);
    private PlanTopologyScopeEditor planTopologyScopeEditor;
    private final GroupServiceMole groupServiceClient = spy(new GroupServiceMole());

    final TopologyPipelineContext context = mock(TopologyPipelineContext.class);

    PlanTopologyInfo.Builder migrationPlanInfo = PlanTopologyInfo.newBuilder()
            .setPlanProjectType(PlanProjectType.CLOUD_MIGRATION)
            .setPlanType("MIGRATE_TO_CLOUD");
    PlanTopologyInfo.Builder optimizePlanInfo = PlanTopologyInfo.newBuilder()
            .setPlanProjectType(PlanProjectType.USER)
            .setPlanType("OPTIMIZE_CLOUD");
    PlanTopologyInfo.Builder optimizeContainerInfo = PlanTopologyInfo.newBuilder()
            .setPlanProjectType(PlanProjectType.USER)
            .setPlanType("OPTIMIZE_CONTAINER_CLUSTER");
    PlanTopologyInfo.Builder onPremInfo = PlanTopologyInfo.newBuilder()
            .setPlanProjectType(PlanProjectType.USER)
            .setPlanType("ON_PREM");

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(groupServiceClient);

    final TopologyInfo onPremTopologyInfo = TopologyInfo.newBuilder()
            .setTopologyContextId(1)
            .setTopologyId(1)
            .setTopologyType(TopologyType.PLAN)
            .setPlanInfo(onPremInfo)
            .build();

    final TopologyInfo containerClusterTopologyInfo = TopologyInfo.newBuilder()
            .setTopologyContextId(1)
            .setTopologyId(1)
            .setTopologyType(TopologyType.PLAN)
            .setPlanInfo(optimizeContainerInfo)
            .build();

    @Before
    public void setup() {
        // Mark the pod as not movable on its volume provider and container not movable on its pod
        pod1.getEntityBuilder().getCommoditiesBoughtFromProvidersBuilderList().stream()
            .filter(bought -> bought.getProviderId() == podVV.getOid())
            .findAny()
            .ifPresent(bought -> bought.setMovable(false));

        planTopologyScopeEditor = new PlanTopologyScopeEditor(GroupServiceGrpc
            .newBlockingStub(grpcServer.getChannel()), true);
        graph = TopologyEntityUtils
            .topologyGraphOf(bt, s1, s2, vmInUSEast, azUSEast, vvInUSEast, regionUSEast,
                bapp1, appc1, vm1InDc1, vm2InDc1, vm4InDc1, virtualVolume1, vmInDc2, virtualVolume, vdcInDc1, pm1InDc1,
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
                unattachedVirtualVolumeInLondon, virtualVolume2InCanada, pod1, cnt1, cntSpec1, podVV,
                    kubeVm1InLondon, kubeVm2InLondon, kubePod1, kubePodVV1, kubeCnt1,
                    kubeCntSpec1, wrkContrller1, namespace1, cntCluster1,
                    kubeVmInCanada, kubePodInAzure, kubePodVVInAzure, kubeCntInAzure,
                    kubeCntSpecInAzure, wrkContrllerInAzure, namespaceInAzure, cntClusterInAzure);
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
     * Tests scope cloud topology for the plan scope with group of 2 DBS.
     * Topology graph contains entities for 3 targets: hypervisor and 2 clouds.
     * expectedEntitiesForAwsDbsGroup - set of cloud entities expected as result of applying plan
     * scope to the topology.
     *
     * <p>This is similar to the above test, except that this is a migration plan, so the database
     * servers should not be present in the topology.
     */
    @Test
    public void testScopeMigrateCloudTopologyForDBSGroup() {
        // DBS in London and DBS in Hong Kong
        final List<Long> oidsList = Arrays.asList(9001L, 9002L);
        final Set<TopologyEntity.Builder> expectedEntitiesWithoutDBS =
                expectedEntitiesForAwsDbsGroup.stream()
                        .filter(e -> EntityType.DATABASE_SERVER_VALUE != e.getEntityType())
                        .collect(Collectors.toSet());
        testScopeCloudTopology(oidsList, expectedEntitiesWithoutDBS, true);
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

    private void testScopeCloudTopology(List<Long> oidsList,
            Set<TopologyEntity.Builder> expectedEntities,
            boolean isMigrationPlan) {
        final TopologyInfo cloudTopologyInfo = TopologyInfo.newBuilder()
                        .setTopologyContextId(1)
                        .setTopologyId(1)
                        .setTopologyType(TopologyType.PLAN)
                        .setPlanInfo(isMigrationPlan
                                ? migrationPlanInfo
                                : optimizePlanInfo)
                        .addAllScopeSeedOids(oidsList)
                        .build();
        final TopologyGraph<TopologyEntity> result = planTopologyScopeEditor.scopeTopology(
                cloudTopologyInfo, graph, new HashSet<>());
        Assert.assertEquals(expectedEntities.size(), result.size());
        expectedEntities.forEach(entity -> assertTrue(entity.getOid() + " is missing", result.getEntity(entity.getOid())
                        .isPresent()));
    }

    private void testScopeCloudTopology(List<Long> oidsList,
            Set<TopologyEntity.Builder> expectedEntities) {
        testScopeCloudTopology(oidsList, expectedEntities, false);
    }

    /**
     * Scenario: scope on cluster 1 with only contains pm1.
     * Expected: the workloads in scope should have vm1 not vm2.
     *
     * @throws Exception An exception thrown when a stage of the pipeline fails.
     */
    @Test
    public void testScopeOnpremTopologyWithVDCAcrossClusters() throws Exception {
        // group only contains pm1 in it
        Grouping g = Grouping.newBuilder()
                .addExpectedTypes(MemberType.newBuilder().setEntity(ApiEntityType.PHYSICAL_MACHINE.typeNumber()))
                .setDefinition(GroupDefinition.newBuilder()
                        .setStaticGroupMembers(StaticMembers.newBuilder()
                                .addMembersByType(StaticMembersByType.newBuilder()
                                        .setType(MemberType.newBuilder()
                                                .setEntity(ApiEntityType.PHYSICAL_MACHINE.typeNumber()))
                                        .addMembers(pm1InDc1.getOid())
                                )))
                .build();

        List<Grouping> groups = Arrays.asList(g);
        when(groupServiceClient.getGroups(GetGroupsRequest.newBuilder()
                .setGroupFilter(GroupFilter.newBuilder().addId(25001L))
                .setReplaceGroupPropertyWithGroupMembershipFilter(true)
                .build())).thenReturn(groups);
        when(groupResolver.resolve(eq(g), eq(graph))).thenReturn(
                new ResolvedGroup(g, Collections.singletonMap(ApiEntityType.PHYSICAL_MACHINE,
                        Sets.newHashSet(pm1InDc1.getOid()))));

        final PlanScope planScope = PlanScope.newBuilder()
                .addScopeEntries(PlanScopeEntry.newBuilder().setClassName("Cluster")
                        .setScopeObjectOid(25001L).setDisplayName("PM1 cluster/dc1").build()).build();
        // populate InvertedIndex
        InvertedIndex<TopologyEntity, TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider>
                index = planTopologyScopeEditor.createInvertedIndex();
        graph.entities().forEach(entity -> index.add(entity));
        // scope using inverted index
        TopologyGraph<TopologyEntity> result = planTopologyScopeEditor
                .indexBasedScoping(index, onPremTopologyInfo, graph, groupResolver, planScope, PlanProjectType.USER);
        Set<Long> vms = result.entities().filter(e -> e.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE)
                .map(TopologyEntity::getOid).collect(
                Collectors.toSet());
        assertTrue(vms.contains(30001L));
        assertFalse(vms.contains(30002L));
    }

    /**
     * Scenario: scope on pm1 and pm2 which consumes on dc1.
     * Expected: the entities in scope should be vm1, vm2, pm1, pm2, vdc, dc1, vv, st1, da1, appc1, as1, ba, bt, s2
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
                .indexBasedScoping(index, onPremTopologyInfo, graph, groupResolver, planScope, PlanProjectType.USER);
        assertEquals(16, result.size());
    }

    /**
     * Scenario: scope on pm1 and pm2 which consumes on dc1.
     * Expected: the entities in scope should be vm1, vm2, pm1, pm2, vdc, dc1, vv, st1, da1, appc1, as1, ba, s2, bt
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
                .indexBasedScoping(index, onPremTopologyInfo, graph, groupResolver, planScope, PlanProjectType.USER);
        assertEquals(16, result.size());
    }

    private static TopologyEntity.Builder createHypervisorHost(long oid,
                                                               String displayName,
                                                               EntityType entityType,
                                                               Map<Long, Pair<Integer, List<TopologyDTO.CommodityType>>> producers,
                                                               List<TopologyDTO.CommodityType> soldComms,
                                                               List<Long> connectedStorages,
                                                               long... connectedEntities) {
        TopologyEntity.Builder entity = createHypervisorTopologyEntity(oid, displayName,
            entityType, producers, soldComms, connectedEntities);
        connectedStorages.forEach(st -> entity.getEntityBuilder().addCommoditySoldList(
            CommoditySoldDTO.newBuilder().setCommodityType(DATASTORE).setAccesses(st)));
        entity.getEntityBuilder().setEnvironmentType(EnvironmentType.ON_PREM);
        return entity;
    }

    /**
     * Scenario: scope on ba which consumes as1 and as2.
     * Expected: the entities in scope should be ba, as1, vm1, vm2, pm1, pm2, dc1, vv, st1,
     * da1, as2, vm3, pm3, dc2, st2, s2
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
                .indexBasedScoping(index, onPremTopologyInfo, graph, groupResolver, planScope, PlanProjectType.USER);
        // THE APP THAT IS NOT CONNECTED TO BAPP WILL NOT GET PULLED INTO THE SCOPE
        assertEquals(16, result.size());
    }

    /**
     * Scenario: scope on st2 which hosts vm on dc2.
     * Expected: the entities in scope should be ba, as2, vm3, pm3, st2, dc2, da1, pod1, cnt1, cntSpec1
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
                .indexBasedScoping(index, onPremTopologyInfo, graph, groupResolver, planScope, PlanProjectType.USER);

        assertEquals(11, result.size());
        final List<Long> resultOids = result.entities().map(TopologyEntity::getOid).collect(Collectors.toList());
        final List<Long> expected = Stream.of(bapp1, as2, vmInDc2, pmInDc2, st2, dc2, da1, pod1, cnt1, cntSpec1, podVV)
            .map(Builder::getOid)
            .collect(Collectors.toList());
        assertThat(resultOids, containsInAnyOrder(expected.toArray()));
    }

    /**
     * Scenario: scope on pm2.
     * Expected: pm2InDc1, vm2InDc1, vm4InDc1, st1, virtualVolume, virtualVolume1, vdcInDc1, dc1, da1
     *
     * @throws Exception An exception thrown when a stage of the pipeline fails.
     */
    @Test
    public void testScopeOnpremTopologyOnPM() throws Exception {
        final PlanScope planScope = PlanScope.newBuilder()
            .addScopeEntries(PlanScopeEntry.newBuilder()
                .setScopeObjectOid(pm2InDc1.getOid())).build();
        // populate InvertedIndex
        InvertedIndex<TopologyEntity, TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider>
            index = planTopologyScopeEditor.createInvertedIndex();
        graph.entities().forEach(index::add);
        // scope using inverted index
        TopologyGraph<TopologyEntity> result = planTopologyScopeEditor
            .indexBasedScoping(index, onPremTopologyInfo, graph, groupResolver, planScope, PlanProjectType.USER);

        assertEquals(9, result.size());
        final List<Long> resultOids = result.entities().map(TopologyEntity::getOid).collect(Collectors.toList());
        final List<Long> expected = Stream.of(pm2InDc1, vm2InDc1, vm4InDc1, st1, virtualVolume, virtualVolume1, vdcInDc1, dc1, da1)
            .map(Builder::getOid)
            .collect(Collectors.toList());
        assertThat(resultOids, containsInAnyOrder(expected.toArray()));
    }

    /**
     * Scenario: scope on vm2 which consumes pm2 on dc1, st1 on da1. The vm2 hosts no application at all.
     * Expected: the entities in scope should be dc1, da1, pm1, pm2, vdc, st1, vm2, and virtualVolume
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
                .indexBasedScoping(index, onPremTopologyInfo, graph, groupResolver, planScope, PlanProjectType.USER);
        result.entities().forEach(e -> System.out.println(e.getOid() + " "));
        assertEquals(8, result.size());

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
                .indexBasedScoping(index, onPremTopologyInfo, graphWithClone, groupResolver, planScope, PlanProjectType.USER);

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
                .indexBasedScoping(index, onPremTopologyInfo, graphWithClone, groupResolver, planScope, PlanProjectType.USER);

        result.entities().forEach(e -> System.out.println(e.getOid() + " "));
        assertEquals(2, result.size());
        // Make sure clone is in scope and it still references original OID.
        assertTrue(result.getEntity(cloneOid).isPresent());
        assertTrue(result.getEntity(cloneOid).get().getClonedFromEntity().isPresent());
        assertEquals(vm1InDc1.getOid(), result.getEntity(cloneOid).get().getClonedFromEntity().get().getOid());
    }

    @Test
    public void testScopeOnCloudContainerCluster() throws Exception {
        final PlanScope planScope = PlanScope.newBuilder()
                .addScopeEntries(PlanScopeEntry.newBuilder().setClassName("ContainerPlatformCluster")
                        .setScopeObjectOid(cntCluster1.getOid()).setDisplayName("cntCluster1").build()).build();
        // populate InvertedIndex
        InvertedIndex<TopologyEntity, TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider>
                index = planTopologyScopeEditor.createInvertedIndex();
        graph.entities().forEach(
                entity -> index.add(entity)
        );
        // scope using inverted index
        TopologyGraph<TopologyEntity> result = planTopologyScopeEditor
                .indexBasedScoping(index, containerClusterTopologyInfo, graph, groupResolver, planScope, PlanProjectType.USER);
        result.entities().forEach(e -> System.out.println(e.getOid() + " " + e.getDisplayName() + " "));

        List<String> expectedEntities = Arrays.asList("cntCluster1", "ns1", "wrkContrller1", "kubeCntSpec1",
                "kubePod1", "kubeCnt1", "Compute tier",
                 "Business account 1", "London", "Storage tier",
                "AZ1 London", "AZ2 London", "Kubernetes VM2 in London",  "Kubernetes VM1 in London");

        assertEquals(expectedEntities.size(), result.size());
        assertEquals(1, result.entitiesOfType(EntityType.BUSINESS_ACCOUNT).count());
        assertEquals(1, result.entitiesOfType(EntityType.REGION).count());
        assertEquals(1, result.entitiesOfType(EntityType.COMPUTE_TIER).count());
        assertEquals(1, result.entitiesOfType(EntityType.STORAGE_TIER).count());
    }

    @Test
    public void testScopeOnCloudContainerClusterWithAzureVMs() throws Exception {
        final PlanScope planScope = PlanScope.newBuilder()
                .addScopeEntries(PlanScopeEntry.newBuilder().setClassName("ContainerPlatformCluster")
                        .setScopeObjectOid(cntClusterInAzure.getOid()).setDisplayName("cntClusterInAzure").build()).build();
        // populate InvertedIndex
        InvertedIndex<TopologyEntity, TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider>
                index = planTopologyScopeEditor.createInvertedIndex();
        graph.entities().forEach(
                entity -> index.add(entity)
        );
        // scope using inverted index
        TopologyGraph<TopologyEntity> result = planTopologyScopeEditor
                .indexBasedScoping(index, containerClusterTopologyInfo, graph, groupResolver, planScope, PlanProjectType.USER);
        result.entities().forEach(e -> System.out.println(e.getOid() + " " + e.getDisplayName() + " "));

        List<String> expectedEntities = Arrays.asList("cntClusterInAzure", "nsInAzure", "wrkContrllerInAzure", "kubeCntSpecInAzure",
                "kubePodInAzure", "kubeCntInAzure", "computerTier2",
                "BusinessAcct4", "Canada", "storageTier2", "Kubernetes VM in Canada");

        assertEquals(expectedEntities.size(), result.size());
        assertEquals(1, result.entitiesOfType(EntityType.BUSINESS_ACCOUNT).count());
        assertEquals(1, result.entitiesOfType(EntityType.REGION).count());
        assertEquals(1, result.entitiesOfType(EntityType.COMPUTE_TIER).count());
        assertEquals(1, result.entitiesOfType(EntityType.STORAGE_TIER).count());
        assertEquals(0, result.entitiesOfType(EntityType.AVAILABILITY_ZONE).count());
    }

    @Test
    public void testScopeOnVMFromCloudContainerCluster() throws Exception {
        final PlanScope planScope = PlanScope.newBuilder()
                .addScopeEntries(PlanScopeEntry.newBuilder().setClassName("VirtualMachine")
                        .setScopeObjectOid(kubeVm1InLondon.getOid()).setDisplayName("kubeVm1InLondon").build()).build();
        // populate InvertedIndex
        InvertedIndex<TopologyEntity, TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider>
                index = planTopologyScopeEditor.createInvertedIndex();
        graph.entities().forEach(
                entity -> index.add(entity)
        );
        // scope using inverted index
        TopologyGraph<TopologyEntity> result = planTopologyScopeEditor
                .indexBasedScoping(index, containerClusterTopologyInfo, graph, groupResolver, planScope, PlanProjectType.USER);
        result.entities().forEach(e -> System.out.println(e.getOid() + " " + e.getDisplayName() + " "));

        List<String> expectedEntities = Arrays.asList("cntCluster1", "ns1", "wrkContrller1", "kubeCntSpec1",
                "kubePod1", "kubeCnt1", "Compute tier", "Storage tier",
                "Business account 1", "London", "AZ1 London",   "Kubernetes VM1 in London");

        assertEquals(expectedEntities.size(), result.size());
        assertEquals(1, result.entitiesOfType(EntityType.BUSINESS_ACCOUNT).count());
        assertEquals(1, result.entitiesOfType(EntityType.REGION).count());
        assertEquals(1, result.entitiesOfType(EntityType.COMPUTE_TIER).count());
        assertEquals(1, result.entitiesOfType(EntityType.STORAGE_TIER).count());
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
                                                                         Map<Long, Pair<Integer, List<TopologyDTO.CommodityType>>> producers,
                                                                         List<TopologyDTO.CommodityType> soldComms,
                                                                         long... connectedEntities) {
        TopologyEntity.Builder entity = TopologyEntityUtils.topologyEntity(oid, HYPERVISOR_TARGET, 0, displayName,
                entityType, soldComms, producers);
        entity.getEntityBuilder().setEnvironmentType(EnvironmentType.ON_PREM);
        Arrays.stream(connectedEntities).forEach(e ->
            entity.getEntityBuilder()
                .addConnectedEntityList(ConnectedEntity.newBuilder()
                        .setConnectedEntityId(e)
                        .setConnectionType(ConnectionType.NORMAL_CONNECTION)
                        .build()));
        return entity;
    }

    private TopologyEntity.Builder createHypervisorConnectedTopologyEntity(long oid,
            long discoveringTargetId,
            long lastUpdatedTime,
            String displayName,
            EntityType entityType,
            long... connectedToEntities) {
        TopologyEntity.Builder builder = TopologyEntityUtils.connectedTopologyEntity(
                oid, discoveringTargetId, lastUpdatedTime, displayName, entityType,
                connectedToEntities);
        builder.getEntityBuilder().setEnvironmentType(EnvironmentType.ON_PREM);
        return builder;
    }

    /**
     * Scenario: scope on emptyClusterGraph's pm1 and pm2 which consumes on dc1.
     * Expected: pm1InDc1, pm2InDc1, dc1, st1, da1, localSt1, localSt2
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
            .indexBasedScoping(index, onPremTopologyInfo, emptyClusterGraph, groupResolver, planScope, PlanProjectType.USER);
        assertEquals(7, result.size());
        assertEquals(ImmutableSet.of(pm1InDc1.getOid(), pm2InDc1.getOid(), dc1.getOid(), st1.getOid(),
            da1.getOid(), localSt1.getOid(), localSt2.getOid()),
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
        TopologyEntity.Builder builder = TopologyEntityUtils.connectedTopologyEntity(oid, targetId, 0,
                                                           displayName, EntityType.AVAILABILITY_ZONE,
                                                           Collections.emptySet());

        final ImmutableList.Builder<CommoditySoldDTO> commSoldList = ImmutableList.builder();
        CommoditySoldDTO.Builder commoditySoldBuilder = CommoditySoldDTO.newBuilder().setCommodityType(
                TopologyDTO.CommodityType.newBuilder().setType(CommodityType.ZONE_VALUE).setKey(displayName).build())
                .setActive(true);
        commSoldList.add(commoditySoldBuilder.build());
        builder.getEntityBuilder().addAllCommoditySoldList(commSoldList.build());

        builder.getEntityBuilder().setEnvironmentType(EnvironmentType.CLOUD);
        return builder;
    }

    private static TopologyEntity.Builder createComputeTier(long targetId, long oid, String displayName,
                                                            long... connectedToEntities) {
        TopologyEntity.Builder builder = createCloudConnectedTopologyEntity(
                targetId, oid, displayName, EntityType.COMPUTE_TIER);
        final ImmutableList.Builder<CommoditySoldDTO> commSoldList = ImmutableList.builder();
        CommoditySoldDTO.Builder commoditySoldBuilder = CommoditySoldDTO.newBuilder().setCommodityType(
                TopologyDTO.CommodityType.newBuilder().setType(CommodityType.CPU_VALUE).setKey(displayName).build())
                .setActive(true);
        commSoldList.add(commoditySoldBuilder.build());
        builder.getEntityBuilder().addAllCommoditySoldList(commSoldList.build());

        builder.getEntityBuilder().setEnvironmentType(EnvironmentType.CLOUD);

        return builder;
    }

    /**
     * Make this entity aggregated by the aggregator
     *
     * @param entity The entity
     * @param aggregatorId The ID of the aggregator
     * @return The entity
     */
    private static TopologyEntity.Builder addAggregatedByConnection(@Nonnull final TopologyEntity.Builder entity,
                                                                    final long aggregatorId) {
        entity.getEntityBuilder().addConnectedEntityList(ConnectedEntity.newBuilder()
            .setConnectedEntityId(aggregatorId)
            .setConnectionType(ConnectionType.AGGREGATED_BY_CONNECTION));

        return entity;
    }

    /**
     * Make this entity controlled by the controller
     *
     * @param entity The entity
     * @param controllerId The ID of the controller
     * @return The entity
     */
    private static TopologyEntity.Builder addControlledByConnection(@Nonnull final TopologyEntity.Builder entity,
                                                                    final long controllerId) {
        entity.getEntityBuilder().addConnectedEntityList(
                ConnectedEntity.newBuilder()
                        .setConnectedEntityId(controllerId)
                        .setConnectionType(ConnectionType.CONTROLLED_BY_CONNECTION));
        return entity;
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
        TopologyEntity.Builder builder = TopologyEntityUtils.connectedTopologyEntity(oid, targetId, 0,
                displayName, entityType, connectedEntities);
        builder.getEntityBuilder().setEnvironmentType(EnvironmentType.CLOUD);
        return builder;
    }

    private static TopologyEntity.Builder createCloudTopologyEntity(long oid,
            long discoveringTargetId,
            long lastUpdatedTime,
            EntityType entityType,
            long... producers) {
        TopologyEntity.Builder builder = TopologyEntityUtils.topologyEntity(
                oid, discoveringTargetId, lastUpdatedTime,
                entityType, producers);
        builder.getEntityBuilder().setEnvironmentType(EnvironmentType.CLOUD);
        return builder;
    }

    private static TopologyEntity.Builder createCloudConnectedTopologyEntity(
            long targetId, long oid, String displayName, EntityType entityType,
            long... connectedToEntities) {
        TopologyEntity.Builder builder = TopologyEntityUtils.connectedTopologyEntity(oid, targetId,
                0, displayName, entityType, connectedToEntities);
        builder.getEntityBuilder().setEnvironmentType(EnvironmentType.CLOUD);
        return builder;
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

        TopologyEntity.Builder builder = TopologyEntityUtils.connectedTopologyEntity(oid, targetId, 0,
                                                           displayName, EntityType.REGION,
                                                           connectedAvailabilityZones);
        builder.getEntityBuilder().setEnvironmentType(EnvironmentType.CLOUD);
        return builder;
    }

    /**
     * Test the migration of an on-prem VM to the cloud AWS London region.
     * The plan scope should contain:
     * 1. The cloud entities of the destination region
     * 2. The on-prem VM and its providers
     *
     * @throws Exception any exception
     */
    @Test
    public void testScopeCloudMigrationPlanOnPremToAwsRegion() {
        final TopologyGraph<TopologyEntity> cloudMigrationGraph = TopologyEntityUtils
            .topologyGraphOf(bapp1, appc1, vm1InDc1, vm2InDc1, vmInDc2, virtualVolume, pm1InDc1,
                pm2InDc1, pmInDc2, dc1, dc2, st1, st2, da1, as1,
                as2, az1London, az2London, azOhio,
                az1HongKong, az2HongKong, regionLondon, regionOhio,
                regionHongKong, computeTier, vm1InLondon,
                vm2InLondon, dbLondon, dbsHongKong,
                vmInOhio, vmInHongKong, businessAcc1, businessAcc2, businessAcc3,
                virtualVolumeInOhio, virtualVolumeInLondon, virtualVolumeInHongKong,
                storageTier, regionCentralUs, regionCanada, dbCentralUs, dbsCentralUs,
                computeTier2, storageTier2, virtualVolumeInCentralUs,
                vmInCentralUs, virtualVolumeInCanada, vmInCanada, businessAcc4, cloudService,
                appAws, appAzure, unattachedVirtualVolumeInCentralUs,
                unattachedVirtualVolumeInLondon, virtualVolume2InCanada);

        final TopologyInfo cloudTopologyInfo = TopologyInfo.newBuilder()
                .setTopologyContextId(1)
                .setTopologyId(1)
                .setTopologyType(TopologyType.PLAN)
                .setPlanInfo(PlanTopologyInfo.newBuilder()
                        .setPlanType(PlanProjectType.CLOUD_MIGRATION.name())
                        .build())
                .addAllScopeSeedOids(Collections.singleton(regionLondon.getOid()))
                .build();
        final Set<TopologyEntity.Builder> awsRegionExpectedEntities = Stream
                .of(vm1InDc1, pm1InDc1, da1, st1, dc1, regionLondon, virtualVolume,
                        computeTier, storageTier, businessAcc1, businessAcc2, businessAcc3)
                .collect(Collectors.toSet());
        final Set<Long> sourceVMOids = new HashSet<>();
        sourceVMOids.add(vm1InDc1.getOid());
        final TopologyGraph<TopologyEntity> result = planTopologyScopeEditor.scopeTopology(
                cloudTopologyInfo, cloudMigrationGraph, sourceVMOids);
        // Now we use generic scoping, that pulls in more entities than directly connected ones.
        Assert.assertEquals(21, result.size());
        awsRegionExpectedEntities.forEach(entity -> assertTrue(entity.getOid()
                + " is missing", result.getEntity(entity.getOid()).isPresent()));
    }

    private static TopologyEntity.Builder createCloudVm(
        final long targetId,
        final long oid,
        final String displayName,
        final long... volumeOids) {
        return createCloudVm(targetId, oid, displayName, Collections.emptyList(), volumeOids);
    }

    private static TopologyEntity.Builder createCloudVm(
            final long targetId,
            final long oid,
            final String displayName,
            List<TopologyDTO.CommodityType> soldComms,
            final long... volumeOids) {
        TopologyEntity.Builder builder = TopologyEntityUtils.topologyEntity(
                oid, targetId, 0, displayName, EntityType.VIRTUAL_MACHINE, soldComms, volumeOids);
        builder.getEntityBuilder().setEnvironmentType(EnvironmentType.CLOUD);
        return builder;
    }

    private static TopologyEntity.Builder createCloudVolume(
            final long targetId,
            final long oid,
            final String displayName,
            final long storageTierOid) {
        TopologyEntity.Builder builder = TopologyEntityUtils.topologyEntity(
                oid, targetId, 0, displayName, EntityType.VIRTUAL_VOLUME, storageTierOid);
        builder.getEntityBuilder().setEnvironmentType(EnvironmentType.CLOUD);
        return builder;
    }

    private static TopologyEntity.Builder createKubeCloudVm (final long targetId,
                                                             final long oid, final String displayName,
                                                            List<TopologyDTO.CommodityType> soldComms,
                                                            Map<Long, List<TopologyDTO.CommodityType>> producers,
                                                            final long... volumeOids) {
        TopologyEntity.Builder builder
                = TopologyEntityUtils.topologyEntity(oid, targetId, 0, displayName,
                                                        EntityType.VIRTUAL_MACHINE, producers, soldComms);
        for (long producer : volumeOids) {
            builder.getEntityBuilder()
                    .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                    .setProviderId(producer)
                    .build());
        }
        builder.getEntityBuilder().setEnvironmentType(EnvironmentType.CLOUD);

        return builder;
    }

    private static TopologyEntity.Builder createContainerCluster(
            long targetId, long oid, String displayName,
            List<TopologyDTO.CommodityType> soldComms,
            Collection<TopologyEntity.Builder> namespaces,
            Collection<TopologyEntity.Builder> aggregatedVms) {

        namespaces.forEach(e ->
                e.getEntityBuilder().addConnectedEntityList(
                        ConnectedEntity.newBuilder()
                                .setConnectionType(ConnectionType.AGGREGATED_BY_CONNECTION)
                                .setConnectedEntityId(oid)
                                .setConnectedEntityType(EntityType.CONTAINER_PLATFORM_CLUSTER_VALUE)));

        aggregatedVms.forEach(e ->
                e.getEntityBuilder().addConnectedEntityList(
                        ConnectedEntity.newBuilder()
                                .setConnectionType(ConnectionType.AGGREGATED_BY_CONNECTION)
                                .setConnectedEntityId(oid)
                                .setConnectedEntityType(EntityType.CONTAINER_PLATFORM_CLUSTER_VALUE)));
        TopologyEntity.Builder builder = TopologyEntityUtils.topologyEntity(oid, targetId, 0, displayName,
                EntityType.CONTAINER_PLATFORM_CLUSTER, new HashMap<>(), soldComms);

        return builder;
    }

    private static TopologyEntity.Builder createNamespace(
            long targetId, long oid, String displayName,
            Map<Long, List<TopologyDTO.CommodityType>> commBoughtByNamespace,
            List<TopologyDTO.CommodityType> soldComms,
            Collection<TopologyEntity.Builder> aggregatedEntities) {
        TopologyEntity.Builder builder = TopologyEntityUtils.topologyEntity(oid, targetId, 0, displayName,
                EntityType.NAMESPACE, commBoughtByNamespace, soldComms);

        aggregatedEntities.forEach(e ->
                e.getEntityBuilder().addConnectedEntityList(
                        ConnectedEntity.newBuilder()
                                .setConnectionType(ConnectionType.AGGREGATED_BY_CONNECTION)
                                .setConnectedEntityId(oid)
                                .setConnectedEntityType(EntityType.NAMESPACE_VALUE)));
        return builder;
    }

    private static TopologyEntity.Builder createWorkloadControllers(
            long targetId, long oid, String displayName,
            Map<Long, List<TopologyDTO.CommodityType>> commBoughtByWorkloadController,
            List<TopologyDTO.CommodityType> soldComms,
            Collection<TopologyEntity.Builder> ownedContainerSpecs) {

        TopologyEntity.Builder builder = TopologyEntityUtils.topologyEntity(oid, targetId, 0, displayName,
                EntityType.WORKLOAD_CONTROLLER, commBoughtByWorkloadController, soldComms);

        final Collection<ConnectedEntity> connectedContainerSpecs  =
                ownedContainerSpecs.stream()
                        .map(e -> ConnectedEntity.newBuilder()
                                .setConnectedEntityId(e.getOid())
                                .setConnectedEntityType(EntityType.CONTAINER_SPEC_VALUE)
                                .setConnectionType(ConnectionType.OWNS_CONNECTION)
                                .build())
                        .collect(Collectors.toList());

        for (ConnectedEntity connectedEntity : connectedContainerSpecs) {
            builder.getEntityBuilder().addConnectedEntityList(ConnectedEntity.newBuilder()
                    .setConnectedEntityId(connectedEntity.getConnectedEntityId())
                    .setConnectionType(connectedEntity.getConnectionType())
                    .build());
        }
        return builder;
    }

    private static TopologyEntity.Builder createCloudNativeTopologyEntity(long oid,
                                                                          String displayName,
                                                                          EntityType entityType,
                                                                          Map<Long, List<TopologyDTO.CommodityType>> producers,
                                                                          List<TopologyDTO.CommodityType> soldComms,
                                                                          long... connectedEntities) {
        TopologyEntity.Builder entity = TopologyEntityUtils.topologyEntity(oid, CLOUD_NATIVE_TARGET, 0, displayName,
                entityType, producers, soldComms);
        Arrays.stream(connectedEntities).forEach(e ->
                entity.getEntityBuilder()
                        .addConnectedEntityList(ConnectedEntity.newBuilder()
                                .setConnectedEntityId(e)
                                .setConnectionType(ConnectionType.NORMAL_CONNECTION)
                                .build()));
        return entity;
    }

}
