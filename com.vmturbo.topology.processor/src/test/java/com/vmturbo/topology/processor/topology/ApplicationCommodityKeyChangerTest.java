package com.vmturbo.topology.processor.topology;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.TopologyEntity.Builder;

public class ApplicationCommodityKeyChangerTest {

    public static final long vmReplica1oid = 1L;
    public static final long vmReplica2oid = 2L;
    public static final long appReplica1oid = 11L;
    public static final long appReplica2oid = 12L;

    private final String appKey = "appKey";
    private final String anotherKey = "anotherKey";

    // vm replica 1
    private final TopologyEntity.Builder vmReplica1 = TopologyEntity.newBuilder(
        TopologyEntityDTO.newBuilder()
            .setOid(vmReplica1oid)
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                    .setType(CommodityDTO.CommodityType.APPLICATION_VALUE)
                    .setKey(appKey)))
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                    .setType(CommodityDTO.CommodityType.CLUSTER_VALUE)
                    .setKey(anotherKey))));

    // vm replica 2
    // it is selling the same appKey1 as vm1, but different oid
    private final TopologyEntity.Builder vmReplica2 = TopologyEntity.newBuilder(
        TopologyEntityDTO.newBuilder()
            .setOid(vmReplica2oid)
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                    .setType(CommodityDTO.CommodityType.APPLICATION_VALUE)
                    .setKey(appKey)))
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                    .setType(CommodityDTO.CommodityType.CLUSTER_VALUE)
                    .setKey(anotherKey))));


    // app buying from vmReplica1
    private final TopologyEntity.Builder appReplica1 = TopologyEntity.newBuilder(
        TopologyEntityDTO.newBuilder()
            .setOid(appReplica1oid)
            .setEntityType(EntityType.APPLICATION_VALUE)
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .setProviderEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setProviderId(vmReplica1oid)
                .addCommodityBought(CommodityBoughtDTO.newBuilder()
                    .setCommodityType(CommodityType.newBuilder()
                        .setType(CommodityDTO.CommodityType.APPLICATION_VALUE)
                        .setKey(appKey)))
                .addCommodityBought(CommodityBoughtDTO.newBuilder()
                    .setCommodityType(CommodityType.newBuilder()
                        .setType(CommodityDTO.CommodityType.CLUSTER_VALUE)
                        .setKey(anotherKey)))));

    // app buying from vmReplica2
    private final TopologyEntity.Builder appReplica2 = TopologyEntity.newBuilder(
        TopologyEntityDTO.newBuilder()
            .setOid(appReplica2oid)
            .setEntityType(EntityType.APPLICATION_VALUE)
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .setProviderEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setProviderId(vmReplica2oid)
                .addCommodityBought(CommodityBoughtDTO.newBuilder()
                    .setCommodityType(CommodityType.newBuilder()
                        .setType(CommodityDTO.CommodityType.APPLICATION_VALUE)
                        .setKey(appKey)))
                .addCommodityBought(CommodityBoughtDTO.newBuilder()
                    .setCommodityType(CommodityType.newBuilder()
                        .setType(CommodityDTO.CommodityType.CLUSTER_VALUE)
                        .setKey(anotherKey)))));


    // create the topology graph
    private final Map<Long, Builder> topologyMap = ImmutableMap.of(
        vmReplica1.getOid(), vmReplica1,
        vmReplica2.getOid(), vmReplica2,
        appReplica1.getOid(), appReplica1,
        appReplica2.getOid(), appReplica2
    );

    private final TopologyGraph topologyGraph = TopologyGraph.newGraph(topologyMap);


    @Test
    public void testExecute() {

        ApplicationCommodityKeyChanger applicationCommodityKeyChanger = new ApplicationCommodityKeyChanger();
        applicationCommodityKeyChanger.execute(topologyGraph);

        // extract the modified commodities list
        List<CommoditySoldDTO> vm1CommoditySoldListList = topologyGraph.getEntity(vmReplica1oid).get()
            .getTopologyEntityDtoBuilder().getCommoditySoldListList();
        List<CommoditySoldDTO> vm2CommoditySoldListList = topologyGraph.getEntity(vmReplica2oid).get()
            .getTopologyEntityDtoBuilder().getCommoditySoldListList();
        List<CommodityBoughtDTO.Builder> app1CommodityBoughtList = topologyGraph.getEntity(appReplica1oid)
            .get().getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersBuilderList().get(0)
            .getCommodityBoughtBuilderList();
        List<CommodityBoughtDTO.Builder> app2CommodityBoughtList = topologyGraph.getEntity(appReplica2oid)
            .get().getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersBuilderList().get(0)
            .getCommodityBoughtBuilderList();

        CommoditySoldDTO vm1AppComm = vm1CommoditySoldListList.stream()
            .filter(comm -> comm.getCommodityType().getType() == CommodityDTO.CommodityType.APPLICATION_VALUE)
            .findFirst()
            .get();

        CommoditySoldDTO vm2AppComm = vm2CommoditySoldListList.stream()
            .filter(comm -> comm.getCommodityType().getType() == CommodityDTO.CommodityType.APPLICATION_VALUE)
            .findFirst()
            .get();

        CommoditySoldDTO vm1ClusterComm = vm1CommoditySoldListList.stream()
            .filter(comm -> comm.getCommodityType().getType() == CommodityDTO.CommodityType.CLUSTER_VALUE)
            .findFirst()
            .get();

        CommoditySoldDTO vm2ClusterComm = vm2CommoditySoldListList.stream()
            .filter(comm -> comm.getCommodityType().getType() == CommodityDTO.CommodityType.CLUSTER_VALUE)
            .findFirst()
            .get();

        CommodityBoughtDTO.Builder app1AppComm = app1CommodityBoughtList.stream()
            .filter(comm -> comm.getCommodityType().getType() == CommodityDTO.CommodityType.APPLICATION_VALUE)
            .findFirst()
            .get();

        CommodityBoughtDTO.Builder app2AppComm = app2CommodityBoughtList.stream()
            .filter(comm -> comm.getCommodityType().getType() == CommodityDTO.CommodityType.APPLICATION_VALUE)
            .findFirst()
            .get();

        CommodityBoughtDTO.Builder app1ClusterComm = app1CommodityBoughtList.stream()
            .filter(comm -> comm.getCommodityType().getType() == CommodityDTO.CommodityType.CLUSTER_VALUE)
            .findFirst()
            .get();

        CommodityBoughtDTO.Builder app2ClusterComm = app2CommodityBoughtList.stream()
            .filter(comm -> comm.getCommodityType().getType() == CommodityDTO.CommodityType.CLUSTER_VALUE)
            .findFirst()
            .get();

        // check that the application commodity key were changed to the vm oid
        assertEquals(Long.toString(vmReplica1oid), vm1AppComm.getCommodityType().getKey());
        assertEquals(Long.toString(vmReplica1oid), app1AppComm.getCommodityType().getKey());
        assertEquals(Long.toString(vmReplica2oid), vm2AppComm.getCommodityType().getKey());
        assertEquals(Long.toString(vmReplica2oid), app2AppComm.getCommodityType().getKey());

        // check that the cluster commodity key is the same as before
        assertEquals(anotherKey, vm1ClusterComm.getCommodityType().getKey());
        assertEquals(anotherKey, app1ClusterComm.getCommodityType().getKey());
        assertEquals(anotherKey, vm2ClusterComm.getCommodityType().getKey());
        assertEquals(anotherKey, app2ClusterComm.getCommodityType().getKey());

    }

}