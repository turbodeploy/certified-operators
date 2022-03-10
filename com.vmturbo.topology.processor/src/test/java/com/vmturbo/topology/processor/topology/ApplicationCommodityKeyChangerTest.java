package com.vmturbo.topology.processor.topology;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityBoughtImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommoditySoldImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommoditySoldView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityTypeImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.CommoditiesBoughtFromProviderImpl;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;

public class ApplicationCommodityKeyChangerTest {

    private static final long vmReplica1oid = 700001L;
    private static final long vmReplica2oid = 700002L;
    private static final long appReplica1oid = 700011L;
    private static final long appReplica2oid = 700012L;
    private static final long appOid = 700013L;

    private static final String appKey = "appKey";
    private static final String anotherKey = "anotherKey";
    private static final String yetAnotherKey = "yetAnotherKey";

    // vm replica 1
    private static final TopologyEntity.Builder vmReplica1 =
            seller(vmReplica1oid, EntityType.VIRTUAL_MACHINE_VALUE,
                commSold(CommodityDTO.CommodityType.APPLICATION_VALUE, appKey),
                commSold(CommodityDTO.CommodityType.CLUSTER_VALUE, anotherKey));

    // vm replica 2
    // it is selling the same appKey1 as vm1, but different oid
    private static final TopologyEntity.Builder vmReplica2 =
            seller(vmReplica2oid, EntityType.VIRTUAL_MACHINE_VALUE,
                commSold(CommodityDTO.CommodityType.APPLICATION_VALUE, appKey),
                commSold(CommodityDTO.CommodityType.CLUSTER_VALUE, anotherKey));

    // app buying from vmReplica1
    private static final TopologyEntity.Builder appReplica1 =
            buyer(appReplica1oid, EntityType.APPLICATION_COMPONENT_VALUE,
                commoditiesBoughtList(vmReplica1,
                        commBought(CommodityDTO.CommodityType.APPLICATION_VALUE, appKey),
                        commBought(CommodityDTO.CommodityType.CLUSTER_VALUE, anotherKey)));

    // app buying from vmReplica2
    private static final TopologyEntity.Builder appReplica2 =
            buyer(appReplica2oid, EntityType.APPLICATION_COMPONENT_VALUE,
                commoditiesBoughtList(vmReplica2,
                        commBought(CommodityDTO.CommodityType.APPLICATION_VALUE, appKey),
                        commBought(CommodityDTO.CommodityType.CLUSTER_VALUE, anotherKey)));

    // another app buying from vmReplica2 and should not change, because the key associated
    // with the application commodity is not changed on the selling vm.
    private static final TopologyEntity.Builder app3 =
            buyer(appOid, EntityType.APPLICATION_COMPONENT_VALUE,
                    commoditiesBoughtList(vmReplica2,
                            commBought(CommodityDTO.CommodityType.APPLICATION_VALUE, yetAnotherKey)));

    // create the topology graph
    private static final Map<Long, TopologyEntity.Builder> topologyMap = ImmutableMap.of(
        vmReplica1.getOid(), vmReplica1,
        vmReplica2.getOid(), vmReplica2,
        appReplica1.getOid(), appReplica1,
        appReplica2.getOid(), appReplica2,
        app3.getOid(), app3
    );

    private static TopologyEntity.Builder seller(long oid, int type, CommoditySoldImpl... commoditiesSold) {
        return TopologyEntity.newBuilder(
                 new TopologyEntityImpl()
                        .setOid(oid)
                        .setEntityType(type)
                        .addAllCommoditySoldList(Sets.newHashSet(commoditiesSold)));
    }

    private static CommoditySoldImpl commSold(int type, String key) {
        return new CommoditySoldImpl()
                .setCommodityType(new CommodityTypeImpl()
                        .setType(type)
                        .setKey(key));
    }

    private static TopologyEntity.Builder buyer(long oid, int type,
            CommoditiesBoughtFromProviderImpl... commoditiesBoughtLists) {
        return  TopologyEntity.newBuilder( new TopologyEntityImpl()
                .setOid(oid)
                .setEntityType(type)
                .addAllCommoditiesBoughtFromProviders(Sets.newHashSet(commoditiesBoughtLists)));
    }

    private static CommoditiesBoughtFromProviderImpl commoditiesBoughtList(
            TopologyEntity.Builder seller, CommodityBoughtImpl... commoditiesBought) {
        return new CommoditiesBoughtFromProviderImpl()
                .setProviderId(seller.getOid())
                .setProviderEntityType(seller.getEntityType())
                .addAllCommodityBought(Sets.newHashSet(commoditiesBought));
    }

    private static CommodityBoughtImpl commBought(int type, String key) {
        return new CommodityBoughtImpl()
                .setCommodityType(new CommodityTypeImpl()
                        .setType(type)
                        .setKey(key));
    }

    private static final TopologyGraph<TopologyEntity> topologyGraph = TopologyEntityTopologyGraphCreator.newGraph(topologyMap);

    @Test
    public void testExecute() {

        ApplicationCommodityKeyChanger applicationCommodityKeyChanger = new ApplicationCommodityKeyChanger();
        applicationCommodityKeyChanger.execute(topologyGraph);

        // check that the application commodity key were changed to the vm oid

        CommoditySoldView vm1AppComm = getCommSold(vmReplica1oid, CommodityDTO.CommodityType.APPLICATION_VALUE);
        CommodityBoughtImpl app1AppComm =
                getCommBought(appReplica1oid, vmReplica1oid, CommodityDTO.CommodityType.APPLICATION_VALUE);
        assertEquals(Long.toString(vmReplica1oid), vm1AppComm.getCommodityType().getKey());
        assertEquals(Long.toString(vmReplica1oid), app1AppComm.getCommodityType().getKey());

        CommoditySoldView vm2AppComm = getCommSold(vmReplica2oid, CommodityDTO.CommodityType.APPLICATION_VALUE);
        CommodityBoughtImpl app2AppComm =
                getCommBought(appReplica2oid, vmReplica2oid, CommodityDTO.CommodityType.APPLICATION_VALUE);
        assertEquals(Long.toString(vmReplica2oid), vm2AppComm.getCommodityType().getKey());
        assertEquals(Long.toString(vmReplica2oid), app2AppComm.getCommodityType().getKey());

        // check that the cluster commodity key is the same as before

        CommoditySoldView vm1ClusterComm = getCommSold(vmReplica1oid, CommodityDTO.CommodityType.CLUSTER_VALUE);
        CommodityBoughtImpl app1ClusterComm =
                getCommBought(appReplica1oid, vmReplica1oid, CommodityDTO.CommodityType.CLUSTER_VALUE);
        assertEquals(anotherKey, vm1ClusterComm.getCommodityType().getKey());
        assertEquals(anotherKey, app1ClusterComm.getCommodityType().getKey());

        CommoditySoldView vm2ClusterComm = getCommSold(vmReplica2oid, CommodityDTO.CommodityType.CLUSTER_VALUE);
        CommodityBoughtImpl app2ClusterComm =
                getCommBought(appReplica2oid, vmReplica2oid, CommodityDTO.CommodityType.CLUSTER_VALUE);
        assertEquals(anotherKey, vm2ClusterComm.getCommodityType().getKey());
        assertEquals(anotherKey, app2ClusterComm.getCommodityType().getKey());

        // Verify that this application commodity key was not changed
        CommodityBoughtImpl app3AppComm =
                getCommBought(appOid, vmReplica2oid, CommodityDTO.CommodityType.APPLICATION_VALUE);
        assertEquals(yetAnotherKey, app3AppComm.getCommodityType().getKey());
    }

    private static CommoditySoldView getCommSold(long sellerOid, int type) {
        return topologyGraph.getEntity(sellerOid).get()
                .getTopologyEntityImpl().getCommoditySoldListList().stream()
                    .filter(commSold -> commSold.getCommodityType().getType() == type)
                    .findAny()
                    .get();
    }

    private static CommodityBoughtImpl getCommBought(long buyerOid, long sellerOid, int type) {
        return topologyGraph.getEntity(buyerOid).get()
                .getTopologyEntityImpl().getCommoditiesBoughtFromProvidersImplList()
                .stream()
                .filter(list -> list.getProviderId() == sellerOid)
                .map(CommoditiesBoughtFromProviderImpl::getCommodityBoughtImplList)
                .flatMap(List::stream)
                .filter(commBought -> commBought.getCommodityType().getType() == type)
                .findAny()
                .get();
    }
}