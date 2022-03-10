package com.vmturbo.topology.processor.topology.pipeline;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.TopologyAddition;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommoditySoldImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommoditySoldView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityTypeImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityTypeView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.EditImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.OriginImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.PlanScenarioOriginImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.RemovedImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.ReplacedImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TypeSpecificInfoImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TypeSpecificInfoImpl.StorageInfoImpl;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.template.TopologyEntityConstructor;
import com.vmturbo.topology.processor.topology.PostScopingTopologyEditor;
import com.vmturbo.topology.processor.topology.PostScopingTopologyEditor.PostScopingTopologyEditResult;
import com.vmturbo.topology.processor.topology.TopologyEntityUtils;

/**
 * Tests for Post scoping topology editor.
 */
public class PostScopingTopologyEditorTest {

    private static final String keyConst = "asdf";
    private static final Long planId = 88L;
    private TopologyEntity.Builder existingPm;
    private TopologyEntity.Builder addedPm;
    private TopologyEntity.Builder existingLocalSt;
    private TopologyEntity.Builder existingSharedSt;
    private TopologyEntity.Builder addedSharedSt;
    private TopologyEntity.Builder existingSharedStRemoved;
    private TopologyEntity.Builder existingSharedStReplaced;

    private static final Map<Integer, Integer> connectedCommSoldByEntityType = ImmutableMap.of(
            EntityType.PHYSICAL_MACHINE_VALUE, CommodityType.DATASTORE_VALUE,
            EntityType.STORAGE_VALUE, CommodityType.DSPM_ACCESS_VALUE);

    private TopologyGraph<TopologyEntity> graph;
    private final List<ScenarioChange> scenarioChangeList = new ArrayList<>();

    /**
     * Create a graph of 5 entities.
     * <pre>
     *
     * Existing Host (1)   -------------------->    Existing local storage (3)
     *                 |
     *                 ------------------------>    Existing shared storage (4)
     *                 |
     *                 ----------------------->    Existing shared storage - Removed (6)
     *                 |
     *                 ----------------------->    Existing shared storage - Replaced (7)
     *
     * Newly added Host (2)                         Newly added storage (5)
     *
     * </pre>
     */
    @Before
    public void setup() {
        existingPm = topologyEntity(1L, EntityType.PHYSICAL_MACHINE_VALUE, false, ImmutableSet.of(3L, 4L), Optional.empty(), false, false);
        addedPm = topologyEntity(2L, EntityType.PHYSICAL_MACHINE_VALUE, true, ImmutableSet.of(), Optional.empty(), false, false);
        existingLocalSt = topologyEntity(3L, EntityType.STORAGE_VALUE, false, ImmutableSet.of(1L), Optional.of(true), false, false);
        existingSharedSt = topologyEntity(4L, EntityType.STORAGE_VALUE, false, ImmutableSet.of(1L), Optional.of(false), false, false);
        addedSharedSt = topologyEntity(5L, EntityType.STORAGE_VALUE, true, ImmutableSet.of(), Optional.empty(), false, false);
        existingSharedStRemoved = topologyEntity(6L, EntityType.STORAGE_VALUE, false, ImmutableSet.of(1L), Optional.of(false), true, false);
        existingSharedStReplaced = topologyEntity(7L, EntityType.STORAGE_VALUE, false, ImmutableSet.of(1L), Optional.of(false), false, true);
        graph = TopologyEntityUtils.pojoGraphOf(existingPm, addedPm, existingLocalSt, existingSharedSt,
                addedSharedSt, existingSharedStRemoved, existingSharedStReplaced);
        ScenarioChange sc1 = ScenarioChange.newBuilder().setTopologyAddition(TopologyAddition.newBuilder()
                .setTargetEntityType(EntityType.PHYSICAL_MACHINE_VALUE).build()).build();
        ScenarioChange sc2 = ScenarioChange.newBuilder().setTopologyAddition(TopologyAddition.newBuilder()
                .setTargetEntityType(EntityType.STORAGE_VALUE).build()).build();
        scenarioChangeList.add(sc1);
        scenarioChangeList.add(sc2);
    }

    /**
     * Run the Post scoping topology edit on this topology and check that
     * 1. the newly added hosts are connected to all non-local storages which are not removed/replaced
     * 2. the newly added storages are connected to all hosts
     */
    @Test
    public void testEditTopology() {
        PostScopingTopologyEditor editor = new PostScopingTopologyEditor();
        List<PostScopingTopologyEditResult> results = editor.editTopology(graph, scenarioChangeList);

        assertEquals(2, results.size());
        PostScopingTopologyEditResult addedHostEditResult = results.stream()
                .filter(r -> r.getAddedEntityType() == EntityType.PHYSICAL_MACHINE_VALUE).findFirst().get();
        assertEquals(1, addedHostEditResult.getAddedEntityCount());
        assertEquals(2, addedHostEditResult.getConnectedEntityCount());
        assertEquals(4, addedHostEditResult.getNumCommoditiesCreated());

        PostScopingTopologyEditResult addedStorageEditResult = results.stream()
                .filter(r -> r.getAddedEntityType() == EntityType.STORAGE_VALUE).findFirst().get();
        assertEquals(1, addedStorageEditResult.getAddedEntityCount());
        assertEquals(1, addedStorageEditResult.getConnectedEntityCount());
        assertEquals(2, addedStorageEditResult.getNumCommoditiesCreated());

        // 1. Check that the existing host is connected to the newly added storage
        // Check the comm type, key and accesses.
        assertEquals(3, existingPm.getTopologyEntityImpl().getCommoditySoldListList().size());
        assertTrue(existingPm.getTopologyEntityImpl().getCommoditySoldListList().stream()
                .allMatch(cs -> cs.getCommodityType().getType() == CommodityType.DATASTORE_VALUE));
        CommoditySoldView commSold = existingPm.getTopologyEntityImpl().getCommoditySoldListList().stream()
                .filter(cs -> cs.getAccesses() == addedSharedSt.getOid()).findFirst().get();
        CommoditySoldImpl expectedAccessCommodity = TopologyEntityConstructor.createAccessCommodity(CommodityType.DATASTORE, addedSharedSt.getOid(), null);
        assertEquals(expectedAccessCommodity.getCommodityType().getKey(), commSold.getCommodityType().getKey());

        // 2. Check that the newly added host is connected to the both the shared storages, but not the local storage
        // Check the comm type, key and accesses.
        assertEquals(2, addedPm.getTopologyEntityImpl().getCommoditySoldListList().size());
        assertTrue(addedPm.getTopologyEntityImpl().getCommoditySoldListList().stream()
                .allMatch(cs -> cs.getCommodityType().getType() == CommodityType.DATASTORE_VALUE));
        commSold = addedPm.getTopologyEntityImpl().getCommoditySoldListList().stream()
                .filter(cs -> cs.getAccesses() == existingSharedSt.getOid()).findFirst().get();
        // Ensure new key is not created if there already exist DS comms pointing to this storage
        assertEquals(createKey(existingSharedSt.getOid()), commSold.getCommodityType().getKey());
        commSold = addedPm.getTopologyEntityImpl().getCommoditySoldListList().stream()
                .filter(cs -> cs.getAccesses() == addedSharedSt.getOid()).findFirst().get();
        assertEquals(expectedAccessCommodity.getCommodityType().getKey(), commSold.getCommodityType().getKey());

        // 3. Nothing new should be created/modified for the local storage
        assertEquals(1, existingLocalSt.getTopologyEntityImpl().getCommoditySoldListList().size());
        assertTrue(existingLocalSt.getTopologyEntityImpl().getCommoditySoldListList().stream()
                .allMatch(cs -> cs.getCommodityType().getType() == CommodityType.DSPM_ACCESS_VALUE));
        assertNotNull(existingLocalSt.getTopologyEntityImpl().getCommoditySoldListList().stream()
                .filter(cs -> cs.getAccesses() == existingPm.getOid()).findFirst().get());

        // 4. Check that the existing storage is now connected to the newly added host
        // Check the comm type, key and accesses.
        assertEquals(2, existingSharedSt.getTopologyEntityImpl().getCommoditySoldListList().size());
        assertTrue(existingSharedSt.getTopologyEntityImpl().getCommoditySoldListList().stream()
                .allMatch(cs -> cs.getCommodityType().getType() == CommodityType.DSPM_ACCESS_VALUE));
        commSold = existingSharedSt.getTopologyEntityImpl().getCommoditySoldListList().stream()
                .filter(cs -> cs.getAccesses() == addedPm.getOid()).findFirst().get();
        expectedAccessCommodity = TopologyEntityConstructor.createAccessCommodity(CommodityType.DSPM_ACCESS, addedPm.getOid(), null);
        assertEquals(expectedAccessCommodity.getCommodityType().getKey(), commSold.getCommodityType().getKey());

        // 5. Check that the newly added storage is now connected to both the hosts
        // Check the comm type, key and accesses.
        assertEquals(2, addedSharedSt.getTopologyEntityImpl().getCommoditySoldListList().size());
        assertTrue(addedSharedSt.getTopologyEntityImpl().getCommoditySoldListList().stream()
                .allMatch(cs -> cs.getCommodityType().getType() == CommodityType.DSPM_ACCESS_VALUE));
        commSold = addedSharedSt.getTopologyEntityImpl().getCommoditySoldListList().stream()
                .filter(cs -> cs.getAccesses() == addedPm.getOid()).findFirst().get();
        expectedAccessCommodity = TopologyEntityConstructor.createAccessCommodity(CommodityType.DSPM_ACCESS, addedPm.getOid(), null);
        assertEquals(expectedAccessCommodity.getCommodityType().getKey(), commSold.getCommodityType().getKey());
        commSold = addedSharedSt.getTopologyEntityImpl().getCommoditySoldListList().stream()
                .filter(cs -> cs.getAccesses() == existingPm.getOid()).findFirst().get();
        assertEquals(createKey(existingPm.getOid()), commSold.getCommodityType().getKey());
    }

    private TopologyEntity.Builder topologyEntity(long oid, int entityType, boolean isPlanOrigin, Set<Long> connectedTo,
                                                  Optional<Boolean> isLocal, boolean isRemoved, boolean isReplaced) {
        TopologyEntityImpl topo = new TopologyEntityImpl()
                .setOid(oid)
                .setDisplayName(EntityType.forNumber(entityType).toString() + oid)
                .setEntityType(entityType);
        for (Long conn : connectedTo) {
            CommodityTypeView commType = new CommodityTypeImpl()
                    .setType(connectedCommSoldByEntityType.get(entityType)).setKey(createKey(conn));
            topo.addCommoditySoldList(new CommoditySoldImpl().setCommodityType(commType).setAccesses(conn));
        }
        if (isPlanOrigin) {
            topo.setOrigin(new OriginImpl().setPlanScenarioOrigin(new PlanScenarioOriginImpl().setPlanId(88L)));
        }
        if (isLocal.isPresent()) {
            topo.setTypeSpecificInfo(new TypeSpecificInfoImpl().setStorage(new StorageInfoImpl().setIsLocal(isLocal.get())));
        }
        if (isRemoved) {
            topo.setEdit(new EditImpl().setRemoved(new RemovedImpl().setPlanId(planId)));
        }
        if (isReplaced) {
            topo.setEdit(new EditImpl().setReplaced(new ReplacedImpl().setPlanId(planId)));
        }
        return TopologyEntity.newBuilder(topo);
    }

    private String createKey(long oid) {
        return keyConst + oid;
    }
}
