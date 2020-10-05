package com.vmturbo.repository.plan.db;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.jooq.DSLContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.TopologyType;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChain;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode.MemberList;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainScope;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.Type;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTOOrBuilder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.repository.db.Repository;
import com.vmturbo.repository.db.Tables;
import com.vmturbo.repository.service.PartialEntityConverter;
import com.vmturbo.repository.topology.TopologyID;
import com.vmturbo.repository.topology.TopologyLifecycleManager.TopologyCreator;
import com.vmturbo.sql.utils.DbCleanupRule;
import com.vmturbo.sql.utils.DbConfigurationRule;
import com.vmturbo.topology.graph.supplychain.SupplyChainCalculator;

/**
 * Unit tests for the {@link MySQLPlanEntityStore}.
 */
public class MySQLPlanEntityStoreTest {

    /**
     * Class rule to create a DB.
     */
    @ClassRule
    public static DbConfigurationRule dbConfig = new DbConfigurationRule(Repository.REPOSITORY);

    /**
     * Rule to automatically cleanup DB data before each test.
     */
    @Rule
    public DbCleanupRule dbCleanup = dbConfig.cleanupRule();

    private MySQLPlanEntityStore planEntityStore;

    private PartialEntityConverter partialEntityConverter = mock(PartialEntityConverter.class);

    private SupplyChainCalculator supplyChainCalculator = mock(SupplyChainCalculator.class);

    private static final long PLAN_ID = 8392;
    private static final long SRC_ID = 888;
    private static final long PROJ_ID = 999;

    private TopologyEntityDTO pm = TopologyEntityDTO.newBuilder()
            .setOid(8L)
            .setDisplayName("I am a host")
            .setEntityType(ApiEntityType.PHYSICAL_MACHINE.typeNumber())
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                    .setCommodityType(CommodityType.newBuilder()
                            .setType(CommodityDTO.CommodityType.MEM_VALUE))
                    .setUsed(2))
            .build();

    private static final long STORAGE_OID = 1L;
    private static final long CONTAINER_OID = 1337L;

    private SupplyChain pmSupplyChain = SupplyChain.newBuilder()
            .addSupplyChainNodes(SupplyChainNode.newBuilder()
                    .setEntityType(ApiEntityType.STORAGE.typeNumber())
                    .putMembersByState(EntityState.POWERED_ON_VALUE, MemberList.newBuilder()
                        .addMemberOids(STORAGE_OID)
                        .build()))
            .build();

    private TopologyEntityDTO vm = TopologyEntityDTO.newBuilder()
            .setOid(7L)
            .setDisplayName("I am an entity")
            .setEntityType(ApiEntityType.VIRTUAL_MACHINE.typeNumber())
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                // Unplaced
                .addCommodityBought(CommodityBoughtDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                    .setType(CommodityDTO.CommodityType.MEM_VALUE))))
            .build();

    private SupplyChain vmSupplyChain = SupplyChain.newBuilder()
            // The PM is in the VMs supply chain.
            .addSupplyChainNodes(SupplyChainNode.newBuilder()
                    .setEntityType(ApiEntityType.PHYSICAL_MACHINE.typeNumber())
                    .putMembersByState(EntityState.POWERED_ON_VALUE, MemberList.newBuilder()
                            .addMemberOids(pm.getOid())
                            .build()))
            // A powered off node.
            .addSupplyChainNodes(SupplyChainNode.newBuilder()
                    .setEntityType(ApiEntityType.CONTAINER.typeNumber())
                    .putMembersByState(EntityState.POWERED_OFF_VALUE, MemberList.newBuilder()
                            .addMemberOids(CONTAINER_OID)
                            .build()))
            // The storage is the same as in the pm supply chain
            .addAllSupplyChainNodes(pmSupplyChain.getSupplyChainNodesList())
            .build();

    /**
     * Common code before every test.
     */
    @Before
    public void setup() {
        planEntityStore = new MySQLPlanEntityStore(dbConfig.getDslContext(), partialEntityConverter,
                supplyChainCalculator, 1, 1);
        when(partialEntityConverter.createPartialEntity(any(), any())).thenAnswer(invocationOnMock -> {
            TopologyEntityDTOOrBuilder dto = invocationOnMock.getArgumentAt(0, TopologyEntityDTOOrBuilder.class);
            if (dto instanceof TopologyEntityDTO.Builder) {
                return PartialEntity.newBuilder()
                    .setFullEntity((TopologyEntityDTO.Builder)dto)
                    .build();
            } else {
                return PartialEntity.newBuilder()
                    .setFullEntity((TopologyEntityDTO)dto)
                    .build();
            }
        });
        when(supplyChainCalculator.getSupplyChainNodes(any(), eq(Collections.singleton(vm.getOid())), any(), any()))
                .thenReturn(vmSupplyChain.getSupplyChainNodesList().stream()
                    .collect(Collectors.toMap(SupplyChainNode::getEntityType, Function.identity())));
        when(supplyChainCalculator.getSupplyChainNodes(any(), eq(Collections.singleton(pm.getOid())), any(), any()))
                .thenReturn(pmSupplyChain.getSupplyChainNodesList().stream()
                        .collect(Collectors.toMap(SupplyChainNode::getEntityType, Function.identity())));
    }

    /**
     * Test that source topology rollbacks remove all relevant data.
     *
     * @throws Exception To satisfy the compiler.
     */
    @Test
    public void testSourceTopologyRollback() throws Exception {
        final TopologyCreator<TopologyEntityDTO> creator = planEntityStore.newSourceTopologyCreator(
            TopologyInfo.newBuilder()
                .setTopologyContextId(PLAN_ID)
                .setTopologyId(SRC_ID)
                .build());
        creator.initialize();
        creator.addEntities(Collections.singletonList(vm), mock(TopologyID.class));
        creator.addEntities(Collections.singletonList(pm), mock(TopologyID.class));
        creator.rollback();

        checkPlanDataRemoved();
    }

    /**
     * Test that projected topology rollbacks remove all relevant data.
     *
     * @throws Exception To satisfy the compiler.
     */
    @Test
    public void testProjectedTopologyRollback() throws Exception {
        final TopologyCreator<ProjectedTopologyEntity> creator = planEntityStore.newProjectedTopologyCreator(
                PROJ_ID, TopologyInfo.newBuilder()
                        .setTopologyContextId(PLAN_ID)
                        .setTopologyId(SRC_ID)
                        .build());
        creator.initialize();
        creator.addEntities(Collections.singletonList(ProjectedTopologyEntity.newBuilder()
                .setOriginalPriceIndex(2)
                .setProjectedPriceIndex(2)
                .setEntity(vm)
                .build()), mock(TopologyID.class));
        creator.rollback();

        checkPlanDataRemoved();
    }

    /**
     * Test retrieving entities from the source topology with various filters.
     *
     * @throws Exception To satisfy the compiler.
     */
    @Test
    public void testSourceTopologyQueries() throws Exception {
        createSourceTopology();

        testFilterQueries(new TopologySelection(PLAN_ID, TopologyType.SOURCE, SRC_ID));
    }

    /**
     * Test listing all plans that have some data.
     *
     * @throws Exception To satisfy the compiler.
     */
    @Test
    public void testListPlans() throws Exception {
        createSourceTopology(1, 11);
        createSourceTopology(2, 22);
        createSourceTopology(3, 33);

        assertThat(planEntityStore.listRegisteredPlans(), containsInAnyOrder(1L, 2L, 3L));
    }

    /**
     * Test that, given a topology ID, the {@link PlanEntityStore} can return the plan ID and
     * topology type.
     *
     * @throws Exception To satisfy the compiler.
     */
    @Test
    public void testTopologySelection() throws Exception {
        createSourceTopology();
        createProjectedTopology(1, 1);

        TopologySelection topologySelection = planEntityStore.getTopologySelection(SRC_ID);
        assertThat(topologySelection.getPlanId(), is(PLAN_ID));
        assertThat(topologySelection.getTopologyType(), is(TopologyType.SOURCE));

        TopologySelection projTopologySelection = planEntityStore.getTopologySelection(PROJ_ID);
        assertThat(projTopologySelection.getPlanId(), is(PLAN_ID));
        assertThat(projTopologySelection.getTopologyType(), is(TopologyType.PROJECTED));
    }

    private void checkPlanDataRemoved() {
        try {
            planEntityStore.getTopologySelection(SRC_ID);
            Assert.fail("Source topology selection check should throw exception after plan deletion.");
        } catch (TopologyNotFoundException e) {
            // Expected.
        }

        try {
            planEntityStore.getTopologySelection(PROJ_ID);
            Assert.fail("Projected topology selection check should throw exception after plan deletion.");
        } catch (TopologyNotFoundException e) {
            // Expected.
        }

        try {
            planEntityStore.getTopologySelection(SRC_ID);
            Assert.fail("Source topology selection should fail after plan deletion.");
        } catch (TopologyNotFoundException e) {
            // Expected.
        }

        try {
            planEntityStore.getTopologySelection(PROJ_ID);
            Assert.fail("Projected topology selection should fail after plan deletion");
        } catch (TopologyNotFoundException e) {
            // Expected.
        }

        DSLContext dsl = dbConfig.getDslContext();
        assertThat(dsl.fetchCount(Tables.PLAN_ENTITY), is(0));
        assertThat(dsl.fetchCount(Tables.PLAN_ENTITY_SCOPE), is(0));
        assertThat(dsl.fetchCount(Tables.PRICE_INDEX), is(0));
        assertThat(dsl.fetchCount(Tables.TOPOLOGY_METADATA), is(0));
    }

    /**
     * Test that plan deletion deletes all the relevant plan data.
     *
     * @throws Exception To satisfy the compiler.
     */
    @Test
    public void testPlanDeletion() throws Exception {
        createSourceTopology();
        createProjectedTopology(1, 1);

        // Should not throw an exception.
        TopologySelection srcSelection = planEntityStore.getTopologySelection(SRC_ID);
        TopologySelection projSelection = planEntityStore.getTopologySelection(PROJ_ID);

        PlanEntityFilter srcFilter = new PlanEntityFilter(null, null, false);
        PlanEntityFilter projFilter = new PlanEntityFilter(null, null, false);
        // Get all entities from source.
        assertThat(planEntityStore.getPlanEntities(srcSelection, srcFilter, Type.FULL).count(), is(2L));
        // Get all entities from projected.
        assertThat(planEntityStore.getPlanEntities(projSelection, projFilter, Type.FULL).count(), is(2L));

        planEntityStore.deletePlanData(PLAN_ID);

        checkPlanDataRemoved();
    }

    /**
     * Test ingesting and retrieving topology entities from the projected topology.
     *
     * @throws Exception To satisfy the compiler.
     */
    @Test
    public void testProjectedTopologyRoundTrip() throws Exception {
        createProjectedTopology(1, 1);

        testFilterQueries(planEntityStore.getTopologySelection(PLAN_ID, TopologyType.PROJECTED));
    }

    /**
     * Test retrieving plan stats from the projected topology.
     *
     * @throws Exception To satisfy the compiler.
     */
    @Test
    public void testProjectedTopologyStats() throws Exception {
        createProjectedTopology(10, 20);

        testHackyStats(planEntityStore.getTopologySelection(PLAN_ID, TopologyType.PROJECTED), 10, 20);
    }

    /**
     * Test retrieving the supply chain (from the projected topology).
     *
     * @throws Exception To satisfy the compiler.
     */
    @Test
    public void testProjectedTopologySupplyChain() throws Exception {
        createProjectedTopology(10, 20);

        TopologySelection topologySelection = planEntityStore.getTopologySelection(PROJ_ID);
        SupplyChain vmRetSc = planEntityStore.getSupplyChain(topologySelection, SupplyChainScope.newBuilder()
                .addStartingEntityOid(vm.getOid())
                .build());
        assertThat(RepositoryDTOUtil.getAllMemberOids(vmRetSc),
                is(RepositoryDTOUtil.getAllMemberOids(vmSupplyChain)));

        SupplyChain pmRetSc = planEntityStore.getSupplyChain(topologySelection, SupplyChainScope.newBuilder()
                .addStartingEntityOid(pm.getOid())
                .build());
        assertThat(RepositoryDTOUtil.getAllMemberOids(pmRetSc),
                is(RepositoryDTOUtil.getAllMemberOids(pmSupplyChain)));

        SupplyChain combinedSc = planEntityStore.getSupplyChain(topologySelection, SupplyChainScope.getDefaultInstance());
        assertThat(RepositoryDTOUtil.getAllMemberOids(combinedSc),
            containsInAnyOrder(STORAGE_OID, pm.getOid(), CONTAINER_OID));

        SupplyChain stateFilterSc = planEntityStore.getSupplyChain(topologySelection, SupplyChainScope.newBuilder()
            .addEntityStatesToInclude(EntityState.POWERED_OFF)
            .build());
        assertThat(RepositoryDTOUtil.getAllMemberOids(stateFilterSc),
           containsInAnyOrder(CONTAINER_OID));
    }

    /**
     * Test retrieving plan stats from the source topology.
     *
     * @throws Exception To satisfy the compiler.
     */
    @Test
    public void testSourceTopologyStats() throws Exception {
        createSourceTopology();

        // The default is 1.
        testHackyStats(planEntityStore.getTopologySelection(PLAN_ID, TopologyType.SOURCE), 1, 1);
    }

    private void createSourceTopology() throws Exception {
        createSourceTopology(PLAN_ID, SRC_ID);
    }

    private void createSourceTopology(long planId, long topologyId) throws Exception {
        final TopologyCreator<TopologyEntityDTO> creator = planEntityStore.newSourceTopologyCreator(
                TopologyInfo.newBuilder()
                        .setTopologyContextId(planId)
                        .setTopologyId(topologyId)
                        .build());
        creator.initialize();
        creator.addEntities(Collections.singletonList(vm), mock(TopologyID.class));
        creator.addEntities(Collections.singletonList(pm), mock(TopologyID.class));
        creator.complete();
    }

    private void createProjectedTopology(double srcPriceIndex, double projPriceIndex) throws Exception {
        final TopologyCreator<ProjectedTopologyEntity> creator = planEntityStore.newProjectedTopologyCreator(
                PROJ_ID, TopologyInfo.newBuilder()
                        .setTopologyContextId(PLAN_ID)
                        .setTopologyId(SRC_ID)
                        .build());
        creator.initialize();
        creator.addEntities(Collections.singletonList(ProjectedTopologyEntity.newBuilder()
                .setOriginalPriceIndex(srcPriceIndex)
                .setProjectedPriceIndex(projPriceIndex)
                .setEntity(vm)
                .build()), mock(TopologyID.class));
        creator.addEntities(Collections.singletonList(ProjectedTopologyEntity.newBuilder()
                .setOriginalPriceIndex(srcPriceIndex)
                .setProjectedPriceIndex(projPriceIndex)
                .setEntity(pm)
                .build()), mock(TopologyID.class));
        creator.complete();

    }

    private void testHackyStats(TopologySelection topologySelection, double expectedSrcPriceIndex, double expectedProjPriceIndex)
            throws TopologyNotFoundException {
        PlanEntityFilter allFilter = new PlanEntityFilter(null, null, false);
        Iterator<List<ProjectedTopologyEntity>> it = planEntityStore.getHackyStatsEntities(
                topologySelection, allFilter);
        List<TopologyEntityDTO> e = new ArrayList<>();
        while (it.hasNext()) {
            List<ProjectedTopologyEntity> next = it.next();
            next.forEach(projE -> {
                assertThat(projE.getOriginalPriceIndex(), is(expectedSrcPriceIndex));
                assertThat(projE.getProjectedPriceIndex(), is(expectedProjPriceIndex));
                e.add(projE.getEntity());
            });
        }

        assertThat(e, containsInAnyOrder(vm, pm));
    }

    private void testFilterQueries(TopologySelection topologySelection)
            throws TopologyNotFoundException {

        // Query to get all entities.
        PlanEntityFilter allFilter = new PlanEntityFilter(null, null, false);
        List<TopologyEntityDTO> allRet = planEntityStore.getPlanEntities(topologySelection, allFilter, Type.FULL)
                .map(PartialEntity::getFullEntity)
                .collect(Collectors.toList());
        assertThat(allRet, containsInAnyOrder(vm, pm));

        // Query to get all unplaced entities.
        PlanEntityFilter unplacedFilter = new PlanEntityFilter(null, null, true);
        List<TopologyEntityDTO> unplacedRet = planEntityStore.getPlanEntities(topologySelection, unplacedFilter, Type.FULL)
                .map(PartialEntity::getFullEntity)
                .collect(Collectors.toList());
        assertThat(unplacedRet, containsInAnyOrder(vm));

        // Query to get entities by id.
        PlanEntityFilter idFilter = new PlanEntityFilter(Collections.singleton(7L), null, false);
        List<TopologyEntityDTO> idRet = planEntityStore.getPlanEntities(topologySelection, idFilter, Type.FULL)
                .map(PartialEntity::getFullEntity)
                .collect(Collectors.toList());
        assertThat(idRet, containsInAnyOrder(vm));

        // Query to get entities by type.
        PlanEntityFilter vmFilter = new PlanEntityFilter(null,
                Collections.singleton((short)ApiEntityType.VIRTUAL_MACHINE.typeNumber()), false);
        List<TopologyEntityDTO> retVms = planEntityStore.getPlanEntities(topologySelection, vmFilter, Type.FULL)
                .map(PartialEntity::getFullEntity)
                .collect(Collectors.toList());
        assertThat(retVms, containsInAnyOrder(vm));

    }

}