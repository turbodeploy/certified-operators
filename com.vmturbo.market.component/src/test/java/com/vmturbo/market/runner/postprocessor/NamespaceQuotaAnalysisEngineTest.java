package com.vmturbo.market.runner.postprocessor;

import static com.vmturbo.market.runner.postprocessor.NamespaceQuotaAnalysisEngine.COMPARISON_EPSILON;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PlanTopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Test {@link NamespaceQuotaAnalysisEngine}.
 */
public class NamespaceQuotaAnalysisEngineTest {

    private final NamespaceQuotaAnalysisEngine namespaceQuotaAnalysisEngine = new NamespaceQuotaAnalysisEngine();
    private final TopologyInfo realTimeTopologyInfo = TopologyInfo.newBuilder()
        .setTopologyType(TopologyType.REALTIME)
        .setTopologyId(1234)
        .setTopologyContextId(12345)
        .build();
    private final TopologyInfo planTopologyInfo = TopologyInfo.newBuilder()
        .setTopologyType(TopologyType.PLAN)
        .setTopologyId(5678)
        .setTopologyContextId(56789)
        .setPlanInfo(PlanTopologyInfo.newBuilder())
        .build();

    private final long containerOID1 = 11;
    private final long containerOID2 = 12;
    private final long containerOID3 = 13;
    private final long podOID1 = 21;
    private final long podOID2 = 22;
    private final long podOID3 = 23;
    private final long workloadControllerOID = 31;
    private final long namespaceOID = 41;

    // Set up topology:
    //
    // container1     container2    container3
    //     |              |              |
    //    pod1           pod2          pod3
    //     \              /              |
    //     workloadController           /
    //              \                  /
    //                   namespace
    private final ProjectedTopologyEntity container1 =
        ProjectedTopologyEntity.newBuilder()
            .setEntity(TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.CONTAINER_VALUE)
                .setOid(containerOID1)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                    .setProviderId(podOID1)))
            .build();
    private final ProjectedTopologyEntity container2 =
        ProjectedTopologyEntity.newBuilder()
            .setEntity(TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.CONTAINER_VALUE)
                .setOid(containerOID2)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                    .setProviderId(podOID2)))
            .build();
    private final ProjectedTopologyEntity container3 =
        ProjectedTopologyEntity.newBuilder()
            .setEntity(TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.CONTAINER_VALUE)
                .setOid(containerOID3)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                    .setProviderId(podOID3)))
            .build();
    private final ProjectedTopologyEntity pod1 =
        ProjectedTopologyEntity.newBuilder()
            .setEntity(TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.CONTAINER_POD_VALUE)
                .setOid(podOID1)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                    .setProviderId(workloadControllerOID)))
            .build();
    private final ProjectedTopologyEntity pod2 =
        ProjectedTopologyEntity.newBuilder()
            .setEntity(TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.CONTAINER_POD_VALUE)
                .setOid(podOID2)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                    .setProviderId(workloadControllerOID)))
            .build();
    private final ProjectedTopologyEntity pod3 =
        ProjectedTopologyEntity.newBuilder()
            .setEntity(TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.CONTAINER_POD_VALUE)
                .setOid(podOID2)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                    .setProviderId(namespaceOID)
                    .setProviderEntityType(EntityType.NAMESPACE_VALUE)))
            .build();
    private final ProjectedTopologyEntity workloadController =
        ProjectedTopologyEntity.newBuilder()
            .setEntity(TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.WORKLOAD_CONTROLLER_VALUE)
                .setOid(workloadControllerOID)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                    .setProviderId(namespaceOID)))
            .build();
    private final ProjectedTopologyEntity namespace =
        ProjectedTopologyEntity.newBuilder()
            .setEntity(TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.NAMESPACE_VALUE)
                .setOid(namespaceOID)
                .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                    .setCommodityType(CommodityType.newBuilder()
                        .setType(CommodityDTO.CommodityType.VCPU_LIMIT_QUOTA_VALUE))))
            .build();
    private final Map<Long, ProjectedTopologyEntity> projectedEntities = new HashMap<>();

    /**
     * Set up the test environment.
     */
    @Before
    public void setup() {
        IdentityGenerator.initPrefix(0);

        projectedEntities.put(containerOID1, container1);
        projectedEntities.put(containerOID2, container2);
        projectedEntities.put(containerOID3, container3);
        projectedEntities.put(podOID1, pod1);
        projectedEntities.put(podOID2, pod2);
        projectedEntities.put(podOID3, pod3);
        projectedEntities.put(workloadControllerOID, workloadController);
        projectedEntities.put(namespaceOID, namespace);
    }

    /**
     * Test NamespaceQuotaAnalysisEngineTest.execute on real-time topology.
     *
     * <p>Given:
     * Namespace quota capacity: 1000
     * Container1 resize: 500 -> 200
     * Container2 resize: 500 -> 800
     *
     * <p>Resize up action will be generated on namespace 1000 -> 1300.
     */
    @Test
    public void testExecuteWithActionInRealTime() {
        TopologyEntityDTO origNamespace = createNamespaceEntityDTO(1000, 1000);
        final Map<Long, TopologyEntityDTO> originalEntities = ImmutableMap.of(namespaceOID, origNamespace);

        Action containerResize1 = createContainerResizeAction(containerOID1, CommodityDTO.CommodityType.VCPU_VALUE, 500, 200);
        Action containerResize2 = createContainerResizeAction(containerOID2, CommodityDTO.CommodityType.VCPU_VALUE, 500, 800);
        List<Action> actions = Arrays.asList(containerResize1, containerResize2);

        List<Action> nsResizeActions = namespaceQuotaAnalysisEngine.execute(realTimeTopologyInfo, originalEntities, projectedEntities, actions);
        assertEquals(1, nsResizeActions.size());
        assertEquals(1300, nsResizeActions.get(0).getInfo().getResize().getNewCapacity(), COMPARISON_EPSILON);

        double projectedCommCapacity = projectedEntities.get(namespaceOID).getEntity().getCommoditySoldListList().stream()
            .filter(comm -> comm.getCommodityType().getType() == CommodityDTO.CommodityType.VCPU_LIMIT_QUOTA_VALUE)
            .findAny()
            .map(CommoditySoldDTO::getCapacity)
            .orElse(0.0);
        assertEquals(1300, projectedCommCapacity, COMPARISON_EPSILON);
    }

    /**
     * Test NamespaceQuotaAnalysisEngineTest.execute on real-time topology.
     *
     * <p>Given:
     * Namespace quota capacity: 1500
     * Container1 resize: 500 -> 200
     * Container2 resize: 500 -> 800
     *
     * <p>No resize up action will be generated on namespace.
     */
    @Test
    public void testExecuteWithNoActionInRealTime() {
        TopologyEntityDTO origNamespace = createNamespaceEntityDTO(1000, 1500);
        final Map<Long, TopologyEntityDTO> originalEntities = ImmutableMap.of(namespaceOID, origNamespace);

        Action containerResize1 = createContainerResizeAction(containerOID1, CommodityDTO.CommodityType.VCPU_VALUE, 500, 200);
        Action containerResize2 = createContainerResizeAction(containerOID2, CommodityDTO.CommodityType.VCPU_VALUE, 500, 800);
        List<Action> actions = Arrays.asList(containerResize1, containerResize2);

        List<Action> nsResizeActions = namespaceQuotaAnalysisEngine.execute(realTimeTopologyInfo, originalEntities, projectedEntities, actions);
        assertEquals(0, nsResizeActions.size());
    }

    /**
     * Test NamespaceQuotaAnalysisEngineTest.execute on real-time topology with bare pod.
     *
     * <p>Given:
     * Namespace quota capacity: 1000
     * Container1 resize: 500 -> 200
     * Container3 (on bare pod) resize: 500 -> 800
     *
     * <p>Resize up action will be generated on namespace 1000 -> 1300.
     */
    @Test
    public void testExecuteWithWithActionWithBarePodInRealTime() {
        TopologyEntityDTO origNamespace = createNamespaceEntityDTO(1000, 1000);
        final Map<Long, TopologyEntityDTO> originalEntities = ImmutableMap.of(namespaceOID, origNamespace);

        Action containerResize1 = createContainerResizeAction(containerOID1, CommodityDTO.CommodityType.VCPU_VALUE, 500, 200);
        Action containerResize2 = createContainerResizeAction(containerOID3, CommodityDTO.CommodityType.VCPU_VALUE, 500, 800);
        List<Action> actions = Arrays.asList(containerResize1, containerResize2);

        List<Action> nsResizeActions = namespaceQuotaAnalysisEngine.execute(realTimeTopologyInfo, originalEntities, projectedEntities, actions);
        assertEquals(1, nsResizeActions.size());
        assertEquals(1300, nsResizeActions.get(0).getInfo().getResize().getNewCapacity(), COMPARISON_EPSILON);

        double projectedCommCapacity = projectedEntities.get(namespaceOID).getEntity().getCommoditySoldListList().stream()
            .filter(comm -> comm.getCommodityType().getType() == CommodityDTO.CommodityType.VCPU_LIMIT_QUOTA_VALUE)
            .findAny()
            .map(CommoditySoldDTO::getCapacity)
            .orElse(0.0);
        assertEquals(1300, projectedCommCapacity, COMPARISON_EPSILON);
    }

    /**
     * Test NamespaceQuotaAnalysisEngineTest.execute on plan topology.
     *
     * <p>Given:
     * Namespace quota capacity: 1000
     * Container1 resize: 500 -> 200
     * Container2 resize: 500 -> 900
     *
     * <p>Resize up action will be generated on namespace 1000 -> 1100.
     */
    @Test
    public void testExecuteWithActionInPlan() {
        TopologyEntityDTO origNamespace = createNamespaceEntityDTO(1000, 1000);
        final Map<Long, TopologyEntityDTO> originalEntities = ImmutableMap.of(namespaceOID, origNamespace);

        Action containerResize1 = createContainerResizeAction(containerOID1, CommodityDTO.CommodityType.VCPU_VALUE, 500, 200);
        Action containerResize2 = createContainerResizeAction(containerOID2, CommodityDTO.CommodityType.VCPU_VALUE, 500, 900);
        List<Action> actions = Arrays.asList(containerResize1, containerResize2);

        List<Action> nsResizeActions = namespaceQuotaAnalysisEngine.execute(planTopologyInfo, originalEntities, projectedEntities, actions);
        assertEquals(1, nsResizeActions.size());
        assertEquals(1100, nsResizeActions.get(0).getInfo().getResize().getNewCapacity(), COMPARISON_EPSILON);

        double projectedCommCapacity = projectedEntities.get(namespaceOID).getEntity().getCommoditySoldListList().stream()
            .filter(comm -> comm.getCommodityType().getType() == CommodityDTO.CommodityType.VCPU_LIMIT_QUOTA_VALUE)
            .findAny()
            .map(CommoditySoldDTO::getCapacity)
            .orElse(0.0);
        assertEquals(1100, projectedCommCapacity, COMPARISON_EPSILON);
    }

    /**
     * Test NamespaceQuotaAnalysisEngineTest.execute on plan topology.
     *
     * <p>Given:
     * Namespace quota capacity: 1000
     * Container1 resize: 500 -> 200
     * Container2 resize: 500 -> 800
     *
     * <p>No resize up action will be generated on namespace.
     */
    @Test
    public void testExecuteWithNoActionInPlan() {
        TopologyEntityDTO origNamespace = createNamespaceEntityDTO(1000, 1000);
        final Map<Long, TopologyEntityDTO> originalEntities = ImmutableMap.of(namespaceOID, origNamespace);

        Action containerResize1 = createContainerResizeAction(containerOID1, CommodityDTO.CommodityType.VCPU_VALUE, 500, 200);
        Action containerResize2 = createContainerResizeAction(containerOID2, CommodityDTO.CommodityType.VCPU_VALUE, 500, 800);
        List<Action> actions = Arrays.asList(containerResize1, containerResize2);

        List<Action> nsResizeActions = namespaceQuotaAnalysisEngine.execute(planTopologyInfo, originalEntities, projectedEntities, actions);
        assertEquals(0, nsResizeActions.size());
    }

    private TopologyEntityDTO createNamespaceEntityDTO(final float commSoldUsed, final float commSoldCapacity) {
        return TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.NAMESPACE_VALUE)
            .setOid(namespaceOID)
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                    .setType(CommodityDTO.CommodityType.VCPU_LIMIT_QUOTA_VALUE))
                .setUsed(commSoldUsed)
                .setCapacity(commSoldCapacity))
            .build();
    }

    private Action createContainerResizeAction(final long containerOID, final int commodityType,
                                               final float oldCapacity, final float newCapacity) {
        return Action.newBuilder()
            .setId(IdentityGenerator.next())
            .setExplanation(Explanation.newBuilder())
            .setDeprecatedImportance(-1.0d)
            .setInfo(ActionInfo.newBuilder()
                .setResize(Resize.newBuilder()
                    .setTarget(ActionEntity.newBuilder()
                        .setType(EntityType.CONTAINER_VALUE)
                        .setId(containerOID))
                    .setCommodityType(CommodityType.newBuilder()
                        .setType(commodityType))
                    .setOldCapacity(oldCapacity)
                    .setNewCapacity(newCapacity)))
            .build();
    }
}
