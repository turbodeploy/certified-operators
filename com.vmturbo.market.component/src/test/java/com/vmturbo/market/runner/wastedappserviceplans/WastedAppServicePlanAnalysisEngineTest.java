package com.vmturbo.market.runner.wastedappserviceplans;

import static com.vmturbo.trax.Trax.trax;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.cloud.common.topology.CloudTopology;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.AnalysisSettings;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
import com.vmturbo.cost.calculation.journal.CostJournal;
import com.vmturbo.cost.calculation.topology.TopologyCostCalculator;
import com.vmturbo.cost.calculation.topology.TopologyCostCalculator.TopologyCostCalculatorFactory;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Unit tests for {@link WastedAppServicePlanAnalysisEngine}.
 */
public class WastedAppServicePlanAnalysisEngineTest {
    private static final long STORAGE_AMOUNT_CAPACITY = 250; // Premium Capacity
    private static final long VMEM_AMOUNT_CAPACITY = 16384; // P2v3 Capacity
    private static final long VCPU_AMOUNT_CAPACITY = 8; // Fake CPU capacity
    private static final long RESPONSE_TIME_AMOUNT_CAPACITY = 8; // Fake response time
    private final long topologyContextId = 1111;
    private final long topologyId = 2222;
    private final TopologyType topologyType = TopologyType.REALTIME;

    private final TopologyInfo topologyInfo = TopologyInfo.newBuilder().setTopologyContextId(
            topologyContextId).setTopologyId(topologyId).setTopologyType(topologyType).build();

    private final WastedAppServicePlanAnalysisEngine wastedAppServicePlanAnalysisEngine =
            new WastedAppServicePlanAnalysisEngine();

    /**
     * Common setup code.
     */
    @Before
    public void before() {
        IdentityGenerator.initPrefix(0L);
    }

    private static final String PLAN_KIND = "Plan Kind";

    private static final String PLAN_SIZE = "Plan Size";

    private static final TopologyEntityDTO.Builder EASTUS = createRegion();

    private static TopologyEntityDTO.Builder createRegion() {
        final TopologyEntityDTO.Builder regionBuilder = TopologyEntityDTO.newBuilder().setOid(
                4923534534L).setEntityType(EntityType.REGION_VALUE).setEnvironmentType(
                EnvironmentType.CLOUD);
        regionBuilder.setDisplayName("azure-East US");
        return regionBuilder;
    }

    private static TopologyEntityDTO.Builder createApp(final long oid, String name) {
        // Note: ASP modeled as Service for now. This will change to App Component Spec later.
        final TopologyEntityDTO.Builder builder = TopologyEntityDTO.newBuilder()
                .setOid(oid)
                .setEntityType(EntityType.SERVICE_VALUE)
                .setEnvironmentType(EnvironmentType.CLOUD);
        builder.setDisplayName("APPLICATION-" + name).addCommoditySoldList(
                CommoditySoldDTO.newBuilder()
                        .setCommodityType(CommodityType.newBuilder()
                                .setType(CommodityDTO.CommodityType.RESPONSE_TIME.getNumber()))
                        .setUsed(RESPONSE_TIME_AMOUNT_CAPACITY)
                        .setCapacity(RESPONSE_TIME_AMOUNT_CAPACITY));
        return builder;
    }

    private static TopologyEntityDTO.Builder createASP(final long oid, String name) {
        return createASP(oid, name, true);
    }

    private static TopologyEntityDTO.Builder createASP(final long oid, String name,
                                                       boolean controllable) {
        return createASP(oid, name, controllable, EntityType.APPLICATION_COMPONENT_VALUE );
    }

    private static TopologyEntityDTO.Builder createASP(final long oid, String name,
                                                       boolean controllable, int entityType) {
        final TopologyEntityDTO.Builder builder = TopologyEntityDTO.newBuilder()
                .setOid(oid)
                .setEntityType(entityType)
                .setEnvironmentType(EnvironmentType.CLOUD);
        // Note: ASP modeled as AppComponent for now. This will change to VMSPEC later.
        builder.setDisplayName("ASP-" + name).addCommoditySoldList(
                CommoditySoldDTO.newBuilder()
                        .setCommodityType(CommodityType.newBuilder()
                                .setType(CommodityDTO.CommodityType.STORAGE_AMOUNT.getNumber()))
                        .setUsed(STORAGE_AMOUNT_CAPACITY)
                        .setCapacity(STORAGE_AMOUNT_CAPACITY)).addCommoditySoldList(
                CommoditySoldDTO.newBuilder()
                        .setCommodityType(CommodityType.newBuilder()
                                .setType(CommodityDTO.CommodityType.VCPU.getNumber()))
                        .setUsed(VCPU_AMOUNT_CAPACITY)
                        .setCapacity(VCPU_AMOUNT_CAPACITY)).addCommoditySoldList(
                CommoditySoldDTO.newBuilder()
                        .setCommodityType(CommodityType.newBuilder()
                                .setType(CommodityDTO.CommodityType.VMEM.getNumber()))
                        .setUsed(VMEM_AMOUNT_CAPACITY)
                        .setCapacity(VMEM_AMOUNT_CAPACITY)).putEntityPropertyMap(PLAN_KIND,
                "Premium v3").addCommoditiesBoughtFromProviders(
                CommoditiesBoughtFromProvider.newBuilder()
                        .setProviderEntityType(EntityType.COMPUTE_TIER_VALUE)
                        .addCommodityBought(CommodityBoughtDTO.newBuilder()
                                .setCommodityType(CommodityType.newBuilder()
                                        .setType(
                                                CommodityDTO.CommodityType.LICENSE_ACCESS.getNumber())
                                        .setKey("Linux_AppServicePlan")))).putEntityPropertyMap(
                PLAN_SIZE, "P2v3").setAnalysisSettings(
                AnalysisSettings.newBuilder().setControllable(controllable).build());
        return builder;
    }

    private static void connectEntities(TopologyEntityDTO.Builder from,
            TopologyEntityDTO.Builder to) {
        from.addConnectedEntityList(ConnectedEntity.newBuilder()
                .setConnectedEntityId(to.getOid())
                .setConnectedEntityType(to.getEntityType()));
    }

    private static void createCommodityLink(@Nonnull final TopologyEntityDTO.Builder consumer,
            @Nonnull final TopologyEntityDTO.Builder producer) {
        consumer.addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .setProviderId(producer.getOid())
                .setProviderEntityType(producer.getEntityType()));
    }

    private void buildPlanAndAssociatedApps(Map<Long, TopologyEntityDTO> topology,
            Map<TopologyEntityDTO.Builder, List<TopologyEntityDTO.Builder>> plansAndTheirApps) {
        for (Entry<TopologyEntityDTO.Builder, List<TopologyEntityDTO.Builder>> entry : plansAndTheirApps.entrySet()) {
            TopologyEntityDTO.Builder plan = entry.getKey();
            List<TopologyEntityDTO.Builder> apps = entry.getValue();

            // Connect plan to region
            connectEntities(plan, EASTUS);
            // Connect apps to plan
            for (TopologyEntityDTO.Builder app : apps) {
                createCommodityLink(app, plan);
                connectEntities(app, plan);
            }
            // Build everything (also set entity property map w/ total app count)
            plan.putEntityPropertyMap("Total App Count", apps.size() + "");
            topology.put(plan.getOid(), plan.build());
            for (TopologyEntityDTO.Builder app : apps) {
                createCommodityLink(app, plan);
                connectEntities(app, plan);
                topology.put(app.getOid(), app.build());
            }
            topology.put(EASTUS.getOid(), EASTUS.build());
        }
    }

    private void mockCostJournal(Map<Long, TopologyEntityDTO> topology,
            TopologyCostCalculator cloudCostCalculator) {
        // Ensure cost returns something so it can properly generate an action. NOTE: ApplicationComponent will switch to VMSPEC at a later date.
        // TODO: remove EntityType.APPLICATION_COMPONENT_VALUE once Legacy Model for Application Service is removed.
        topology.values().stream().filter(
                dto -> ImmutableSet.of( EntityType.APPLICATION_COMPONENT_VALUE,  EntityType.VIRTUAL_MACHINE_SPEC_VALUE)
                        .contains(dto.getEntityType())).forEach(
                dto -> {
                    CostJournal<TopologyEntityDTO> costJournal = mock(CostJournal.class);
                    when(costJournal.getTotalHourlyCost()).thenReturn(trax(10d * dto.getOid()));
                    when(cloudCostCalculator.calculateCostForEntity(any(), eq(dto))).thenReturn(
                            Optional.of(costJournal));
                });
    }

    /**
     * Test whether controllable analysis setting turns on and off actions.
     */
    @Test
    public void testGenerateActionControllableTurnedOff() {
        // Create topology
        Map<Long, TopologyEntityDTO> topology = new HashMap<>();

        // Create one ASP connected to an app.
        final long planOid = 1L;
        final long appOid = 2L;
        Map<TopologyEntityDTO.Builder, List<TopologyEntityDTO.Builder>> utilized = new HashMap<>();
        utilized.put(createASP(planOid, "testPLAN", false),
                Collections.singletonList(createApp(appOid, "testAPP")));
        buildPlanAndAssociatedApps(topology, utilized);

        // Build one wasted app;
        final long wastedPlanOid = 3L;
        Map<TopologyEntityDTO.Builder, List<TopologyEntityDTO.Builder>> wasted = new HashMap<>();
        wasted.put(createASP(wastedPlanOid, "testWASTED", false), new ArrayList<>());
        buildPlanAndAssociatedApps(topology, wasted);

        // Mocks for cost
        final TopologyCostCalculator cloudCostCalculator = mock(TopologyCostCalculator.class);
        when(cloudCostCalculator.getCloudCostData()).thenReturn(CloudCostData.empty());
        final TopologyCostCalculatorFactory cloudCostCalculatorFactory = mock(
                TopologyCostCalculatorFactory.class);
        final CloudTopology<TopologyEntityDTO> originalCloudTopology = mock(CloudTopology.class);
        when(cloudCostCalculatorFactory.newCalculator(topologyInfo,
                originalCloudTopology)).thenReturn(cloudCostCalculator);
        mockCostJournal(topology, cloudCostCalculator);

        // Generate the analysis with one used app, one unused with controllable OFF.
        final WastedAppServicePlanResults analysis =
                wastedAppServicePlanAnalysisEngine.analyzeWastedAppServicePlans(topologyInfo,
                        topology, cloudCostCalculator, originalCloudTopology);
        // Expect to see that no ASP sits unused.
        Collection<Action> actions = analysis.getActions();
        assertEquals(0, actions.size());
    }

    /**
     * Test what happens when there is a used ASP and one unused ASP in an environment.
     */
    @Test
    public void testGenerateActionForWastedASPOneWastedOneUsed() {
        // Create topology
        Map<Long, TopologyEntityDTO> topology = new HashMap<>();

        // Create one ASP connected to an app.
        final long planOid = 1L;
        final long appOid = 2L;
        Map<TopologyEntityDTO.Builder, List<TopologyEntityDTO.Builder>> utilized = new HashMap<>();
        utilized.put(createASP(planOid, "testPLAN"),
                Collections.singletonList(createApp(appOid, "testAPP")));
        buildPlanAndAssociatedApps(topology, utilized);

        // Build one wasted app;
        final long wastedPlanOid = 3L;
        Map<TopologyEntityDTO.Builder, List<TopologyEntityDTO.Builder>> wasted = new HashMap<>();
        wasted.put(createASP(wastedPlanOid, "testWASTED"), new ArrayList<>());
        buildPlanAndAssociatedApps(topology, wasted);

        // Mocks for cost
        final TopologyCostCalculator cloudCostCalculator = mock(TopologyCostCalculator.class);
        when(cloudCostCalculator.getCloudCostData()).thenReturn(CloudCostData.empty());
        final TopologyCostCalculatorFactory cloudCostCalculatorFactory = mock(
                TopologyCostCalculatorFactory.class);
        final CloudTopology<TopologyEntityDTO> originalCloudTopology = mock(CloudTopology.class);
        when(cloudCostCalculatorFactory.newCalculator(topologyInfo,
                originalCloudTopology)).thenReturn(cloudCostCalculator);
        mockCostJournal(topology, cloudCostCalculator);

        // Generate the analysis with one used app, one unused.
        final WastedAppServicePlanResults analysis =
                wastedAppServicePlanAnalysisEngine.analyzeWastedAppServicePlans(topologyInfo,
                        topology, cloudCostCalculator, originalCloudTopology);
        // Expect to see that one ASP sits unused.
        Collection<Action> actions = analysis.getActions();
        assertEquals(1, actions.size());
        // Make sure we got back the wasted ASP and not a different one.
        Action action = actions.toArray(actions.toArray(new Action[1]))[0];
        assertEquals(wastedPlanOid, action.getInfo().getDelete().getTarget().getId());
    }

    /**
     * Test what happens when there are no wasted ASPs in an environment.
     */
    @Test
    public void testGenerateActionForWastedASPNoWastedASPs() {
        // Create topology
        Map<Long, TopologyEntityDTO> topology = new HashMap<>();

        // Create two ASPs each connected to an app.
        final long planOid = 1L;
        final long appOid = 2L;
        final long plan2Oid = 3L;
        final long app2Oid = 4L;
        Map<TopologyEntityDTO.Builder, List<TopologyEntityDTO.Builder>> utilized = new HashMap<>();
        utilized.put(createASP(planOid, "testPLAN"),
                Collections.singletonList(createApp(appOid, "testAPP")));
        utilized.put(createASP(plan2Oid, "testPLAN2"),
                Collections.singletonList(createApp(app2Oid, "testAPP2")));
        buildPlanAndAssociatedApps(topology, utilized);

        // Mocks for cost
        final TopologyCostCalculator cloudCostCalculator = mock(TopologyCostCalculator.class);
        when(cloudCostCalculator.getCloudCostData()).thenReturn(CloudCostData.empty());
        final TopologyCostCalculatorFactory cloudCostCalculatorFactory = mock(
                TopologyCostCalculatorFactory.class);
        final CloudTopology<TopologyEntityDTO> originalCloudTopology = mock(CloudTopology.class);
        when(cloudCostCalculatorFactory.newCalculator(topologyInfo,
                originalCloudTopology)).thenReturn(cloudCostCalculator);
        mockCostJournal(topology, cloudCostCalculator);

        final WastedAppServicePlanResults analysis =
                wastedAppServicePlanAnalysisEngine.analyzeWastedAppServicePlans(topologyInfo,
                        topology, cloudCostCalculator, originalCloudTopology);
        // Expect to see that no ASP sits unused since both have apps attached to them.
        Collection<Action> actions = analysis.getActions();
        assertEquals(0, actions.size());
    }

    /**
     * Test what happens when there are only wasted ASPs in an environment.
     */
    @Test
    public void testGenerateActionForWastedASPAllWastedASPs() {
        // Create topology
        Map<Long, TopologyEntityDTO> topology = new HashMap<>();

        // Create two wasted apps. There will be no used apps.
        final long planOid = 1L;
        final long plan2Oid = 2L;
        Map<TopologyEntityDTO.Builder, List<TopologyEntityDTO.Builder>> wasted = new HashMap<>();
        wasted.put(createASP(planOid, "testPLAN"), new ArrayList<>());
        wasted.put(createASP(plan2Oid, "testPLAN2"), new ArrayList<>());
        buildPlanAndAssociatedApps(topology, wasted);

        // Mocks for cost
        final TopologyCostCalculator cloudCostCalculator = mock(TopologyCostCalculator.class);
        when(cloudCostCalculator.getCloudCostData()).thenReturn(CloudCostData.empty());
        final TopologyCostCalculatorFactory cloudCostCalculatorFactory = mock(
                TopologyCostCalculatorFactory.class);
        final CloudTopology<TopologyEntityDTO> originalCloudTopology = mock(CloudTopology.class);
        when(cloudCostCalculatorFactory.newCalculator(topologyInfo,
                originalCloudTopology)).thenReturn(cloudCostCalculator);
        mockCostJournal(topology, cloudCostCalculator);

        final WastedAppServicePlanResults analysis =
                wastedAppServicePlanAnalysisEngine.analyzeWastedAppServicePlans(topologyInfo,
                        topology, cloudCostCalculator, originalCloudTopology);
        // Expect to see that there are two ASPs that have delete actions
        Collection<Action> actions = analysis.getActions();
        assertEquals(2, actions.size());
    }

    /**
     * Test what happens when there are only wasted ASPs in an environment.
     */
    @Test
    public void testGenerateDeleteActionForVMSpec() {
        // Create topology
        Map<Long, TopologyEntityDTO> topology = new HashMap<>();

        // Create two wasted apps. There will be no used apps.
        final long planOid = 1L;
        final long plan2Oid = 2L;
        Map<TopologyEntityDTO.Builder, List<TopologyEntityDTO.Builder>> wasted = new HashMap<>();
        wasted.put(createASP(planOid, "testPLAN", true, EntityType.VIRTUAL_MACHINE_SPEC_VALUE), new ArrayList<>());
        wasted.put(createASP(plan2Oid, "testPLAN2", true, EntityType.VIRTUAL_MACHINE_SPEC_VALUE), new ArrayList<>());
        buildPlanAndAssociatedApps(topology, wasted);

        // Mocks for cost
        final TopologyCostCalculator cloudCostCalculator = mock(TopologyCostCalculator.class);
        when(cloudCostCalculator.getCloudCostData()).thenReturn(CloudCostData.empty());
        final TopologyCostCalculatorFactory cloudCostCalculatorFactory = mock(
                TopologyCostCalculatorFactory.class);
        final CloudTopology<TopologyEntityDTO> originalCloudTopology = mock(CloudTopology.class);
        when(cloudCostCalculatorFactory.newCalculator(topologyInfo,
                originalCloudTopology)).thenReturn(cloudCostCalculator);
        mockCostJournal(topology, cloudCostCalculator);

        final WastedAppServicePlanResults analysis =
                wastedAppServicePlanAnalysisEngine.analyzeWastedAppServicePlans(topologyInfo,
                        topology, cloudCostCalculator, originalCloudTopology);
        // Expect to see that there are two ASPs that have delete actions
        Collection<Action> actions = analysis.getActions();
        assertEquals(2, actions.size());
    }

    /**
     * Test what happens when there is an ASP with multiple apps on it + one wasted.
     */
    @Test
    public void testGenerateActionForWastedASPGeneralCaseButOneASPHasMultipleApps() {
        // Create an ASP connected to multiple apps.
        final long planOid = 1L;
        final long appOid = 2L;
        final long app2Oid = 10L;
        final long app3Oid = 11L;
        // Create one ASP connected to three apps.
        Map<Long, TopologyEntityDTO> topology = new HashMap<>();
        Map<TopologyEntityDTO.Builder, List<TopologyEntityDTO.Builder>> utilized = new HashMap<>();
        utilized.put(createASP(planOid, "testPLAN"),
                Arrays.asList(createApp(appOid, "testAPP"), createApp(app2Oid, "testAPP2"),
                        createApp(app3Oid, "testAPP3")));
        buildPlanAndAssociatedApps(topology, utilized);

        // Create an ASP with no apps.
        final long wastedPlanOid = 3L;
        Map<TopologyEntityDTO.Builder, List<TopologyEntityDTO.Builder>> wasted = new HashMap<>();
        wasted.put(createASP(wastedPlanOid, "testWASTED"), new ArrayList<>());
        buildPlanAndAssociatedApps(topology, wasted);

        // Mocks for cost
        final TopologyCostCalculator cloudCostCalculator = mock(TopologyCostCalculator.class);
        when(cloudCostCalculator.getCloudCostData()).thenReturn(CloudCostData.empty());
        final TopologyCostCalculatorFactory cloudCostCalculatorFactory = mock(
                TopologyCostCalculatorFactory.class);
        final CloudTopology<TopologyEntityDTO> originalCloudTopology = mock(CloudTopology.class);
        when(cloudCostCalculatorFactory.newCalculator(topologyInfo,
                originalCloudTopology)).thenReturn(cloudCostCalculator);
        mockCostJournal(topology, cloudCostCalculator);

        // Generate the analysis with one used app, one unused.
        final WastedAppServicePlanResults analysis =
                wastedAppServicePlanAnalysisEngine.analyzeWastedAppServicePlans(topologyInfo,
                        topology, cloudCostCalculator, originalCloudTopology);
        // Expect to see that one ASP sits unused.
        Collection<Action> actions = analysis.getActions();
        assertEquals(1, actions.size());
        // Make sure we got back the wasted ASP and not a different one.
        Action action = actions.toArray(actions.toArray(new Action[1]))[0];
        assertEquals(wastedPlanOid, action.getInfo().getDelete().getTarget().getId());
    }

    private static void createNonASPSameModel(Map<Long, TopologyEntityDTO> topology,
            final long oidBuyer, final long oidSeller, String name) {
        final TopologyEntityDTO.Builder buyer = TopologyEntityDTO.newBuilder()
                .setOid(oidBuyer)
                .setEntityType(EntityType.APPLICATION_COMPONENT_VALUE)
                .setEnvironmentType(EnvironmentType.CLOUD);
        // Note: ASP modeled as AppComponent for now. This will change to VMSPEC later.
        buyer.setDisplayName("DatabaseServerAppComponent-" + name).addCommoditySoldList(
                CommoditySoldDTO.newBuilder()
                        .setCommodityType(CommodityType.newBuilder()
                                .setType(CommodityDTO.CommodityType.STORAGE_AMOUNT.getNumber()))
                        .setUsed(STORAGE_AMOUNT_CAPACITY)
                        .setCapacity(STORAGE_AMOUNT_CAPACITY)).addCommoditySoldList(
                CommoditySoldDTO.newBuilder()
                        .setCommodityType(CommodityType.newBuilder()
                                .setType(CommodityDTO.CommodityType.VCPU.getNumber()))
                        .setUsed(VCPU_AMOUNT_CAPACITY)
                        .setCapacity(VCPU_AMOUNT_CAPACITY)).addCommoditySoldList(
                CommoditySoldDTO.newBuilder()
                        .setCommodityType(CommodityType.newBuilder()
                                .setType(CommodityDTO.CommodityType.VMEM.getNumber()))
                        .setUsed(VMEM_AMOUNT_CAPACITY)
                        .setCapacity(VMEM_AMOUNT_CAPACITY)).putEntityPropertyMap(
                "Max Concurrent Sessions", "10000").putEntityPropertyMap("Max Concurrent Workers",
                "100");
        final TopologyEntityDTO.Builder seller = TopologyEntityDTO.newBuilder()
                .setOid(oidSeller)
                .setEntityType(EntityType.DATABASE_SERVER_VALUE)
                .setEnvironmentType(EnvironmentType.CLOUD);
        seller.setDisplayName("DatabaseServer-" + name).addCommoditySoldList(
                CommoditySoldDTO.newBuilder()
                        .setCommodityType(CommodityType.newBuilder()
                                .setType(CommodityDTO.CommodityType.STORAGE_AMOUNT.getNumber()))
                        .setUsed(STORAGE_AMOUNT_CAPACITY)
                        .setCapacity(STORAGE_AMOUNT_CAPACITY)).addCommoditySoldList(
                CommoditySoldDTO.newBuilder()
                        .setCommodityType(CommodityType.newBuilder()
                                .setType(CommodityDTO.CommodityType.VCPU.getNumber()))
                        .setUsed(VCPU_AMOUNT_CAPACITY)
                        .setCapacity(VCPU_AMOUNT_CAPACITY)).addCommoditySoldList(
                CommoditySoldDTO.newBuilder()
                        .setCommodityType(CommodityType.newBuilder()
                                .setType(CommodityDTO.CommodityType.VMEM.getNumber()))
                        .setUsed(VMEM_AMOUNT_CAPACITY)
                        .setCapacity(VMEM_AMOUNT_CAPACITY)).putEntityPropertyMap(
                "Max Concurrent Sessions", "10000").putEntityPropertyMap("Max Concurrent Workers",
                "100");
        createCommodityLink(buyer, seller);
        connectEntities(buyer, seller);
        topology.put(oidBuyer, buyer.build());
        topology.put(oidSeller, seller.build());
    }

    /**
     * Test what happens in a realistic environment with multiple used and multiple wasted ASPs +
     * some entities of same type ie VirtualMachineSpec that aren't App Service Plans.
     */
    @Test
    public void testGenerateActionForWastedASPMixedEnvironmentMultipleWastedMultipleUtilized() {
        // Create entities modeled the same as ASPs
        final long junkOid = 50L;
        final TopologyEntityDTO.Builder builder = TopologyEntityDTO.newBuilder()
                .setOid(junkOid)
                .setEntityType(EntityType.APPLICATION_COMPONENT_VALUE)
                .setEnvironmentType(EnvironmentType.CLOUD);
        builder.setDisplayName("JUNK-" + junkOid).addCommoditySoldList(
                CommoditySoldDTO.newBuilder()
                        .setCommodityType(CommodityType.newBuilder()
                                .setType(CommodityDTO.CommodityType.RESPONSE_TIME.getNumber()))
                        .setUsed(RESPONSE_TIME_AMOUNT_CAPACITY)
                        .setCapacity(RESPONSE_TIME_AMOUNT_CAPACITY));

        // Create two ASPs each connected to an app.
        Map<Long, TopologyEntityDTO> topology = new HashMap<>();
        final long planOid = 1L;
        final long appOid = 2L;
        final long plan2Oid = 10L;
        final long app2Oid = 11L;
        Map<TopologyEntityDTO.Builder, List<TopologyEntityDTO.Builder>> utilized = new HashMap<>();
        utilized.put(createASP(planOid, "testPLAN"),
                Collections.singletonList(createApp(appOid, "testAPP")));
        utilized.put(createASP(plan2Oid, "testPLAN2"),
                Collections.singletonList(createApp(app2Oid, "testAPP2")));
        buildPlanAndAssociatedApps(topology, utilized);

        // Create two ASPs each with no apps.
        final long wastedPlanOid = 3L;
        final long wastedPlanOid2 = 20L;
        Map<TopologyEntityDTO.Builder, List<TopologyEntityDTO.Builder>> wasted = new HashMap<>();
        wasted.put(createASP(wastedPlanOid, "testWASTED"), new ArrayList<>());
        wasted.put(createASP(wastedPlanOid2, "testWASTED2"), new ArrayList<>());
        buildPlanAndAssociatedApps(topology, wasted);

        // Create some non ASPs that are modeled the same (app component)
        createNonASPSameModel(topology, 5436425L, 95664L, "RandomDatabaseServer");
        createNonASPSameModel(topology, 12334523532L, 546452L, "RandomDatabaseServer2");

        // Mocks for cost
        final TopologyCostCalculator cloudCostCalculator = mock(TopologyCostCalculator.class);
        when(cloudCostCalculator.getCloudCostData()).thenReturn(CloudCostData.empty());
        final TopologyCostCalculatorFactory cloudCostCalculatorFactory = mock(
                TopologyCostCalculatorFactory.class);
        final CloudTopology<TopologyEntityDTO> originalCloudTopology = mock(CloudTopology.class);
        when(cloudCostCalculatorFactory.newCalculator(topologyInfo,
                originalCloudTopology)).thenReturn(cloudCostCalculator);
        mockCostJournal(topology, cloudCostCalculator);

        // Generate the analysis with one used app, one unused.
        final WastedAppServicePlanResults analysis =
                wastedAppServicePlanAnalysisEngine.analyzeWastedAppServicePlans(topologyInfo,
                        topology, cloudCostCalculator, originalCloudTopology);
        // Expect to see that two ASP sit unused and the non ASPs weren't counted
        Collection<Action> actions = analysis.getActions();
        assertEquals(2, actions.size());

        // Make sure we got back the wasted ASPs
        Set<Long> wastedPlanIds = new HashSet<>(Arrays.asList(wastedPlanOid, wastedPlanOid2));
        actions.forEach(action -> {
            long id = action.getInfo().getDelete().getTarget().getId();
            assertTrue(wastedPlanIds.contains(id));
            wastedPlanIds.remove(id);
        });
        assertEquals("Found unexpected wasted plans", 0, wastedPlanIds.size());
    }
}
