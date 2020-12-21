package com.vmturbo.systest.policy;

import static com.vmturbo.platform.common.builders.CommodityBuilders.coolingDegC;
import static com.vmturbo.platform.common.builders.CommodityBuilders.cpuMHz;
import static com.vmturbo.platform.common.builders.CommodityBuilders.memKB;
import static com.vmturbo.platform.common.builders.CommodityBuilders.netThroughputKBps;
import static com.vmturbo.platform.common.builders.CommodityBuilders.powerWatts;
import static com.vmturbo.platform.common.builders.CommodityBuilders.space;
import static com.vmturbo.platform.common.builders.CommodityBuilders.vMemKB;
import static com.vmturbo.platform.common.builders.EntityBuilders.io;
import static com.vmturbo.platform.common.builders.EntityBuilders.processor;
import static com.vmturbo.systest.policy.AnalysisResultsMatchers.fromHost;
import static com.vmturbo.systest.policy.AnalysisResultsMatchers.hasProvider;
import static com.vmturbo.systest.policy.AnalysisResultsMatchers.movesVm;
import static com.vmturbo.systest.policy.AnalysisResultsMatchers.toHost;
import static com.vmturbo.systest.policy.AnalysisResultsMatchers.withConsumerCount;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.grpc.stub.StreamObserver;
import io.opentracing.SpanContext;
import io.swagger.annotations.ApiOperation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.json.GsonHttpMessageConverter;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;
import org.springframework.web.util.UriComponentsBuilder;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.cost.Cost.ProjectedEntityCosts;
import com.vmturbo.common.protobuf.cost.Cost.ProjectedEntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.EntityFilters;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.EntityFilters.EntityFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.SearchParametersCollection;
import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.common.protobuf.group.PolicyDTO.Policy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo.AtMostNBoundPolicy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo.AtMostNPolicy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo.BindToComplementaryGroupPolicy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo.BindToGroupPolicy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyResponse;
import com.vmturbo.common.protobuf.group.PolicyServiceGrpc.PolicyServiceImplBase;
import com.vmturbo.common.protobuf.market.MarketNotification.AnalysisStatusNotification;
import com.vmturbo.common.protobuf.search.Search;
import com.vmturbo.common.protobuf.search.Search.ComparisonOperator;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.NumericFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.StoppingCondition;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.TraversalDirection;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.AnalysisSummary;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopology;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.Topology;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyBroadcastRequest;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyServiceGrpc;
import com.vmturbo.common.protobuf.topology.TopologyServiceGrpc.TopologyServiceBlockingStub;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.components.api.client.KafkaMessageConsumer;
import com.vmturbo.components.api.test.IntegrationTestServer;
import com.vmturbo.components.test.utilities.ComponentTestRule;
import com.vmturbo.components.test.utilities.communication.ComponentStubHost;
import com.vmturbo.components.test.utilities.component.ComponentCluster;
import com.vmturbo.components.test.utilities.component.ComponentUtils;
import com.vmturbo.components.test.utilities.component.DockerEnvironment;
import com.vmturbo.market.component.api.ActionsListener;
import com.vmturbo.market.component.api.MarketComponent;
import com.vmturbo.market.component.api.ProjectedTopologyListener;
import com.vmturbo.market.component.api.impl.MarketComponentNotificationReceiver;
import com.vmturbo.mediation.delegatingprobe.DelegatingProbe.DelegatingDiscoveryRequest;
import com.vmturbo.mediation.delegatingprobe.DelegatingProbeAccount;
import com.vmturbo.platform.common.builders.EntityBuilders;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.PowerState;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.topology.processor.api.AccountValue;
import com.vmturbo.topology.processor.api.DiscoveryStatus;
import com.vmturbo.topology.processor.api.EntitiesListener;
import com.vmturbo.topology.processor.api.ProbeInfo;
import com.vmturbo.topology.processor.api.ProbeListener;
import com.vmturbo.topology.processor.api.TargetData;
import com.vmturbo.topology.processor.api.TargetInfo;
import com.vmturbo.topology.processor.api.TargetListener;
import com.vmturbo.topology.processor.api.TopologyProcessor;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TopologyProcessorNotification;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorClient;

/**
 * A system test that brings up the TopologyProcessor and sends a specific topology to it.
 * A policy is set up and the results are sent to the market. After analysis, the output
 * is examined to ensure that actions appropriate to the policy are generated.
 */
public class PlacementPolicySysTest {
    private static final Logger logger = LogManager.getLogger();

    private TopologyServiceBlockingStub topologyService;

    private TopologyProcessor topologyProcessor;

    private MarketComponent marketComponent;

    private ExecutorService threadPool = Executors.newCachedThreadPool();

    private final PolicyServiceStub policyServiceStub = new PolicyServiceStub();

    private IMessageReceiver<TopologyProcessorNotification> tpMessageReceiver;
    private IMessageReceiver<Topology> tpTopologyReceiver;
    private IMessageReceiver<ProjectedTopology> projectedTopologyReceiver;
    private IMessageReceiver<ProjectedEntityCosts> projectedEntityCostReceiver;
    private IMessageReceiver<AnalysisSummary> analysisSummaryReceiver;
    private IMessageReceiver<AnalysisStatusNotification> analysisStatusReceiver;
    private IMessageReceiver<ProjectedEntityReservedInstanceCoverage> projectedEntityRiCoverageReceiver;
    private IMessageReceiver<ActionPlan> actionsReceiver;

    private DiscoveryDriverController discoveryDriverController;
    private KafkaMessageConsumer kafkaMessageConsumer;
    private IntegrationTestServer testServer;

    @Rule
    public ComponentTestRule componentTestRule = ComponentTestRule.newBuilder()
        .withComponentCluster(ComponentCluster.newBuilder()
            .withService(ComponentCluster.newService("topology-processor")
                .withConfiguration("groupHost", ComponentUtils.getDockerHostRoute())
                .logsToLogger(logger))
            .withService(ComponentCluster.newService("mediation-delegatingprobe")
                    .withConfiguration("serverHttpPort", "8080")
                    .withConfiguration("consul_host", "consul")
                    .withConfiguration("consul_port", "8500")
                    .withConfiguration("serverGrpcPort", "9001")
                .logsToLogger(logger))
            .withService(ComponentCluster.newService("market")
                .logsToLogger(logger)))
        .withStubs(ComponentStubHost.newBuilder()
            .withGrpcServices(policyServiceStub))
        .noMetricsCollection();

    @Rule
    public TestName testName = new TestName();

    // All hosts with the number two in their display name.
    private final GroupDefinition hostsWithTwo = GroupDefinition.newBuilder()
            .setEntityFilters(EntityFilters.newBuilder()
                    .addEntityFilter(EntityFilter.newBuilder()
                            .setEntityType(EntityType.PHYSICAL_MACHINE.getNumber())
                            .setSearchParametersCollection(SearchParametersCollection.newBuilder()
                                    .addSearchParameters(SearchParameters.newBuilder()
                                            .setStartingFilter(Search.PropertyFilter.newBuilder()
                                                    .setPropertyName("entityType")
                                                    .setNumericFilter(NumericFilter.newBuilder()
                                                            .setComparisonOperator(
                                                                    ComparisonOperator.EQ)
                                                            .setValue(
                                                                    EntityType.PHYSICAL_MACHINE.getNumber())))
                                            .addSearchFilter(SearchFilter.newBuilder()
                                                    .setPropertyFilter(
                                                            Search.PropertyFilter.newBuilder()
                                                                    .setPropertyName("displayName")
                                                                    .setStringFilter(
                                                                            StringFilter.newBuilder()
                                                                                    .setStringPropertyRegex(
                                                                                            ".*2.*"))))))))
            .build();

    // All hosts with the phrase "in-group" in their display name.
    private final GroupDefinition hostsWithInGroup = GroupDefinition.newBuilder()
            .setEntityFilters(EntityFilters.newBuilder()
                    .addEntityFilter(EntityFilter.newBuilder()
                            .setEntityType(EntityType.PHYSICAL_MACHINE.getNumber())
                            .setSearchParametersCollection(SearchParametersCollection.newBuilder()
                                    .addSearchParameters(SearchParameters.newBuilder()
                                            .setStartingFilter(Search.PropertyFilter.newBuilder()
                                                    .setPropertyName("entityType")
                                                    .setNumericFilter(NumericFilter.newBuilder()
                                                            .setComparisonOperator(
                                                                    ComparisonOperator.EQ)
                                                            .setValue(
                                                                    EntityType.PHYSICAL_MACHINE.getNumber())))
                                            .addSearchFilter(SearchFilter.newBuilder()
                                                    .setPropertyFilter(
                                                            Search.PropertyFilter.newBuilder()
                                                                    .setPropertyName("displayName")
                                                                    .setStringFilter(
                                                                            StringFilter.newBuilder()
                                                                                    .setStringPropertyRegex(
                                                                                            "in-group"))))))))
            .build();

    // All virtual machines consuming from physical machines.
    private final GroupDefinition vmsOnHosts = GroupDefinition.newBuilder()
            .setEntityFilters(EntityFilters.newBuilder()
                    .addEntityFilter(EntityFilter.newBuilder()
                            .setSearchParametersCollection(SearchParametersCollection.newBuilder()
                                    .addSearchParameters(SearchParameters.newBuilder()
                                            .setStartingFilter(Search.PropertyFilter.newBuilder()
                                                    .setPropertyName("entityType")
                                                    .setNumericFilter(NumericFilter.newBuilder()
                                                            .setComparisonOperator(
                                                                    ComparisonOperator.EQ)
                                                            .setValue(
                                                                    EntityType.PHYSICAL_MACHINE.getNumber())))
                                            .addSearchFilter(SearchFilter.newBuilder()
                                                    .setTraversalFilter(TraversalFilter.newBuilder()
                                                            .setTraversalDirection(
                                                                    TraversalDirection.PRODUCES)
                                                            .setStoppingCondition(
                                                                    StoppingCondition.newBuilder()
                                                                            .setStoppingPropertyFilter(
                                                                                    Search.PropertyFilter
                                                                                            .newBuilder()
                                                                                            .setPropertyName(
                                                                                                    "entityType")
                                                                                            .setNumericFilter(
                                                                                                    NumericFilter
                                                                                                            .newBuilder()
                                                                                                            .setComparisonOperator(
                                                                                                                    ComparisonOperator.EQ)
                                                                                                            .setValue(
                                                                                                                    EntityType.VIRTUAL_MACHINE
                                                                                                                            .getNumber()))))))))))
            .build();

    @Before
    public void setup() throws Exception {
        IdentityGenerator.initPrefix(0);
        testServer = new IntegrationTestServer(testName, ContextConfiguration.class);
        discoveryDriverController = testServer.getBean(DiscoveryDriverController.class);

        kafkaMessageConsumer = new KafkaMessageConsumer(DockerEnvironment.getKafkaBootstrapServers(),
                "placement-policy-system-tests");

        topologyService = TopologyServiceGrpc.newBlockingStub(
            componentTestRule.getCluster().newGrpcChannel("topology-processor"));
        tpTopologyReceiver = kafkaMessageConsumer.messageReceiver(
                TopologyProcessorClient.TOPOLOGY_LIVE, Topology::parseFrom);
        tpMessageReceiver =
                kafkaMessageConsumer.messageReceiver(TopologyProcessorClient.NOTIFICATIONS_TOPIC,
                        TopologyProcessorNotification::parseFrom);
        ;
        topologyProcessor = TopologyProcessorClient.rpcAndNotification(
            componentTestRule.getCluster().getConnectionConfig("topology-processor"),
            threadPool, tpMessageReceiver, tpTopologyReceiver, null, null, null);

        projectedTopologyReceiver = kafkaMessageConsumer.messageReceiver(
                MarketComponentNotificationReceiver.PROJECTED_TOPOLOGIES_TOPIC,
                ProjectedTopology::parseFrom);
        projectedEntityCostReceiver = kafkaMessageConsumer.messageReceiver(
                MarketComponentNotificationReceiver.PROJECTED_ENTITY_COSTS_TOPIC,
                ProjectedEntityCosts::parseFrom);
        projectedEntityRiCoverageReceiver = kafkaMessageConsumer.messageReceiver(
                MarketComponentNotificationReceiver.PROJECTED_ENTITY_RI_COVERAGE_TOPIC,
                ProjectedEntityReservedInstanceCoverage::parseFrom);
        actionsReceiver = kafkaMessageConsumer.messageReceiver(
                MarketComponentNotificationReceiver.ACTION_PLANS_TOPIC, ActionPlan::parseFrom);
        analysisSummaryReceiver = kafkaMessageConsumer.messageReceiver(
            MarketComponentNotificationReceiver.ANALYSIS_SUMMARY_TOPIC,
            TopologyDTO.AnalysisSummary::parseFrom);
        analysisStatusReceiver = kafkaMessageConsumer.messageReceiver(
                         MarketComponentNotificationReceiver.ANALYSIS_STATUS_NOTIFICATION_TOPIC,
                              AnalysisStatusNotification::parseFrom);
        marketComponent = new MarketComponentNotificationReceiver(
                projectedTopologyReceiver, projectedEntityCostReceiver, projectedEntityRiCoverageReceiver,
                actionsReceiver, analysisSummaryReceiver, analysisStatusReceiver,
                threadPool, 0);
        kafkaMessageConsumer.start();
    }

    @After
    public void teardown() {
        kafkaMessageConsumer.close();
        try {
            threadPool.shutdownNow();
            threadPool.awaitTermination(10, TimeUnit.MINUTES);
        } catch (Exception e) {
            logger.error("Failed to tear down in TopologyProcessorPerformanceTest!", e);
        }
    }

    TopologyResult topologyResult = null;

    @Rule
    public TestRule testFailureRule = new TestWatcher() {
        @Override
        protected void failed(Throwable e, Description description) {
            if (topologyResult != null) {
                System.out.println("Test " + description + " failed.");
                System.out.println("Source topology was: " + topologyResult.getEntities());
            }
        }

        @Override
        protected void succeeded(Description description) {
            System.out.println("Test " + description + " succeeded.");
        }
    };

    @Test
    public void testPolicies() throws Exception {
        // Set up a DelegationProbe target.
        addDelegatingProbeTarget();

        // The market may already be running an analysis on the first empty topology.
        // Since the market doesn't have an API to see if an analysis is running, wait
        // in the hope that there won't be an analysis in progress by the time we
        // start the below tests.
        Thread.sleep(1000);

        // Run the tests.
        testBindToGroupPolicy();
        testBindToComplementaryGroupPolicy();
        testAtMostNPolicy();
        testAtMostNBoundPolicy();
        testMustRunTogetherPolicy();
    }

    /**
     * Test the bindToGroup policy type by setting up a VM on a host with plenty of capacity to support it.
     * Apply a policy that moves the VM onto a host with less capacity. The only reason for this move
     * should be due to the policy.
     *
     * Topology:
     *
     * VM1  VM2
     *  |  /
     * PM1    PM2
     *  |      |
     *   \    /
     *    \  /
     *    DC1
     *
     * Set up a BindToGroup policy that forces VM1 and VM2 to move to PM2.
     */
    private void testBindToGroupPolicy() throws Exception {
        logger.info("testBindToGroupPolicy");

        final EntityDTO datacenter = datacenter();
        final EntityDTO pm1 = physicalMachine("pm-1", datacenter, 5.0, 5.0);
        final EntityDTO pm2 = physicalMachine("pm-2", datacenter, 5.0, 5.0);
        final EntityDTO vm1 = virtualMachine("vm-1", pm1);
        final EntityDTO vm2 = virtualMachine("vm-2", pm1);

        policyServiceStub.setPolicy(bindToGroup());

        discoveryDriverController.setDiscoveredEntities(Arrays.asList(datacenter, pm1, pm2, vm1, vm2));

        topologyResult = discoverAndBroadcast();
        final Map<String, Long> idMap = topologyResult.getIdMap();
        logger.info("Received {} entities", idMap.size());

        final AnalysisResults analysisResults = waitForAnalysis();
        final ActionPlan actionPlan = analysisResults.getActionPlan();

        logger.info("Received actionPlan with {} actions", actionPlan.getActionCount());

        final long vm1Id = idMap.get(vm1.getDisplayName());
        final long vm2Id = idMap.get(vm2.getDisplayName());
        final long pm1Id = idMap.get(pm1.getDisplayName());
        final long pm2Id = idMap.get(pm2.getDisplayName());

        assertThat(actionPlan, movesVm(vm1Id, fromHost(pm1Id), toHost(pm2Id)));
        assertThat(actionPlan, movesVm(vm2Id, fromHost(pm1Id), toHost(pm2Id)));
    }

    /**
     * Test the bindToComplementaryGroup policy type by setting up a VM1 on host PM2.
     * Apply a bindToComplementaryGroup policy that not allow VM1 running on host PM2. Because PM1
     * and PM2 capacity are exactly same, if it have move action to move VM1 from PM2 to PM1, the only
     * reason for this move should be due to the policy.
     *
     * Topology:
     *
     *       VM1
     *        |
     * PM1    PM2
     *  |      |
     *   \    /
     *    \  /
     *    DC1
     *
     * Set up a BindToComplementaryGroup policy that forces VM1 to move from PM2 to PM1.
     */
    private void testBindToComplementaryGroupPolicy() throws Exception {
        final EntityDTO datacenter = datacenter();
        final EntityDTO pm1 = physicalMachine("pm-1", datacenter, 5.0, 5.0);
        final EntityDTO pm2 = physicalMachine("pm-2", datacenter, 5.0, 5.0);
        final EntityDTO vm1 = virtualMachine("vm-1", pm2);

        policyServiceStub.setPolicy(bindToComplementaryGroup());
        discoveryDriverController.setDiscoveredEntities(Arrays.asList(datacenter, pm1, pm2, vm1));

        topologyResult = discoverAndBroadcast();
        final Map<String, Long> idMap = topologyResult.getIdMap();
        logger.info("Received {} entities", idMap.size());

        final AnalysisResults analysisResults = waitForAnalysis();
        final ActionPlan actionPlan = analysisResults.getActionPlan();
        logger.info("Received actionPlan with {} actions", actionPlan.getActionCount());
        final long vm1Id = idMap.get(vm1.getDisplayName());
        final long pm1Id = idMap.get(pm1.getDisplayName());
        final long pm2Id = idMap.get(pm2.getDisplayName());

        assertThat(actionPlan, movesVm(vm1Id, fromHost(pm2Id), toHost(pm1Id)));
    }

    /**
     * Test the atMostN policy type by setting up three VMs on a group of hosts where two of the hosts
     * have plenty of capacity to support and the last.
     * Apply a policy that forces the VMs to be moved in such a way that no two are allowed on any host
     * in the providers group and the host outside of the providers group has insufficient capacity
     * to support multiple hosts.
     *
     * The expectation is that in the below topology, VM1 or VM2 will be forced to move to PM2.
     *
     * Topology:
     *
     * VM1  VM2    VM3  <--- VM1,2,3 are all in the consumers group
     *  |  /        |
     * PM1    PM2  PM3  <--- PM1,2 are in the providers group, PM3 is not.
     *  |      |   |
     *   \    /   /
     *    \  /   /
     *    DC1---
     *
     * Set up a atMostN policy that does not permit multiple VMs on any of the PMs in the providers group.
     */
    private void testAtMostNPolicy() throws Exception {
        logger.info("testAtMostNPolicy");

        final EntityDTO datacenter = datacenter();
        final EntityDTO pm1 = physicalMachine("pm-in-group-1", datacenter, 5.0, 5.0);
        final EntityDTO pm2 = physicalMachine("pm-in-group-2", datacenter, 5.0, 5.0);
        final EntityDTO pm3 = physicalMachine("pm-out-of-group-3", datacenter, 1.9, 1.9);
        final EntityDTO vm1 = virtualMachine("vm-1", pm1);
        final EntityDTO vm2 = virtualMachine("vm-2", pm1);
        final EntityDTO vm3 = virtualMachine("vm-3", pm3);

        policyServiceStub.setPolicy(atMostN());

        discoveryDriverController.setDiscoveredEntities(Arrays.asList(datacenter, pm1, pm2, pm3, vm1, vm2, vm3));

        topologyResult = discoverAndBroadcast();
        final Map<String, Long> idMap = topologyResult.getIdMap();
        logger.info("Received {} entities", idMap.size());

        final AnalysisResults analysisResults = waitForAnalysis();
        final ActionPlan actionPlan = analysisResults.getActionPlan();
        final Map<Long, TopologyEntityDTO> projectedTopology = analysisResults.getProjectedTopologyMap();
        logger.info("Received actionPlan with {} actions", actionPlan.getActionCount());

        final long pm1Id = idMap.get(pm1.getDisplayName());
        final long pm2Id = idMap.get(pm2.getDisplayName());
        final long pm3Id = idMap.get(pm3.getDisplayName());

        // Each host should be hosting exactly 1 vm.
        assertThat(projectedTopology, hasProvider(pm1Id, withConsumerCount(1)));
        assertThat(projectedTopology, hasProvider(pm2Id, withConsumerCount(1)));
        assertThat(projectedTopology, hasProvider(pm3Id, withConsumerCount(1)));
    }

    /**
     * Test the atMostNBound policy type by setting up three VMs on a group of hosts where all of the
     * hosts have plenty of capacity to host the VMs.
     *
     * Apply a policy that forces the VMs to be balanced on the two hosts in the policy provider
     * group and none are allowed to run on the host outside the provider group.
     *
     * The expectation is that in the below topology, all VMs must be moved off PM3.
     * PM1 and PM2 can both run 2 VMs.
     *
     * Topology:
     *
     * VM1       VM2  VM3  VM4     <--- VM1,2,3,4 are all in the consumers group
     *  |          \  /   /
     * PM1    PM2  PM3---          <--- PM1,2 are in the providers group, PM3 is not.
     *  |      |   |
     *   \    /   /
     *    \  /   /
     *    DC1---
     *
     * Set up a atMostNBound policy that does not permit multiple VMs on any of the PMs in the providers group.
     */
    private void testAtMostNBoundPolicy() throws Exception {
        logger.info("testAtMostNBoundPolicy");

        final EntityDTO datacenter = datacenter();
        final EntityDTO pm1 = physicalMachine("pm-in-group-1", datacenter, 5.0, 5.0);
        final EntityDTO pm2 = physicalMachine("pm-in-group-2", datacenter, 5.0, 5.0);
        final EntityDTO pm3 = physicalMachine("pm-out-of-group-3", datacenter, 10.0, 10.0);
        final EntityDTO vm1 = virtualMachine("vm-1", pm1);
        final EntityDTO vm2 = virtualMachine("vm-2", pm3);
        final EntityDTO vm3 = virtualMachine("vm-3", pm3);
        final EntityDTO vm4 = virtualMachine("vm-4", pm3);

        policyServiceStub.setPolicy(atMostNBound());

        discoveryDriverController.setDiscoveredEntities(Arrays.asList(datacenter, pm1, pm2, pm3, vm1, vm2, vm3, vm4));

        topologyResult = discoverAndBroadcast();
        final Map<String, Long> idMap = topologyResult.getIdMap();
        logger.info("Received {} entities", idMap.size());

        final AnalysisResults analysisResults = waitForAnalysis();
        final ActionPlan actionPlan = analysisResults.getActionPlan();
        final Map<Long, TopologyEntityDTO> projectedTopology = analysisResults.getProjectedTopologyMap();
        logger.info("Received actionPlan with {} actions", actionPlan.getActionCount());

        final long pm1Id = idMap.get(pm1.getDisplayName());
        final long pm2Id = idMap.get(pm2.getDisplayName());
        final long pm3Id = idMap.get(pm3.getDisplayName());

        // Hosts 1 and 2 should each be hosting exactly 2 VMs.
        // Host 3 should be running 0 VMs.
        assertThat(projectedTopology, hasProvider(pm1Id, withConsumerCount(2)));
        assertThat(projectedTopology, hasProvider(pm2Id, withConsumerCount(2)));
        assertThat(projectedTopology, hasProvider(pm3Id, withConsumerCount(0)));
    }

    /**
     * Test the mustRunTogether policy type by setting up a VM1, VM2, VM3 and PM1, PM2.
     * Apply a mustRunTogether policy that must run VM1, VM2, VM3 on PM1 or PM2 together to force a move
     * action which move VM3 from PM2 to PM1, since PM1 has 2 VMs and PM2 only has 1 VMs.
     * Topology:
     *
     * VM1 VM2  VM3
     *  \ /     |
     * PM1     PM2
     *  |      /
     *   \    /
     *    \  /
     *    DC1
     *
     * Set up a MustRunTogether policy that forces VM3 to move from PM2 to PM1.
     */
    private void testMustRunTogetherPolicy() throws Exception {
        final EntityDTO datacenter = datacenter();
        final EntityDTO pm1 = physicalMachine("pm-in-group-1", datacenter, 5.0, 5.0);
        final EntityDTO pm2 = physicalMachine("pm-in-group-2", datacenter, 5.0, 5.0);
        final EntityDTO vm1 = virtualMachine("vm-1", pm1);
        final EntityDTO vm2 = virtualMachine("vm-2", pm1);
        final EntityDTO vm3 = virtualMachine("vm-3", pm2);

        policyServiceStub.setPolicy(mustRunTogether());
        discoveryDriverController.setDiscoveredEntities(Arrays.asList(datacenter, pm1, pm2, vm1, vm2, vm3));

        topologyResult = discoverAndBroadcast();
        final Map<String, Long> idMap = topologyResult.getIdMap();
        logger.info("Received {} entities", idMap.size());

        final AnalysisResults analysisResults = waitForAnalysis();
        final ActionPlan actionPlan = analysisResults.getActionPlan();
        logger.info("Received actionPlan with {} actions", actionPlan.getActionCount());
        final long vm3Id = idMap.get(vm3.getDisplayName());
        final long pm1Id = idMap.get(pm1.getDisplayName());
        final long pm2Id = idMap.get(pm2.getDisplayName());

        assertThat(actionPlan, movesVm(vm3Id, fromHost(pm2Id), toHost(pm1Id)));
    }

    /**
     * Add target of the probe type DelegatingProbe.
     *
     * @throws Exception
     */
    private void addDelegatingProbeTarget() throws Exception {
        discoveryDriverController.setDiscoveredEntities(Collections.singletonList(datacenter()));
        final String uri = UriComponentsBuilder.newInstance()
            .scheme("http")
            .host(ComponentUtils.getDockerHostRoute())
            .port(testServer.connectionConfig().getPort())
            .path("discoveryDriver")
            .build()
            .toUriString();

        final CompletableFuture<Long> probeFuture = new CompletableFuture<>();
        topologyProcessor.addProbeListener(new TestProbeListener(probeFuture));
        checkIfProbeAlreadyRegistered(topologyProcessor, probeFuture);
        final long probeId = probeFuture.get(10, TimeUnit.MINUTES);
        logger.info("Probe registered with id {}", probeId);

        final CompletableFuture<DiscoveryStatus> discoveryFuture = new CompletableFuture<>();
        final CompletableFuture<TargetInfo> targetFuture = new CompletableFuture<>();
        topologyProcessor.addTargetListener(new TestTargetListener(targetFuture, discoveryFuture));

        final DelegatingProbeAccount delegatingProbeAccount = new DelegatingProbeAccount(
            "delegating-probe", uri, "discover");
        topologyProcessor.addTarget(probeId, buildTargetData(delegatingProbeAccount));
        final TargetInfo target = targetFuture.get(10, TimeUnit.MINUTES);
        logger.info("Added target with id {} for probe {}", target.getId(), target.getProbeId());

        discoveryFuture.get(10, TimeUnit.MINUTES);
        logger.info("Initial discovery completed.");
    }

    /**
     * Perform a discovery and broadcast the discovered entities to the rest of the system.
     *
     * @return A map of entity displayName -> OID for all entities received in the broadcast.
     * @throws Exception If something goes wrong.
     */
    private TopologyResult discoverAndBroadcast() throws Exception {
        final CompletableFuture<DiscoveryStatus> discoveryFuture = new CompletableFuture<>();
        final CompletableFuture<TargetInfo> targetFuture = new CompletableFuture<>();

        topologyProcessor.addTargetListener(new TestTargetListener(targetFuture, discoveryFuture));
        topologyProcessor.discoverAllTargets();

        final DiscoveryStatus discovery = discoveryFuture.get(10, TimeUnit.MINUTES);
        logger.info("Discovery {} completed success={}", discovery.getId(), discovery.isSuccessful());

        // Request a broadcast and wait for it to complete.
        final CompletableFuture<TopologyResult> entitiesFuture = new CompletableFuture<>();
        topologyProcessor.addLiveTopologyListener(new TestEntitiesListener(entitiesFuture));
        topologyService.requestTopologyBroadcast(TopologyBroadcastRequest.getDefaultInstance());
        return entitiesFuture.get(10, TimeUnit.MINUTES);
    }

    /**
     * Wait for analysis to complete and return the generated {@link ActionPlan}ץ
     * @return The {@link ActionPlan} generated by analysis.
     * @throws Exception
     */
    private AnalysisResults waitForAnalysis() throws Exception {
        final CompletableFuture<AnalysisResults> analysisResultsFuture = new CompletableFuture<>();
        final TestAnalysisListener analysisListener = new TestAnalysisListener(analysisResultsFuture);

        marketComponent.addActionsListener(analysisListener);
        marketComponent.addProjectedTopologyListener(analysisListener);

        return analysisResultsFuture.get(10, TimeUnit.MINUTES);
    }

    /**
     * There is a chance the probe registration process has completed prior to our health check for the
     * stress probe container finishing. So check if the topology processor has already registered a probe.
     *
     * @param topologyProcessor The topology processor where the probe may have registered.
     * @param probeFuture The future to complete on probe registration.
     * @throws Exception If something goes wrong.
     */
    private void checkIfProbeAlreadyRegistered(@Nonnull final TopologyProcessor topologyProcessor,
                                               @Nonnull final CompletableFuture<Long> probeFuture) throws Exception {
        Set<ProbeInfo> probes = topologyProcessor.getAllProbes();
        if (probes.size() > 1) {
            throw new RuntimeException("There should not be multiple probes registered");
        } else if (probes.isEmpty()) {
            return; // No probes registered
        }

        // Complete the future with the registered probe
        probeFuture.complete(probes.iterator().next().getId());
    }

    private TargetData buildTargetData(@Nonnull final DelegatingProbeAccount account) {
        return new TargetData() {
            @Nonnull
            @Override
            public Set<AccountValue> getAccountData() {
                return new HashSet<>(Arrays.asList(
                    new TestAccountValue("targetId", account.getTargetId()),
                    new TestAccountValue("driverRootUri", account.getDriverRootUri()),
                    new TestAccountValue("driverEndpoint", account.getDriverEndpoint()))
                );
            }

            @Nonnull
            @Override
            public Optional<String> getCommunicationBindingChannel() {
                return Optional.empty();
            }
        };
    }

    private EntityDTO datacenter() {
        // Create discoveredEntities
        return EntityBuilders.datacenter("datacenter-1")
            .displayName("datacenter-1")
            .selling(space().capacity(100.0).used(1.0))
            .selling(powerWatts().capacity(100.0).used(1.0))
            .selling(coolingDegC().capacity(100.0).used(1.0))
            .build();
    }

    private static Policy bindToGroup() {
        return Policy.newBuilder()
            .setId(IdentityGenerator.next())
            .setPolicyInfo(PolicyInfo.newBuilder()
                .setBindToGroup(BindToGroupPolicy.newBuilder()
                    .setConsumerGroupId(policyGroupingID())
                    .setProviderGroupId(policyGroupingID())))
            .build();
    }

    private static Policy atMostN() {
        return Policy.newBuilder()
            .setId(IdentityGenerator.next())
            .setPolicyInfo(PolicyInfo.newBuilder()
                .setAtMostN(AtMostNPolicy.newBuilder()
                    .setCapacity(1.0f)
                    .setConsumerGroupId(policyGroupingID())
                    .setProviderGroupId(policyGroupingID())))
            .build();
    }

    private static Policy atMostNBound() {
        return Policy.newBuilder()
            .setId(IdentityGenerator.next())
            .setPolicyInfo(PolicyInfo.newBuilder()
                .setAtMostNbound(AtMostNBoundPolicy.newBuilder()
                    .setCapacity(2.0f)
                    .setConsumerGroupId(policyGroupingID())
                    .setProviderGroupId(policyGroupingID())))
            .build();
    }

    private static Policy bindToComplementaryGroup() {
        return Policy.newBuilder()
            .setId(IdentityGenerator.next())
            .setPolicyInfo(PolicyInfo.newBuilder()
                .setBindToComplementaryGroup(BindToComplementaryGroupPolicy.newBuilder()
                    .setConsumerGroupId(policyGroupingID())
                    .setProviderGroupId(policyGroupingID())))
            .build();
    }

    private static Policy mustRunTogether() {
        return Policy.newBuilder()
            .setId(IdentityGenerator.next())
            .setPolicyInfo(PolicyInfo.newBuilder()
                .setMustRunTogether(PolicyInfo.MustRunTogetherPolicy.newBuilder()
                    .setGroupId(policyGroupingID())
                    .build()))
            .build();
    }

    private static long policyGroupingID() {
        return IdentityGenerator.next();
    }

    /**
     * Nested configuration for Spring context. Allow construction and registration of the
     * controller for our test's REST API.
     */
    @Configuration
    @EnableWebMvc
    // The default @EnableAutoConfiguration implied by @SpringBootApplication will pick up
    // the spring security JAR in the class-path, and set up authentication for the
    // test application. We want to avoid dealing with that for the purpose of the test, so
    // we explicitly exclude security-related AutoConfiguration.
    static class ContextConfiguration extends WebMvcConfigurerAdapter {
        @Bean
        public DiscoveryDriverController discoveryDriverController() {
            return new DiscoveryDriverController();
        }

        @Bean
        public GsonHttpMessageConverter gsonHttpMessageConverter() {
            final GsonHttpMessageConverter msgConverter = new GsonHttpMessageConverter();
            msgConverter.setGson(ComponentGsonFactory.createGson());
            return msgConverter;
        }
    }

    /**
     * A REST controller that drives the delegating probe discovery response.
     */
    @RestController
    public static class DiscoveryDriverController {
        List<EntityDTO> discoveredEntities;

        public DiscoveryDriverController() {
            discoveredEntities = Collections.emptyList();
        }

        public void setDiscoveredEntities(@Nonnull final List<EntityDTO> discoveredEntities) {
            logger.info("Setting {} discovered entities", discoveredEntities.size());
            this.discoveredEntities = Objects.requireNonNull(discoveredEntities);
        }

        /**
         * Get discovery response.
         *
         * Note that unfortunately we must transmit the {@link DiscoveryResponse} because
         * ClassLoader issues that complicate deserialization on the client side in the probe
         * prevent transmission in a more understandable format.
         *
         * @return The discovery response.
         */
        @ApiOperation(value = "Run a discovery")
        @RequestMapping(path = "/discoveryDriver/discover",
            method = RequestMethod.POST,
            consumes = {MediaType.APPLICATION_JSON_VALUE},
            produces = {MediaType.APPLICATION_OCTET_STREAM_VALUE})
        @ResponseBody
        public @Nonnull
        ResponseEntity<byte[]> discover(@RequestBody DelegatingDiscoveryRequest request) throws Exception {
            logger.info("Handling discovery request and responding with {} entities", discoveredEntities.size());

            return new ResponseEntity<>(
                DiscoveryResponse.newBuilder()
                    .addAllEntityDTO(discoveredEntities)
                    .build()
                    .toByteArray(),
                HttpStatus.OK
            );
        }
    }

    /**
     * Listener for Topology Processor probe notifications.
     */
    private static class TestProbeListener implements ProbeListener {
        private final CompletableFuture<Long> probeRegistrationFuture;

        TestProbeListener(@Nonnull final CompletableFuture<Long> probeRegistrationFuture) {
            this.probeRegistrationFuture = probeRegistrationFuture;
        }

        @Override
        public void onProbeRegistered(@Nonnull TopologyProcessorDTO.ProbeInfo probe) {
            probeRegistrationFuture.complete(probe.getId());
        }
    }

    /**
     * Listener for Topology Processor target notifications.
     */
    private static class TestTargetListener implements TargetListener {
        private final CompletableFuture<TargetInfo> targetFuture;
        private final CompletableFuture<DiscoveryStatus> discoveryFuture;

        TestTargetListener(@Nonnull final CompletableFuture<TargetInfo> targetFuture,
                           @Nonnull final CompletableFuture<DiscoveryStatus> discoveryFuture) {
            this.targetFuture = targetFuture;
            this.discoveryFuture = discoveryFuture;
        }

        @Override
        public void onTargetAdded(@Nonnull TargetInfo target) {
            targetFuture.complete(target);
        }

        @Override
        public void onTargetDiscovered(@Nonnull DiscoveryStatus result) {
            if (result.isCompleted()) {
                discoveryFuture.complete(result);
            }
        }
    }

    private static class TopologyResult {
        /**
         * A map from displayName -> OID
         */
        private final Map<String, Long> idMap = new HashMap<>();

        private final Collection<TopologyEntityDTO> entities = new ArrayList<>();

        public TopologyResult() {

        }

        public Map<String, Long> getIdMap() {
            return idMap;
        }

        public Collection<TopologyEntityDTO> getEntities() {
            return entities;
        }
    }

    /**
     * Listener for Topology Processor topology broadcast notifications.
     */
    private static class TestEntitiesListener implements EntitiesListener {
        private final CompletableFuture<TopologyResult> entitiesFuture;

        private final TopologyResult topologyResult = new TopologyResult();

        public TestEntitiesListener(@Nonnull final CompletableFuture<TopologyResult> entitiesFuture) {
            this.entitiesFuture = entitiesFuture;
        }

        @Override
        public void onTopologyNotification(TopologyInfo topologyInfo,
                                           @Nonnull RemoteIterator<Topology.DataSegment> topologyDTOs,
                                           @Nonnull SpanContext tracingContext) {
            while (topologyDTOs.hasNext()) {
                try {
                    for (Topology.DataSegment entityDTO : topologyDTOs.nextChunk()) {
                        topologyResult.getIdMap().put(entityDTO.getEntity().getDisplayName(), entityDTO.getEntity().getOid());
                        topologyResult.getEntities().add(entityDTO.getEntity());
                    }
                } catch (Exception e) {
                    logger.error("Error during topology broadcast reading: ", e);
                    entitiesFuture.complete(null);
                }
            }

            entitiesFuture.complete(topologyResult);
        }
    }

    /**
     * A basic account value implementation to permit adding targets.
     */
    private static class TestAccountValue implements AccountValue {
        final String name;
        final String value;

        TestAccountValue(@Nonnull final String name, @Nonnull final String value) {
            this.name = name;
            this.value = value;
        }

        @Nonnull
        @Override
        public String getName() {
            return name;
        }

        @Nullable
        @Override
        public String getStringValue() {
            return value;
        }

        @Nullable
        @Override
        public List<List<String>> getGroupScopeProperties() {
            return Collections.emptyList();
        }
    }

    /**
     * A stub for the PolicyService that normally lives in the group component so that we can
     * guarantee certain policies get applied.
     */
    private class PolicyServiceStub extends PolicyServiceImplBase {
        private Policy policy = null;

        @Override
        public void getPolicies(final PolicyDTO.PolicyRequest request,
                                   final StreamObserver<PolicyResponse> responseObserver) {
            if (getPolicy() != null) {
                responseObserver.onNext(
                    PolicyResponse.newBuilder()
                        .setPolicy(getPolicy())
                        .build());
            }
            responseObserver.onCompleted();
        }

        public Policy getPolicy() {
            return policy;
        }

        public void setPolicy(Policy policy) {
            this.policy = policy;
        }
    }

    private static class AnalysisResults {
        private final ActionPlan actionPlan;
        private final Map<Long, TopologyEntityDTO> projectedTopologyMap;
        private final List<TopologyEntityDTO> projectedEntities;

        public AnalysisResults(@Nonnull final ActionPlan actionPlan,
                               @Nonnull final Map<Long, TopologyEntityDTO> projectedTopology,
                               @Nonnull final List<TopologyEntityDTO> projectedEntities) {
            this.actionPlan = Objects.requireNonNull(actionPlan);
            this.projectedTopologyMap = Objects.requireNonNull(projectedTopology);
            this.projectedEntities = projectedEntities;
        }

        public ActionPlan getActionPlan() {
            return actionPlan;
        }

        public Map<Long, TopologyEntityDTO> getProjectedTopologyMap() {
            return projectedTopologyMap;
        }

        public Collection<TopologyEntityDTO> getProjectedEntities() {
            return projectedEntities;
        }
    }

    /**
     * A listener for actions that we expect to receive.
     */
    private static class TestAnalysisListener implements ActionsListener, ProjectedTopologyListener {
        private final CompletableFuture<AnalysisResults> analysisResultsFuture;

        private ActionPlan actionPlan = null;
        private Map<Long, TopologyEntityDTO> projectedTopology = null;
        private List<TopologyEntityDTO> projectedEntities = null;

        public TestAnalysisListener(@Nonnull final CompletableFuture<AnalysisResults> analysisResultsFuture) {
            this.analysisResultsFuture = Objects.requireNonNull(analysisResultsFuture);
        }

        @Override
        public void onActionsReceived(@Nonnull final ActionPlan actionPlan,
                                      @Nonnull final SpanContext tracingContext) {
            this.actionPlan = Objects.requireNonNull(actionPlan);

            if (isComplete()) {
                this.analysisResultsFuture.complete(new AnalysisResults(
                    this.actionPlan, this.projectedTopology, this.projectedEntities));
            }
        }

        @Override
        public void onProjectedTopologyReceived(final long projectedTopologyId,
                                                @Nonnull final TopologyInfo sourceTopologyInfo,
                                                @Nonnull final RemoteIterator<ProjectedTopologyEntity> projectedTopology,
                                                @Nonnull final SpanContext tracingContext) {
            this.projectedTopology = new HashMap<>();
            this.projectedEntities = new ArrayList<>();

            try {
                while (projectedTopology.hasNext()) {
                    projectedTopology.nextChunk().forEach(entity -> {
                        this.projectedTopology.put(entity.getEntity().getOid(), entity.getEntity());
                        this.projectedEntities.add(entity.getEntity());
                    });
                }
            } catch (InterruptedException | CommunicationException | TimeoutException e) {
                logger.error(e);
                this.analysisResultsFuture.completeExceptionally(e);
                return;
            }

            if (isComplete()) {
                this.analysisResultsFuture.complete(
                    new AnalysisResults(this.actionPlan, this.projectedTopology, this.projectedEntities));
            }
        }

        private boolean isComplete() {
            return this.actionPlan != null && this.projectedTopology != null;
        }
    }

    private EntityDTO physicalMachine(@Nonnull final String name, @Nonnull final EntityDTO dataCenter,
                                      final double cpuCapacity, final double memCapacity) {
        return EntityBuilders.physicalMachine(name)
            .displayName(name)
            .numCpuCores(12)
            .io(io("io-1").speedKbps(1000.0 * 1000.0).macAddress("79:67:AD:88:02:38"))
            .io(io("io-2").speedKbps(1000.0 * 1000.0).macAddress("CC:40:88:D6:AD:AA"))
            .processor(processor("FirstProc/" + name).displayName("FirstProc/" + name).capacityMhz(cpuCapacity))
            .powerState(PowerState.POWERED_ON)
            .maintenance(false)
            .selling(memKB().capacity(memCapacity).used(1.0))
            .selling(cpuMHz().capacity(cpuCapacity).used(1.0))
            .selling(netThroughputKBps().capacity(100.0).used(1.0))
            .buying(coolingDegC().used(1.0).from(dataCenter.getId()))
            .buying(powerWatts().used(1.0).from(dataCenter.getId()))
            .buying(space().used(1.0).from(dataCenter.getId()))
            .build();
    }

    private EntityDTO virtualMachine(@Nonnull final String name, @Nonnull final EntityDTO host) {
        return EntityBuilders.virtualMachine(name)
            .displayName(name)
            .processor(processor("FirstProc/" + name).displayName("FirstProc/" + name).capacityMhz(1.0))
            .powerState(PowerState.POWERED_ON)
            .selling(vMemKB().capacity(1.0).used(0.5))
            .selling(cpuMHz().capacity(1.0).used(0.5))
            .selling(netThroughputKBps().capacity(100.0).used(1.0))
            .buying(memKB().used(1.0).from(host.getId()))
            .buying(cpuMHz().used(1.0).from(host.getId()))
            .build();
    }
}
