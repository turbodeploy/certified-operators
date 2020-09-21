package com.vmturbo.components.test.performance.action.orchestrator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import io.grpc.Channel;
import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import tec.units.ri.unit.MetricPrefix;

import com.vmturbo.action.orchestrator.api.ActionsListener;
import com.vmturbo.action.orchestrator.api.impl.ActionOrchestratorNotificationReceiver;
import com.vmturbo.action.orchestrator.dto.ActionMessages.ActionOrchestratorNotification;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.DeleteActionsRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.DeleteActionsResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionRequest.ActionQuery;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionResponse.TypeCase;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionsUpdated;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.MultiEntityRequest;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.SeverityCountsResponse;
import com.vmturbo.common.protobuf.action.EntitySeverityServiceGrpc;
import com.vmturbo.common.protobuf.action.EntitySeverityServiceGrpc.EntitySeverityServiceBlockingStub;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTOMoles.RepositoryServiceMole;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceImplBase;
import com.vmturbo.common.protobuf.topology.Probe.GetProbeActionCapabilitiesRequest;
import com.vmturbo.common.protobuf.topology.Probe.GetProbeActionCapabilitiesResponse;
import com.vmturbo.common.protobuf.topology.Probe.ProbeActionCapability;
import com.vmturbo.common.protobuf.topology.Probe.ProbeActionCapability.ActionCapability;
import com.vmturbo.common.protobuf.topology.Probe.ProbeActionCapability.ActionCapabilityElement;
import com.vmturbo.common.protobuf.topology.ProbeActionCapabilitiesServiceGrpc.ProbeActionCapabilitiesServiceImplBase;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntityBatch;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.components.api.client.KafkaMessageConsumer;
import com.vmturbo.components.test.utilities.ComponentTestRule;
import com.vmturbo.components.test.utilities.alert.Alert;
import com.vmturbo.components.test.utilities.communication.ComponentStubHost;
import com.vmturbo.components.test.utilities.component.ComponentCluster;
import com.vmturbo.components.test.utilities.component.ComponentUtils;
import com.vmturbo.components.test.utilities.component.DockerEnvironment;
import com.vmturbo.components.test.utilities.utils.ActionPlanGenerator;
import com.vmturbo.market.MarketNotificationSender;
import com.vmturbo.market.api.MarketKafkaSender;

@Alert({"ao_populate_store_duration_seconds_sum{store_type='Live'}/5minutes",
        "ao_populate_store_duration_seconds_sum{store_type='Plan'}/5minutes",
        "ao_delete_plan_action_plan_duration_seconds_sum",
        "jvm_memory_bytes_used_max"})
public class ActionOrchestratorPerformanceTest {

    private static final Logger logger = LogManager.getLogger();

    private EntityServiceStub entityServiceStub = new EntityServiceStub();

    private Channel actionOrchestratorChannel;

    @Rule
    public ComponentTestRule componentTestRule = ComponentTestRule.newBuilder()
        .withComponentCluster(ComponentCluster.newBuilder()
            .withService(ComponentCluster.newService("action-orchestrator")
                .withConfiguration("marketHost", ComponentUtils.getDockerHostRoute())
                .withConfiguration("groupHost", ComponentUtils.getDockerHostRoute())
                .withConfiguration("topologyProcessorHost", ComponentUtils.getDockerHostRoute())
                .withConfiguration("repositoryHost", ComponentUtils.getDockerHostRoute())
                .withMemLimit(3.5, MetricPrefix.GIGA)
                .logsToLogger(logger)))
        .withStubs(ComponentStubHost.newBuilder()
                .withGrpcServices(new ProbeActionCapabilitiesServiceStub(),
                    entityServiceStub,
                    // Empty group service returns no clusters/groups.
                    new GroupServiceMole(),
                    new RepositoryServiceMole()))
        .scrapeClusterAndLocalMetricsToInflux();

    @BeforeClass
    public static void setupClass() {
        IdentityGenerator.initPrefix(0);
    }

    private ActionOrchestratorNotificationReceiver actionOrchestrator;
    private ActionsServiceBlockingStub actionsService;
    private EntitySeverityServiceBlockingStub severitiesService;
    private KafkaMessageConsumer messageConsumer;
    private ExecutorService threadPool = Executors.newCachedThreadPool();
    private MarketNotificationSender marketNotificationSender;

    private static final int SEVERITY_PARTITION_COUNT = 8;

    @Before
    public void setup() {
        actionOrchestratorChannel = componentTestRule.getCluster().newGrpcChannel("action-orchestrator");

        messageConsumer = new KafkaMessageConsumer(DockerEnvironment.getKafkaBootstrapServers(),
                "action-orchestrator-perf-test");
        final IMessageReceiver<ActionOrchestratorNotification> messageReceiver =
                messageConsumer.messageReceiver(
                        ActionOrchestratorNotificationReceiver.ACTIONS_TOPIC,
                        ActionOrchestratorNotification::parseFrom);
        actionsService = ActionsServiceGrpc.newBlockingStub(actionOrchestratorChannel);
        severitiesService = EntitySeverityServiceGrpc.newBlockingStub(actionOrchestratorChannel);
        marketNotificationSender =
                MarketKafkaSender.createMarketSender(componentTestRule.getKafkaMessageProducer());
        actionOrchestrator =
                new ActionOrchestratorNotificationReceiver(messageReceiver, threadPool, 0);
        messageConsumer.start();
    }

    @After
    public void teardown() {
        try {
            messageConsumer.close();
            threadPool.shutdownNow();
            threadPool.awaitTermination(10, TimeUnit.MINUTES);
        } catch (Exception e) {
            logger.error("Failed to tear down in ActionOrchestratorPerformanceTest!", e);
        }
    }

    @Test
    public void test100kActionPlan() throws Exception {
        testPlanActionPlan(100_000);
        testLiveActionPlan(100_000);
    }

    @Test
    public void test200kActionPlan() throws Exception  {
        testPlanActionPlan(200_000);
        testLiveActionPlan(200_000);
    }

    @Test
    public void test300kActionPlan() throws Exception  {
        testPlanActionPlan(300_000);
        testLiveActionPlan(300_000);
    }

    @Test
    public void test400kActionPlan() throws Exception  {
        testPlanActionPlan(400_000);
        testLiveActionPlan(400_000);
    }

    /**
     * 1. Send a live action plan.
     * 2. Fetch the live action plan.
     */
    public void testLiveActionPlan(int actionPlanSize) throws Exception {
        final ActionPlanGenerator actionPlanGenerator = new ActionPlanGenerator();
        final ActionPlan sendActionPlan = actionPlanGenerator.generate(actionPlanSize, 1,
            ComponentUtils.REALTIME_TOPOLOGY_CONTEXT);
        entityServiceStub.loadEntitiesForActionPlan(sendActionPlan);
        populateActions(actionOrchestrator, sendActionPlan, "LIVE");

        fetchActions(actionsService, FilteredActionRequest.newBuilder().addActionQuery(
                ActionQuery.newBuilder().setQueryFilter(
                    ActionQueryFilter.newBuilder().setVisible(true))
            ).setTopologyContextId(ComponentUtils.REALTIME_TOPOLOGY_CONTEXT)
                .build(), "VISIBLE LIVE");

        fetchActions(actionsService, FilteredActionRequest.newBuilder()
                .setTopologyContextId(ComponentUtils.REALTIME_TOPOLOGY_CONTEXT)
                .build(), "ALL LIVE");
        fetchSeverities(actionPlanSize);
    }

    /**
     * 1. Send a plan action plan.
     * 2. Fetch the plan action plan.
     * 3. Delete the plan action plan.
     */
    public void testPlanActionPlan(int actionPlanSize) throws Exception {
        final long planContextId = 0xABC;

        final ActionPlanGenerator actionPlanGenerator = new ActionPlanGenerator();
        final ActionPlan sendActionPlan = actionPlanGenerator.generate(actionPlanSize, 1, planContextId);
        populateActions(actionOrchestrator, sendActionPlan, "PLAN");

        fetchActions(actionsService, FilteredActionRequest.newBuilder()
            .setTopologyContextId(planContextId)
            .build(), "PLAN");

        final long start = System.currentTimeMillis();
        final DeleteActionsResponse deleteActionsResponse = actionsService.deleteActions(
            DeleteActionsRequest.newBuilder()
                .setTopologyContextId(planContextId)
                .build());

        logger.info("Took {} seconds to delete {} PLAN actions.",
            (System.currentTimeMillis() - start) / 1000.0f,
            deleteActionsResponse.getActionCount());
    }

    /**
     * Send the specified action plan to the action orchestrator for processing.
     *
     * @param actionOrchestrator The action orchestrator.
     * @param actionPlan The action plan.
     * @param type The type.
     */
    public void populateActions(@Nonnull final ActionOrchestratorNotificationReceiver actionOrchestrator,
                                @Nonnull final ActionPlan actionPlan,
                                @Nonnull final String type) throws Exception {
        final CompletableFuture<ActionsUpdated> actionsUpdatedFuture = new CompletableFuture<>();
        actionOrchestrator.addListener(new TestActionsListener(actionsUpdatedFuture));

        final long start = System.currentTimeMillis();
        marketNotificationSender.notifyActionsRecommended(actionPlan);
        final ActionsUpdated receivedActionsUpdated = actionsUpdatedFuture.get(10, TimeUnit.MINUTES);

        logger.info("Took {} seconds to receive and process {} action plan of size {}.",
            (System.currentTimeMillis() - start) / 1000.0f,
            type, actionPlan.getActionList().size());
    }

    public void fetchActions(@Nonnull final ActionsServiceBlockingStub actionsService,
                             @Nonnull final FilteredActionRequest request,
                             @Nonnull final String type) throws Exception {
        final long startFetchVisible = System.currentTimeMillis();

        final PaginationParameters paginationParameters = PaginationParameters.newBuilder()
                // Set a high limit. The server may trim it down.
                .setLimit(100000)
                .build();
        final Iterator<FilteredActionResponse> responseIterator = actionsService.getAllActions(
                request.toBuilder().setPaginationParams(paginationParameters).build());
        int counter = 0;
        while (responseIterator.hasNext()) {
            final FilteredActionResponse response = responseIterator.next();
            if (response.getTypeCase() == TypeCase.ACTION_CHUNK) {
                counter += response.getActionChunk().getActionsCount();
            }
        }
        logger.info("Took {} retrieve the first page of {} {} actions.",
            (System.currentTimeMillis() - startFetchVisible) / 1000.0f, type, counter);
    }

    public void fetchSeverities(long actionPlanSize) throws Exception {
        final List<Long> entityIdsList = LongStream.range(0, actionPlanSize / 2)
            .mapToObj(id -> id)
            .collect(Collectors.toList());
        final long startFetch = System.currentTimeMillis();

        // Approximate the call for fetching severities for various entity types in the supply chain.
        // The number of partition slices is equivalent to the number of nodes in the supply chain.
        final AtomicLong total = new AtomicLong();
        final int partitionSize = entityIdsList.size() / SEVERITY_PARTITION_COUNT;
        Lists.partition(entityIdsList, partitionSize).forEach(subList -> {
            final SeverityCountsResponse response = severitiesService.getSeverityCounts(
                MultiEntityRequest.newBuilder()
                    .addAllEntityIds(subList)
                    .setTopologyContextId(ComponentUtils.REALTIME_TOPOLOGY_CONTEXT)
                    .build());
            total.getAndAdd(response.getUnknownEntityCount());
            response.getCountsList().forEach(count -> total.getAndAdd(count.getEntityCount()));
        });

        logger.info("Took {} seconds to retrieve {} severity counts in {} calls.",
            (System.currentTimeMillis() - startFetch) / 1000.0f,
            total.get(),
            SEVERITY_PARTITION_COUNT);
    }

    private static class TestActionsListener implements ActionsListener {
        private final CompletableFuture<ActionsUpdated> actionsUpdatedFuture;

        public TestActionsListener(@Nonnull final CompletableFuture<ActionsUpdated> actionsUpdatedFuture) {
            this.actionsUpdatedFuture = actionsUpdatedFuture;
        }

        @Override
        public void onActionsUpdated(@Nonnull final ActionsUpdated actionsUpdated) {
            actionsUpdatedFuture.complete(actionsUpdated);
        }
    }

    /**
     * Entity Service Stub that provides bare-bones entity information for entities referenced in
     * an action plan.
     */
    public class EntityServiceStub extends RepositoryServiceImplBase {

        private Map<Long, ActionPartialEntity> entitiesForActionPlan = new HashMap<>();

        public void loadEntitiesForActionPlan(ActionPlan actionPlan) throws UnsupportedActionException {
            // create simple entities for each action in the plan
            entitiesForActionPlan = ActionDTOUtil.getInvolvedEntityIds(actionPlan.getActionList()).stream()
                    .map(entityId -> ActionPartialEntity.newBuilder()
                    .addAllDiscoveringTargetIds(Arrays.asList(1L))
                    .setOid(entityId).build())
                    .collect(Collectors.toMap(ActionPartialEntity::getOid, Function.identity()));
        }
        @Override
        public void retrieveTopologyEntities(final RetrieveTopologyEntitiesRequest request,
                                              final StreamObserver<PartialEntityBatch> responseObserver) {
            List<PartialEntity> actionPartialEntities = new ArrayList<>();
            for (final Long entityId : request.getEntityOidsList()) {
                ActionPartialEntity actionPartialEntity = entitiesForActionPlan.get(entityId);
                if (actionPartialEntity != null) {
                    actionPartialEntities.add(PartialEntity.newBuilder().setAction(actionPartialEntity).build());
                }
            }
            responseObserver.onNext(PartialEntityBatch.newBuilder().addAllEntities(actionPartialEntities).build());
            responseObserver.onCompleted();
        }
    }

    public class ProbeActionCapabilitiesServiceStub extends ProbeActionCapabilitiesServiceImplBase {
        @Nonnull
        private List<ProbeActionCapability> createActionCapabilitiesList() {
            // creating a single action capability for the purposes of the test
            ProbeActionCapability supportedCapability = ProbeActionCapability.newBuilder()
                    .addCapabilityElement(ActionCapabilityElement.newBuilder()
                            .setActionType(ActionDTO.ActionType.MOVE)
                            .setActionCapability(ActionCapability.NOT_EXECUTABLE)
                            .build())
                    .build();
            return ImmutableList.of(supportedCapability);
        }

        /**
         * Stub method that returns a fixed set of probe action capabilities as test data.
         * @param request
         * @param responseObserver
         */
        @Override
        public void getProbeActionCapabilities(final GetProbeActionCapabilitiesRequest request,
                                               final StreamObserver<GetProbeActionCapabilitiesResponse> responseObserver) {
            List<ProbeActionCapability> allCapabilities = createActionCapabilitiesList();
            GetProbeActionCapabilitiesResponse response =
                    GetProbeActionCapabilitiesResponse.newBuilder()
                            .addAllActionCapabilities(allCapabilities)
                            .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

}
