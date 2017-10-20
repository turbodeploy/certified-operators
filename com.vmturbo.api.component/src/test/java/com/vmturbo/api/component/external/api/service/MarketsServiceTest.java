package com.vmturbo.api.component.external.api.service;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.MediaType;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.GsonHttpMessageConverter;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.AnnotationConfigWebContextLoader;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

import com.google.gson.Gson;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import com.vmturbo.api.MarketNotificationDTO.MarketNotification;
import com.vmturbo.api.MarketNotificationDTO.StatusNotification;
import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.mapper.ActionSpecMapper;
import com.vmturbo.api.component.external.api.mapper.MarketMapper;
import com.vmturbo.api.component.external.api.mapper.PolicyMapper;
import com.vmturbo.api.component.external.api.mapper.ScenarioMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.websocket.UINotificationChannel;
import com.vmturbo.api.controller.MarketsController;
import com.vmturbo.api.dto.MarketApiDTO;
import com.vmturbo.api.handler.GlobalExceptionHandler;
import com.vmturbo.api.serviceinterfaces.IBusinessUnitsService;
import com.vmturbo.api.serviceinterfaces.IGroupsService;
import com.vmturbo.api.serviceinterfaces.IPoliciesService;
import com.vmturbo.api.serviceinterfaces.ISettingsPoliciesService;
import com.vmturbo.api.serviceinterfaces.IStatsService;
import com.vmturbo.api.serviceinterfaces.ISupplyChainsService;
import com.vmturbo.api.serviceinterfaces.ITemplatesService;
import com.vmturbo.api.serviceinterfaces.IUsersService;
import com.vmturbo.api.validators.InputDTOValidator;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.group.GroupFetcher;
import com.vmturbo.common.protobuf.group.PolicyServiceGrpc;
import com.vmturbo.common.protobuf.group.PolicyServiceGrpc.PolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.PlanDTO.CreatePlanRequest;
import com.vmturbo.common.protobuf.plan.PlanDTO.GetPlansOptions;
import com.vmturbo.common.protobuf.plan.PlanDTO.OptionalPlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanId;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanScenario;
import com.vmturbo.common.protobuf.plan.PlanDTO.Scenario;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioInfo;
import com.vmturbo.common.protobuf.plan.PlanServiceGrpc;
import com.vmturbo.common.protobuf.plan.PlanServiceGrpc.PlanServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.PlanServiceGrpc.PlanServiceImplBase;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.topology.processor.api.TopologyProcessor;

/**
 * Unit test for {@link MarketsService}.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@WebAppConfiguration
@ContextConfiguration(loader = AnnotationConfigWebContextLoader.class)
@DirtiesContext(classMode = ClassMode.BEFORE_EACH_TEST_METHOD)
public class MarketsServiceTest {

    private static final long REALTIME_CONTEXT_ID = 777777;

    private static final long REALTIME_PLAN_ID = 0;
    private static final long TEST_PLAN_OVER_PLAN_ID = REALTIME_PLAN_ID + 1;
    private static final long TEST_SCENARIO_ID = 12;

    @Autowired
    private TestConfig testConfig;
    @Autowired
    private WebApplicationContext wac;

    private MockMvc mockMvc;
    private static final Gson GSON = new Gson();
    private final PlanInstance planDefault;

    public MarketsServiceTest() {
        planDefault = PlanInstance.newBuilder()
                .setPlanId(111)
                .setActionPlanId(222)
                .setStartTime(System.currentTimeMillis() - 10000)
                .setEndTime(System.currentTimeMillis() - 10)
                .setStatus(PlanStatus.QUEUED)
                .setScenario(Scenario.newBuilder()
                        .setId(555)
                        .setScenarioInfo(ScenarioInfo.newBuilder().setName("Some scenario")))
                .build();
    }

    @Before
    public void startup() {
        mockMvc = MockMvcBuilders.webAppContextSetup(wac).build();
    }

    /**
     * Tests getting all the plan instances (markets in API terminology).
     *
     * @throws Exception if exception occurs
     */
    @Test
    public void testGetAllMarkets() throws Exception {
        final PlanInstance plan1 = PlanInstance.newBuilder(planDefault).setPlanId(1).build();
        final PlanInstance plan2 = PlanInstance.newBuilder(planDefault).setPlanId(2).build();
        testConfig.planService().addInstance(plan1);
        testConfig.planService().addInstance(plan2);

        final MvcResult result = mockMvc.perform(MockMvcRequestBuilders.get("/markets")
                .accept(MediaType.APPLICATION_JSON_UTF8_VALUE))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andReturn();
        final Collection<MarketApiDTO> resp = Arrays.asList(
                GSON.fromJson(result.getResponse().getContentAsString(), MarketApiDTO[].class));
        assertEquals(2, resp.size());
        final MarketApiDTO market1 =
                resp.stream().filter(market -> market.getUuid().equals("1")).findFirst().get();
        final MarketApiDTO market2 =
                resp.stream().filter(market -> market.getUuid().equals("2")).findFirst().get();
        comparePlanAndMarket(plan1, market1);
        comparePlanAndMarket(plan2, market2);
    }

    /**
     * Test that successfully deleting a "market" (i.e. plan) causes a DELETE notification
     * to be sent to the {@link UINotificationChannel}.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testMarketDeleteNotification() throws Exception {
        final PlanInstance plan1 = PlanInstance.newBuilder(planDefault).setPlanId(1).build();
        testConfig.planService().addInstance(plan1);

        final MarketsService service = testConfig.marketsService();
        final UINotificationChannel channel = testConfig.uiNotificationChannel();

        service.deleteMarketByUuid("1");

        final ArgumentCaptor<MarketNotification> notificationCaptor =
                ArgumentCaptor.forClass(MarketNotification.class);
        Mockito.verify(channel).broadcastMarketNotification(notificationCaptor.capture());

        final MarketNotification notification = notificationCaptor.getValue();
        assertEquals("1", notification.getMarketId());
        assertEquals(StatusNotification.Status.DELETED,
                notification.getStatusNotification().getStatus());
    }

    /**
     * Tests getting a plan instance (markets in API terminology) by its id.
     *
     * @throws Exception if exception occurs
     */
    @Test
    public void testGetMarketById() throws Exception {
        final PlanInstance plan1 = PlanInstance.newBuilder(planDefault).setPlanId(1).build();
        final PlanInstance plan2 = PlanInstance.newBuilder(planDefault).setPlanId(2).build();
        testConfig.planService().addInstance(plan1);
        testConfig.planService().addInstance(plan2);

        final MvcResult result = mockMvc.perform(MockMvcRequestBuilders.get("/markets/" + 2)
                .accept(MediaType.APPLICATION_JSON_UTF8_VALUE))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andReturn();
        final MarketApiDTO resp =
                GSON.fromJson(result.getResponse().getContentAsString(), MarketApiDTO.class);
        comparePlanAndMarket(plan2, resp);
    }

    private void comparePlanAndMarket(@Nonnull PlanInstance expected, @Nonnull MarketApiDTO market) {
        assertEquals(Long.toString(expected.getPlanId()), market.getUuid());
        assertEquals(expected.getScenario().getId(),
                Long.parseLong(market.getScenario().getUuid()));
    }

    @Test
    public void testRunPlanOnMainMarket() throws Exception {

        // the uuid "Market" is what the UI uses to distinguish the live market
        String runPlanUri = "/markets/Market/scenarios/1";

        final MvcResult result = mockMvc.perform(MockMvcRequestBuilders.post(runPlanUri)
                .accept(MediaType.APPLICATION_JSON_UTF8_VALUE))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andReturn();

        final MarketApiDTO resp =
                GSON.fromJson(result.getResponse().getContentAsString(), MarketApiDTO.class);
        assertThat(resp.getUuid(), is(Long.toString(REALTIME_PLAN_ID)));
    }

    @Test
    public void testRunPlanOnPlan() throws Exception {

        final PlanInstance plan1 = PlanInstance.newBuilder(planDefault)
                .setPlanId(TEST_PLAN_OVER_PLAN_ID)
                .setStatus(PlanStatus.SUCCEEDED)
                .build();
        testConfig.planService().addInstance(plan1);

        // the market UUID is the ID of the Plan Spec to start from
        String runPlanUri = "/markets/" + TEST_PLAN_OVER_PLAN_ID + "/scenarios/"
                + TEST_SCENARIO_ID;

        final MvcResult result = mockMvc.perform(MockMvcRequestBuilders.post(runPlanUri)
                .accept(MediaType.APPLICATION_JSON_UTF8_VALUE))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andReturn();

        final MarketApiDTO resp =
                GSON.fromJson(result.getResponse().getContentAsString(), MarketApiDTO.class);
        assertThat(resp.getUuid(), is(Long.toString(TEST_PLAN_OVER_PLAN_ID)));
        assertEquals(TEST_SCENARIO_ID, Long.parseLong(resp.getScenario().getUuid()));
        // note that the market status comes from the StatusNotification enum
        assertThat(resp.getState(), is(StatusNotification.Status.CREATED.name()));
    }

    /**
     * Spring configuration to startup all the beans, necessary for test execution.
     */
    @Configuration
    @EnableWebMvc
    public static class TestConfig extends WebMvcConfigurerAdapter {

        @Bean
        public TopologyProcessor topologyProcessor() {
            return Mockito.mock(TopologyProcessor.class);
        }

        @Bean
        public MarketsService marketsService() {
            return new MarketsService(actionSpecMapper(), uuidMapper(), actionRpcService(),
                    policyCpcService(), planRpcService(), policyMapper(), marketMapper(),
                    groupFetcher(), uiNotificationChannel());
        }

        @Bean
        public UINotificationChannel uiNotificationChannel() {
            return Mockito.mock(UINotificationChannel.class);
        }

        @Bean
        public UuidMapper uuidMapper() {
            return new UuidMapper(REALTIME_CONTEXT_ID);
        }

        @Bean
        public ActionSpecMapper actionSpecMapper() {
            return Mockito.mock(ActionSpecMapper.class);
        }

        @Bean
        public MarketMapper marketMapper() {
            return new MarketMapper(scenarioMapper());
        }

        @Bean
        public ScenarioMapper scenarioMapper() {
            return new ScenarioMapper(repositoryApi());
        }

        @Bean
        public RepositoryApi repositoryApi() {
            return Mockito.mock(RepositoryApi.class);
        }

        @Bean
        public PlanServiceMock planService() {
            return new PlanServiceMock();
        }

        @Bean
        public GrpcTestServer grpcTestServer() {
            try {
                final GrpcTestServer testServer = GrpcTestServer.newServer(planService());
                testServer.start();
                return testServer;
            } catch (IOException e) {
                throw new BeanCreationException("Failed to create test channel", e);
            }
        }

        @Bean
        public ActionsServiceBlockingStub actionRpcService() {
            return ActionsServiceGrpc.newBlockingStub(grpcTestServer().getChannel());
        }

        @Bean
        public PolicyServiceBlockingStub policyCpcService() {
            return PolicyServiceGrpc.newBlockingStub(grpcTestServer().getChannel());
        }

        @Bean
        public PlanServiceBlockingStub planRpcService() {
            return PlanServiceGrpc.newBlockingStub(grpcTestServer().getChannel());
        }

        @Bean
        public PolicyMapper policyMapper() {
            return Mockito.mock(PolicyMapper.class);
        }

        @Bean
        public GroupFetcher groupFetcher() {
            return Mockito.mock(GroupFetcher.class);
        }

        @Bean
        public MarketsController targetController() {
            return new MarketsController();
        }

        @Bean
        public GlobalExceptionHandler exceptionHandler() {
            return new GlobalExceptionHandler();
        }

        @Bean
        public InputDTOValidator inputDtoValidator() {
            return Mockito.mock(InputDTOValidator.class);
        }

        @Bean
        public IPoliciesService policiesService() {
            return Mockito.mock(IPoliciesService.class);
        }

        @Bean
        public ITemplatesService templatesService() {
            return Mockito.mock(ITemplatesService.class);
        }

        @Bean
        public IStatsService statsService() {
            return Mockito.mock(IStatsService.class);
        }

        @Bean
        public IBusinessUnitsService businessUnitsService() {
            return Mockito.mock(IBusinessUnitsService.class);
        }

        @Bean
        public ISupplyChainsService supplyChainsService() {
            return Mockito.mock(ISupplyChainsService.class);
        }

        @Bean
        public IGroupsService groupsService() {
            return Mockito.mock(IGroupsService.class);
        }

        @Bean
        public IUsersService usersService() {
            return Mockito.mock(IUsersService.class);
        }

        @Bean
        ISettingsPoliciesService settingsPoliciesService() {
            return Mockito.mock(ISettingsPoliciesService.class);
        }

        @Bean
        public GlobalExceptionHandler globalExceptionHandler() {
            return new GlobalExceptionHandler();
        }

        @Override
        public void configureMessageConverters(List<HttpMessageConverter<?>> converters) {
            GsonHttpMessageConverter msgConverter = new GsonHttpMessageConverter();
            msgConverter.setGson(ComponentGsonFactory.createGson());
            converters.add(msgConverter);
        }
    }

    /**
     * Fake Plan service, just holding some pre-defined plan instances.
     */
    public static class PlanServiceMock extends PlanServiceImplBase {

        @GuardedBy("lock")
        private Map<Long, PlanInstance> planInstances = new HashMap<>();

        /**
         * Using an explicit lock to synchronize access to planInstances because we
         * have methods that iterate over the values.
         */
        private final Object lock = new Object();

        public void addInstance(@Nonnull final PlanInstance planInstance) {
            synchronized (lock) {
                planInstances.put(planInstance.getPlanId(), planInstance);
            }
        }

        @Override
        public void getAllPlans(GetPlansOptions request,
                StreamObserver<PlanInstance> responseObserver) {
            synchronized (lock) {
                planInstances.values().forEach(responseObserver::onNext);
            }
            responseObserver.onCompleted();
        }

        @Override
        public void getPlan(PlanId request, StreamObserver<OptionalPlanInstance> responseObserver) {
            final OptionalPlanInstance.Builder result = OptionalPlanInstance.newBuilder();
            synchronized (lock) {
                final Optional<PlanInstance> plan =
                        Optional.ofNullable(planInstances.get(request.getPlanId()));
                plan.ifPresent(result::setPlanInstance);
            }

            responseObserver.onNext(result.build());
            responseObserver.onCompleted();
        }

        @Override
        public void createPlan(CreatePlanRequest request, StreamObserver<PlanInstance> responseObserver) {
            // set one or the other plan ID based on the topology ID - REALTIME vs plan
            long planId = request.getTopologyId() == REALTIME_PLAN_ID ? REALTIME_PLAN_ID :
                    TEST_PLAN_OVER_PLAN_ID;
            PlanInstance planResponse = PlanInstance.newBuilder()
                    .setPlanId(planId)
                    .setTopologyId(request.getTopologyId())
                    .setStatus(PlanStatus.QUEUED)
                    .build();
            synchronized (lock) {
                planInstances.put(planId, planResponse);
            }
            responseObserver.onNext(planResponse);
            responseObserver.onCompleted();
        }

        @Override
        public void updatePlanScenario(PlanScenario request, StreamObserver<PlanInstance> responseObserver) {

            final PlanInstance  planResponse;
            synchronized (lock) {
                final PlanInstance plan = planInstances.get(request.getPlanId());

                Assert.assertNotNull("Plan " + request.getPlanId() + " not found!", plan);

                Scenario newScenario = Scenario.newBuilder()
                        .setId(request.getScenarioId())
                        .build();
                // implement the "replace scenario" scheme - currently under discussion
                planResponse = PlanInstance.newBuilder()
                        .setPlanId(request.getPlanId())
                        .setTopologyId(plan.getTopologyId())
                        .setScenario(newScenario)
                        .setStatus(PlanStatus.READY)
                        .build();

                planInstances.put(request.getPlanId(), planResponse);
            }
            responseObserver.onNext(planResponse);
            responseObserver.onCompleted();
        }

        @Override
        public void runPlan(PlanId request, StreamObserver<PlanInstance> responseObserver) {
            synchronized (lock) {
                planInstances.values().forEach(responseObserver::onNext);
            }
            responseObserver.onCompleted();
        }

        @Override
        public void deletePlan(PlanId request, StreamObserver<PlanInstance> responseObserver) {
            final PlanInstance instance;
            synchronized (lock) {
                instance = planInstances.remove(request.getPlanId());
            }
            if (instance == null) {
                responseObserver.onError(Status.NOT_FOUND.asException());
            } else {
                responseObserver.onNext(instance);
            }
            responseObserver.onCompleted();
        }

        @Override
        public void createPlanOverPlan(PlanScenario request,
                                       StreamObserver<PlanInstance> responseObserver) {
            final PlanInstance newPlan;
            synchronized (lock) {
                PlanInstance previousPlan = planInstances.get(request.getPlanId());
                Assert.assertNotNull("Plan " + request.getPlanId() + " not found!",
                        previousPlan);
                planInstances.clear();
                newPlan = PlanInstance.newBuilder()
                        .setStatus(PlanStatus.READY)
                        .setPlanId(request.getPlanId())
                        .setTopologyId(previousPlan.getProjectedTopologyId())
                        .setProjectedTopologyId(0)
                        .setScenario(Scenario.newBuilder()
                                .setId(request.getScenarioId())
                                .build())
                        .build();
                planInstances.put(request.getPlanId(), newPlan);
            }
            responseObserver.onNext(newPlan);
            responseObserver.onCompleted();
        }
    }
}
