package com.vmturbo.plan.orchestrator.plan;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.grpc.Channel;

import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.cost.BuyRIAnalysisServiceGrpc;
import com.vmturbo.common.protobuf.cost.BuyRIAnalysisServiceGrpc.BuyRIAnalysisServiceBlockingStub;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc.CostServiceBlockingStub;
import com.vmturbo.common.protobuf.cost.PlanReservedInstanceServiceGrpc;
import com.vmturbo.common.protobuf.cost.PlanReservedInstanceServiceGrpc.PlanReservedInstanceServiceBlockingStub;
import com.vmturbo.common.protobuf.cost.ReservedInstanceBoughtServiceGrpc;
import com.vmturbo.common.protobuf.cost.ReservedInstanceBoughtServiceGrpc.ReservedInstanceBoughtServiceBlockingStub;
import com.vmturbo.common.protobuf.cost.RIBuyContextFetchServiceGrpc;
import com.vmturbo.common.protobuf.cost.RIBuyContextFetchServiceGrpc.RIBuyContextFetchServiceBlockingStub;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanServiceGrpc;
import com.vmturbo.common.protobuf.plan.PlanServiceGrpc.PlanServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RepositoryOperationResponse;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RepositoryOperationResponseCode;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingServiceMole;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.AnalysisDTOMoles.AnalysisServiceMole;
import com.vmturbo.common.protobuf.topology.AnalysisServiceGrpc;
import com.vmturbo.common.protobuf.topology.AnalysisServiceGrpc.AnalysisServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.AnalysisServiceGrpc.AnalysisServiceImplBase;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.api.test.SenderReceiverPair;
import com.vmturbo.plan.orchestrator.reservation.ReservationDao;
import com.vmturbo.plan.orchestrator.reservation.ReservationDaoImpl;
import com.vmturbo.plan.orchestrator.reservation.ReservationManager;
import com.vmturbo.plan.orchestrator.reservation.ReservationPlacementHandler;
import com.vmturbo.repository.api.RepositoryClient;
import com.vmturbo.sql.utils.TestSQLDatabaseConfig;

/**
 * Test configuration for interacting with the DB.
 */
@Configuration
@EnableTransactionManagement
@Import({TestSQLDatabaseConfig.class})
public class PlanTestConfig {

    public static final long REALTIME_TOPOLOGY_ID = 77777;

    @Autowired
    protected TestSQLDatabaseConfig dbConfig;

    @Bean
    public static PropertySourcesPlaceholderConfigurer properties() {
        final PropertySourcesPlaceholderConfigurer pspc = new PropertySourcesPlaceholderConfigurer();
        final Properties properties = new Properties();
        properties.setProperty("originalSchemaName", "plan");
        pspc.setProperties(properties);
        return pspc;
    }

    @Bean
    protected GrpcTestServer analysisGrpcServer() throws IOException {
        final GrpcTestServer server = GrpcTestServer.newServer(analysisServer());
        server.start();
        return server;
    }

    @Bean
    public RIBuyContextFetchServiceBlockingStub riBuyContextService() throws IOException {
        return RIBuyContextFetchServiceGrpc.newBlockingStub(analysisGrpcServer().getChannel());
    }

    @Bean
    protected BuyRIAnalysisServiceBlockingStub buyRIService() throws IOException {
        return BuyRIAnalysisServiceGrpc.newBlockingStub(analysisGrpcServer().getChannel());
    }

    /**
     * Bean for the bought (existng) RI service.
     *
     * @return ReservedInstanceBoughtServiceBlockingStub.
     * @throws IOException if there's an error.
     */
    @Bean
    protected ReservedInstanceBoughtServiceBlockingStub boughtRIService() throws IOException {
        return ReservedInstanceBoughtServiceGrpc.newBlockingStub(analysisGrpcServer().getChannel());
    }

    @Bean
    protected PlanReservedInstanceServiceBlockingStub planRIService() throws IOException {
        return PlanReservedInstanceServiceGrpc.newBlockingStub(analysisGrpcServer().getChannel());
    }

    @Bean
    protected CostServiceBlockingStub costService() throws IOException {
        return CostServiceGrpc.newBlockingStub(analysisGrpcServer().getChannel());
    }

    @Bean
    protected AnalysisServiceBlockingStub analysisClient() throws IOException {
        return AnalysisServiceGrpc.newBlockingStub(analysisGrpcServer().getChannel());
    }

    @Bean
    protected PlanServiceBlockingStub planClient() throws IOException {
        return PlanServiceGrpc.newBlockingStub(planGrpcServer().getChannel());
    }

    @Bean
    public GroupServiceBlockingStub groupServiceBlockingStub() throws IOException {
        return GroupServiceGrpc.newBlockingStub(groupGrpcServer().getChannel());
    }

    @Bean
    protected GrpcTestServer groupGrpcServer() throws IOException {
        final GrpcTestServer server = GrpcTestServer.newServer( Mockito.spy(new GroupServiceMole()));
        server.start();
        return server;
    }

    @Bean
    protected GrpcTestServer planGrpcServer() throws IOException {
        final GrpcTestServer server = GrpcTestServer.newServer(planServer());
        server.start();
        return server;
    }

    @Bean
    public AnalysisServiceImplBase analysisServer() {
        return Mockito.spy(new AnalysisServiceMole());
    }

    @Bean
    public PlanNotificationSender planNotificationSender() {
        final PlanNotificationSender sender = new PlanNotificationSender(messageChannel());
        try {
            planDao().addStatusListener(sender);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return sender;
    }

    @Bean
    public PlanRpcService planServer() throws IOException {
        return new PlanRpcService(planDao(),
            analysisClient(), planNotificationSender(), startAnalysisThreadPool(),
            userSessionContext(), buyRIService(), groupServiceBlockingStub(),
            repositoryServiceBlockingStub(), planRIService(), boughtRIService(), 1,
            TimeUnit.SECONDS, REALTIME_TOPOLOGY_ID);
    }

    @Bean
    public UserSessionContext userSessionContext() {
        UserSessionContext userSessionContext = Mockito.mock(UserSessionContext.class);
        return userSessionContext;
    }

    public static final RepositoryOperationResponse OK = RepositoryOperationResponse.newBuilder()
            .setResponseCode(RepositoryOperationResponseCode.OK)
            .build();

    @Bean(destroyMethod = "shutdownNow")
    public ExecutorService startAnalysisThreadPool() {
        final ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("plan-analysis-starter-%d")
                .build();
        return Executors.newSingleThreadExecutor(threadFactory);
    }

    @Bean
    public RepositoryClient repositoryClient() {
        RepositoryClient reposOkClient = Mockito.mock(RepositoryClient.class);
        Mockito
                .when(reposOkClient.deleteTopology(Mockito.anyLong(), Mockito.anyLong(), Mockito.any()))
                .thenReturn(OK);

        return reposOkClient;
    }

    /**
     * An instance of the search rpc service used for testing.
     *
     * @return search rpc stub
     */
    @Bean
    public SearchServiceBlockingStub searchClient() {
        return SearchServiceGrpc.newBlockingStub(Mockito.mock(Channel.class));
    }

    @Bean
    public ActionsServiceBlockingStub actionServiceClient() {
        return ActionsServiceGrpc.newBlockingStub(
                Mockito.mock(Channel.class));
    }

    @Bean
    public StatsHistoryServiceBlockingStub statsServiceClient() {
        return StatsHistoryServiceGrpc.newBlockingStub(
                Mockito.mock(Channel.class));
    }

    @Bean
    public SettingServiceMole settingServer() {
        return Mockito.spy(new SettingServiceMole());
    }

    @Bean
    protected GrpcTestServer settingGrpcServer() {
        final GrpcTestServer server = GrpcTestServer.newServer(settingServer());
        try {
            server.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return server;
    }

    /**
     * The default plan time out value (6) is set in factoryInstalledComponents.yml, if it's updated
     * we need to change the value here too.
     *
     * @return plan DAO
     * @throws IOException if there was a problem during plan DAO creation.
     */
    @Bean
    public PlanDao planDao() throws IOException {
        return Mockito.spy(
                new PlanDaoImpl(dbConfig.dsl(), repositoryClient(),
                        actionServiceClient(), statsServiceClient(), settingGrpcServer().getChannel(),
                        userSessionContext(), searchClient(), riBuyContextService(), planRIService(),
                        costService(), 6));
    }

    @Bean
    public RepositoryServiceBlockingStub repositoryServiceBlockingStub() {
        return RepositoryServiceGrpc.newBlockingStub(Mockito.mock(Channel.class));
    }

    @Bean
    public ReservationPlacementHandler reservationPlacementHandler() {
        ReservationPlacementHandler reservationPlacementHandler =
                Mockito.spy(new ReservationPlacementHandler(reservationManager(),
                        repositoryServiceBlockingStub()));
        Mockito.doNothing().when(reservationPlacementHandler).updateReservations(Mockito.anyLong(),
                Mockito.anyLong(), Mockito.anyBoolean());
        return reservationPlacementHandler;
    }

    @Bean
    public ReservationManager reservationManager() {
        return Mockito.mock(ReservationManager.class);
    }

    @Bean
    public PlanProgressListener actionsListener() throws IOException {
        return new PlanProgressListener(planDao(), planServer(), reservationPlacementHandler(), REALTIME_TOPOLOGY_ID);
    }

    @PostConstruct
    public void init() {
        IdentityGenerator.initPrefix(0);
        dbConfig.flyway().clean();
        dbConfig.flyway().migrate();
    }

    @PreDestroy
    public void destroy() {
        dbConfig.flyway().clean();
    }

     @Bean
     public SenderReceiverPair<PlanInstance> messageChannel() {
         return new SenderReceiverPair<>();
     }

 }
