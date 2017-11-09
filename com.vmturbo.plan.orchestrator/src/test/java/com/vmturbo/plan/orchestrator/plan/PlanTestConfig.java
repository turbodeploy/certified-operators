package com.vmturbo.plan.orchestrator.plan;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import javax.annotation.PostConstruct;

import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.grpc.Channel;

import com.vmturbo.common.protobuf.action.ActionsServiceGrpc;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanServiceGrpc;
import com.vmturbo.common.protobuf.plan.PlanServiceGrpc.PlanServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.PlanServiceGrpc.PlanServiceImplBase;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RepositoryOperationResponse;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RepositoryOperationResponseCode;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.AnalysisDTOMoles.AnalysisServiceMole;
import com.vmturbo.common.protobuf.topology.AnalysisServiceGrpc;
import com.vmturbo.common.protobuf.topology.AnalysisServiceGrpc.AnalysisServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.AnalysisServiceGrpc.AnalysisServiceImplBase;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.api.test.SenderReceiverPair;
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
    protected AnalysisServiceBlockingStub analysisClient() throws IOException {
        return AnalysisServiceGrpc.newBlockingStub(analysisGrpcServer().getChannel());
    }

    @Bean
    protected PlanServiceBlockingStub planClient() throws IOException {
        return PlanServiceGrpc.newBlockingStub(planGrpcServer().getChannel());
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
        return new PlanNotificationSender(messageChannel());
    }

    @Bean
    public PlanServiceImplBase planServer() throws IOException {
        return new PlanRpcService(planDao(),
                analysisClient(), planNotificationSender(), startAnalysisThreadPool());
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
            .when(reposOkClient.deleteTopology(Mockito.anyLong(), Mockito.anyLong()))
            .thenReturn(OK);

        return reposOkClient;
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
    public PlanDao planDao() {
        return Mockito.spy(
                new PlanDaoImpl(dbConfig.dsl(), planNotificationSender(), repositoryClient(),
                        actionServiceClient(), statsServiceClient()));
    }

    @Bean
    public PlanProgressListener actionsListener() {
        return new PlanProgressListener(planDao(), REALTIME_TOPOLOGY_ID);
    }

    @PostConstruct
    public void init() {
        IdentityGenerator.initPrefix(0);
        dbConfig.flyway().clean();
        dbConfig.flyway().migrate();
    }

     @Bean
     public SenderReceiverPair<PlanInstance> messageChannel() {
         return new SenderReceiverPair<>();
     }

 }
