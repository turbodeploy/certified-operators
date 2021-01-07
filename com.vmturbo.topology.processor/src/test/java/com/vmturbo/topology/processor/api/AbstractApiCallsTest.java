package com.vmturbo.topology.processor.api;

import static org.mockito.Matchers.any;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.mockito.AdditionalAnswers;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.topology.ActionExecutionServiceGrpc;
import com.vmturbo.common.protobuf.topology.ActionExecutionServiceGrpc.ActionExecutionServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntitiesWithNewState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.Topology;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologySummary;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.api.test.IntegrationTestServer;
import com.vmturbo.platform.common.dto.Discovery.AccountValue;
import com.vmturbo.platform.common.dto.Discovery.AccountValue.PropertyValueList;
import com.vmturbo.topology.processor.actions.ActionExecutionRpcService;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TopologyProcessorNotification;
import com.vmturbo.topology.processor.api.dto.InputField;
import com.vmturbo.topology.processor.api.impl.TargetRESTApi;
import com.vmturbo.topology.processor.api.impl.TargetRESTApi.TargetSpec;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorClient;
import com.vmturbo.topology.processor.api.server.TopoBroadcastManager;
import com.vmturbo.topology.processor.targets.GroupScopeResolver;
import com.vmturbo.topology.processor.targets.Target;

/**
 * Test various API calls using Jetty server started up and some clients connecting to.
 */
public abstract class AbstractApiCallsTest {

    protected final Logger logger = LogManager.getLogger();

    protected static final int TIMEOUT_MS = 10000;

    private ExecutorService threadPool;

    protected IntegrationTestServer integrationTestServer;

    @Rule
    public TestName testName = new TestName();

    private TopologyProcessorClient topologyProcessor;

    protected ActionExecutionServiceBlockingStub actionExecutionService;

    /**
     * Not using it as a Rule because we get the services from the {@link IntegrationTestServer}.
     */
    private GrpcTestServer grpcServer;

    @Before
    public final void init() throws Exception {
        Thread.currentThread().setName(testName.getMethodName() + "-main");
        logger.debug("Starting @Before");
        final ThreadFactory threadFactory = new ThreadFactoryBuilder()
                        .setNameFormat("clt-" + testName.getMethodName() + "-%d").build();
        threadPool = Executors.newCachedThreadPool(threadFactory);

        integrationTestServer = new IntegrationTestServer(testName, TestApiServerConfig.class);


        grpcServer = GrpcTestServer.newServer(
                integrationTestServer.getBean(ActionExecutionRpcService.class));
        grpcServer.start();

        final IMessageReceiver<TopologyProcessorNotification> notificationReceiver =
                integrationTestServer.getBean("notificationsConnection");
        final IMessageReceiver<Topology> liveTopologyReceiver =
                integrationTestServer.getBean("liveTopologyConnection");
        final IMessageReceiver<Topology> planTopologyReceiver =
                integrationTestServer.getBean("planTopologyConnection");
        final IMessageReceiver<TopologySummary> topologySummaryReceiver =
                integrationTestServer.getBean("topologySummaryConnection");
        final IMessageReceiver<EntitiesWithNewState> entitiesWithNewStateReceiver =
            integrationTestServer.getBean("entitiesWithNewStateConnection");
        topologyProcessor =
                TopologyProcessorClient.rpcAndNotification(integrationTestServer.connectionConfig(),
                        threadPool, notificationReceiver, liveTopologyReceiver,
                        planTopologyReceiver, topologySummaryReceiver, entitiesWithNewStateReceiver);

        actionExecutionService =
                ActionExecutionServiceGrpc.newBlockingStub(grpcServer.getChannel());

        logger.debug("Finished @Before");
    }

    @After
    public final void shutdown() throws Exception {
        logger.debug("Starting @After");
        grpcServer.close();
        integrationTestServer.close();
        logger.debug("Finished @After");
    }

    protected void assertEquals(@Nonnull final TargetInfo left, @Nonnull final TargetInfo right) {
        Assert.assertEquals(left.getId(), right.getId());
        Assert.assertEquals(left.getProbeId(), right.getProbeId());
        Assert.assertEquals(left.getAccountData(), right.getAccountData());
    }

    protected static TargetInfo wrapTarget(@Nonnull final Target target) {
        final GroupScopeResolver groupScopeResolver = Mockito.mock(GroupScopeResolver.class);
        Mockito.when(groupScopeResolver.processGroupScope(any(), any(), any()))
                .then(AdditionalAnswers.returnsSecondArg());

        final List<InputField> fields = target.getMediationAccountVals(groupScopeResolver).stream()
                        .map(AbstractApiCallsTest::convertToRest).collect(Collectors.toList());
        final TargetSpec spec =
                        new TargetSpec(target.getProbeId(), fields);
        return new TargetRESTApi.TargetInfo(target.getId(), target.getDisplayName(), null, spec, true, "Validated",
                LocalDateTime.now());
    }

    private static InputField convertToRest(@Nonnull final AccountValue src) {
        final List<List<String>> groupScopeValue = src.getGroupScopePropertyValuesList().stream()
                        .map(PropertyValueList::getValueList).collect(Collectors.toList());
        return new InputField(src.getKey(), src.getStringValue(), Optional.of(groupScopeValue));
    }

    protected TopoBroadcastManager getEntitiesListener() {
        return integrationTestServer.getBean(TopoBroadcastManager.class);
    }

    protected TopologyProcessor getTopologyProcessor() {
        return topologyProcessor;
    }
}
