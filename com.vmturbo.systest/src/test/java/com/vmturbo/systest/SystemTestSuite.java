package com.vmturbo.systest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.ClassRule;
import org.junit.rules.ExternalResource;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.common.protobuf.group.PolicyServiceGrpc;
import com.vmturbo.components.test.utilities.ComponentTestRule;
import com.vmturbo.components.test.utilities.communication.ComponentStubHost;
import com.vmturbo.components.test.utilities.component.ComponentCluster;
import com.vmturbo.components.test.utilities.component.ComponentUtils;

/**
 * Test Suite driver for System Tests. Loads the test server using the {@link ComponentTestRule}
 * framework. After all tests have run, brings the server down.
 **/
@RunWith(Suite.class)
@Suite.SuiteClasses({
        PlanOverPlanSysTest.class
})


public class SystemTestSuite {

    private static ComponentTestRule componentTestRule;

    private static final Logger logger = LogManager.getLogger();

    private static final String SYSTEST_PROJECT = "systest";

    @ClassRule
    public static ExternalResource getExternalResource() {
        return new ComponentClusterConfiguration();
    }

    private static class ComponentClusterConfiguration extends ExternalResource {

        /**
         * Before all tests, bring up a test server. The handle to the server needs to be
         * exposed to various system components.
         */
        @Override
        protected void before() {
            logger.info("Bring up the test server...");
            componentTestRule = ComponentTestRule.newBuilder()
                    .withComponentCluster(ComponentCluster.newBuilder()
                            .projectName(SYSTEST_PROJECT)
                            .withService(ComponentCluster.newService("topology-processor")
                                    .withConfiguration("groupHost", ComponentUtils.getDockerHostRoute())
                                    .logsToLogger(logger))
                            .withService(ComponentCluster.newService("mediation-stressprobe")
                                    .logsToLogger(logger))
                            .withService(ComponentCluster.newService("market")
                                    .logsToLogger(logger))
                            .withService(ComponentCluster.newService("repository")
                                    .logsToLogger(logger))
                            .withService(ComponentCluster.newService("history")
                                    .logsToLogger(logger))
                            .withService(ComponentCluster.newService("action-orchestrator")
                                    .logsToLogger(logger))
                            .withService(ComponentCluster.newService("plan-orchestrator")
                                    .logsToLogger(logger))
                            .withService(ComponentCluster.newService("api")
                                    .logsToLogger(logger))
                            .withService(ComponentCluster.newService("auth")
                                    .logsToLogger(logger)))
                    .withStubs(ComponentStubHost.newBuilder()
                            .withGrpcServices(new PolicyServiceStub()))
                    .noMetricsCollection(); // if you don't want metrics
//                    .scrapeClusterAndLocalMetricsToInflux(); // if you want metrics
            componentTestRule.getCluster().up();
            logger.info("..............test server running.  Tests begin");

        }

        /**
         * After all tests in the suite, bring down the test server.
         */
        @Override
        protected void after() {
            componentTestRule.getCluster().down();
        }
    }

    public static ComponentTestRule getComponentTestRule() {
        return componentTestRule;
    }

    /**
     * Stub for the PolicyService container since it is not implemented (nor necessary).
     */
    public static class PolicyServiceStub extends PolicyServiceGrpc.PolicyServiceImplBase {
        @Override
        public void getPolicies(final PolicyDTO.PolicyRequest request,
                                   final StreamObserver<PolicyDTO.PolicyResponse> responseObserver) {
            responseObserver.onCompleted();
        }
    }

}
