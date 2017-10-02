package com.vmturbo.api.component.external.api.service;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.List;

import javax.annotation.Nonnull;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import io.grpc.stub.StreamObserver;

import com.vmturbo.api.component.external.api.mapper.ActionSpecMapper;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.common.protobuf.action.ActionDTO.AcceptActionResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.SingleActionRequest;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc;
import com.vmturbo.components.api.test.GrpcTestServer;


/**
 * Test the service methods for the /actions endpoint.
 */
public class ActionsServiceTest {

    public static final String[] ALL_ACTION_MODES = {
            "RECOMMEND", "DISABLED", "MANUAL", "AUTOMATIC"
    };
    /**
     * The backend the API forwards calls to (i.e. the part that's in the plan orchestrator).
     */
    private final TestActionsRpcService actionsServiceBackend =
            Mockito.spy(new TestActionsRpcService());

    private GrpcTestServer grpcServer;

    private ActionsService actionsServiceUnderTest;

    ActionsServiceGrpc.ActionsServiceBlockingStub actionsRpcService;

    private ActionSpecMapper actionSpecMapper;

    private final long REALTIME_TOPOLOGY_ID = 777777L;

    private final String UUID = "12345";

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setup() throws IOException {

        // set up a mock Actions RPC server
        grpcServer = GrpcTestServer.withServices(actionsServiceBackend);
        actionSpecMapper = Mockito.mock(ActionSpecMapper.class);
        actionsRpcService = ActionsServiceGrpc.newBlockingStub(grpcServer.getChannel());

        // set up the ActionsService to test
        actionsServiceUnderTest = new ActionsService(actionsRpcService, actionSpecMapper,
                REALTIME_TOPOLOGY_ID);
    }

    @After
    public void teardown() {
        grpcServer.close();
    }

    /**
     * Test the return of available action modes for a given actionType and seType.
     * Note that the actionType and seType are currently ignored in XL, pending implementation
     * of user permissions, etc.
     */
    @Test
    public void testGetAvailActionModes() throws Exception {
        // Arrange

        // note that the getAvailableActions()
        String actionType = "ignored";
        String seType = "ignored";
        // Act
        List<String> modes = actionsServiceUnderTest.getAvailActionModes(actionType, seType);
        // Assert
        assertThat(modes, containsInAnyOrder(ALL_ACTION_MODES));

    }

    /**
     * Test execute action throw correct exception when RPC call return errors
     * @throws Exception
     */
    @Test
    public void testExecuteActionThrowException() throws Exception {
        expectedException.expect(UnknownObjectException.class);
        actionsServiceUnderTest.executeAction(UUID, true);
    }

    /**
     * This class mocks backend ActionsService RPC calls. An instance is injected into the
     * {@link ActionsService} under test.
     * <p>
     * No method implementations are currently needed, but will be required as we test
     * methods in ActionsService that make RPC calls.
     */
    private class TestActionsRpcService extends ActionsServiceGrpc.ActionsServiceImplBase {

        /**
         * Mock RPC accept action call return action not exist error
         */
        @Override
        public void acceptAction(SingleActionRequest request,
                                 StreamObserver<AcceptActionResponse> responseObserver) {
            responseObserver.onNext(acceptanceError("Action not exist"));
            responseObserver.onCompleted();
        }
    }

    private static AcceptActionResponse acceptanceError(@Nonnull final String error) {
        return AcceptActionResponse.newBuilder()
                .setError(error)
                .build();
    }
}