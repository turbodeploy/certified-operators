package com.vmturbo.plan.orchestrator.scenario;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.auth.api.authorization.scoping.EntityAccessScope;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanScope;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanScopeEntry;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioInfo;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.identity.ArrayOidSet;

/**
 *
 */
public class ScenarioScopeAccessCheckerTest {
    private GroupServiceMole groupServiceMole = spy(GroupServiceMole.class);

    private GroupServiceBlockingStub groupServiceClient;

    private GrpcTestServer groupGrpcServer;

    private final UserSessionContext userSessionContext = mock(UserSessionContext.class);

    private ScenarioScopeAccessChecker scenarioScopeAccessChecker;


    @Before
    public void setup() throws Exception {
        // test grpc server for the group service -- it's separate to avoid a circular dependency
        groupGrpcServer = GrpcTestServer.newServer(groupServiceMole);
        groupGrpcServer.start();
        groupServiceClient = GroupServiceGrpc.newBlockingStub(groupGrpcServer.getChannel());
        scenarioScopeAccessChecker = new ScenarioScopeAccessChecker(userSessionContext, groupServiceClient);
    }


    @Test
    public void testScenarioInfoScopeAccessDefault() {
        // verify that a regular non-scoped user should have access to a scoped scenario
        when(userSessionContext.isUserScoped()).thenReturn(false);
        ScenarioInfo scenarioInfo = ScenarioInfo.newBuilder()
                .setScope(PlanScope.newBuilder()
                        .addScopeEntries(PlanScopeEntry.newBuilder()
                                .setScopeObjectOid(1)))
                .build();
        assertTrue(scenarioScopeAccessChecker.checkScenarioAccess(scenarioInfo));
    }

    @Test
    public void testScenarioInfoScopeAccessScopeMatches() {
        // verify that a scoped user will have access as long as the scope objects are in scope
        EntityAccessScope accessScope = new EntityAccessScope(null, null,
                new ArrayOidSet(Arrays.asList(1L)), null);
        when(userSessionContext.isUserScoped()).thenReturn(true);
        when(userSessionContext.getUserAccessScope()).thenReturn(accessScope);
        ScenarioInfo scenarioInfo = ScenarioInfo.newBuilder()
                .setScope(PlanScope.newBuilder()
                        .addScopeEntries(PlanScopeEntry.newBuilder()
                                .setClassName("Entity")
                                .setScopeObjectOid(1)))
                .build();

        assertTrue(scenarioScopeAccessChecker.checkScenarioAccess(scenarioInfo));
    }

    @Test(expected = StatusRuntimeException.class)
    public void testScenarioInfoScopeAccessGroupOutOfScope() {
        // verify that a scoped user will have access as long as the scope objects are in scope
        EntityAccessScope accessScope = new EntityAccessScope(null, null,
                new ArrayOidSet(Arrays.asList(1L)), null);
        when(userSessionContext.isUserScoped()).thenReturn(true);
        when(userSessionContext.getUserAccessScope()).thenReturn(accessScope);

        // fake a group of out scope error
        when(groupServiceMole.getGroups(any())).thenThrow(new StatusRuntimeException(Status.PERMISSION_DENIED));

        ScenarioInfo scenarioInfo = ScenarioInfo.newBuilder()
                .setScope(PlanScope.newBuilder()
                        .addScopeEntries(PlanScopeEntry.newBuilder()
                                .setClassName("Group")
                                .setScopeObjectOid(2)))
                .build();
        // this should trigger an grpc exception
        assertFalse(scenarioScopeAccessChecker.checkScenarioAccess(scenarioInfo));
    }



}
