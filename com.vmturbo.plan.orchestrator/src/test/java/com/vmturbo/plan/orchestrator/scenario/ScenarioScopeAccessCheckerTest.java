package com.vmturbo.plan.orchestrator.scenario;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.auth.api.authorization.scoping.EntityAccessScope;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.PlanScope;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.PlanScopeEntry;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioInfo;
import com.vmturbo.common.protobuf.search.Search.SearchEntityOidsResponse;
import com.vmturbo.common.protobuf.search.SearchMoles.SearchServiceMole;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.identity.ArrayOidSet;
import com.vmturbo.components.common.utils.StringConstants;

/**
 *
 */
public class ScenarioScopeAccessCheckerTest {
    private GroupServiceMole groupServiceMole = spy(GroupServiceMole.class);

    private GroupServiceBlockingStub groupServiceClient;

    private GrpcTestServer groupGrpcServer;

    private SearchServiceMole searchServiceMole = spy(SearchServiceMole.class);

    private SearchServiceBlockingStub searchServiceClient;

    private GrpcTestServer searchGrpcServer;

    private final UserSessionContext userSessionContext = mock(UserSessionContext.class);

    private ScenarioScopeAccessChecker scenarioScopeAccessChecker;


    @Before
    public void setup() throws Exception {
        // test grpc server for the group service -- it's separate to avoid a circular dependency
        groupGrpcServer = GrpcTestServer.newServer(groupServiceMole);
        groupGrpcServer.start();
        groupServiceClient = GroupServiceGrpc.newBlockingStub(groupGrpcServer.getChannel());

        searchGrpcServer = GrpcTestServer.newServer(searchServiceMole);
        searchGrpcServer.start();
        searchServiceClient = SearchServiceGrpc.newBlockingStub(searchGrpcServer.getChannel());

        scenarioScopeAccessChecker = new ScenarioScopeAccessChecker(userSessionContext,
            groupServiceClient, searchServiceClient);
    }

    @Test
    public void testScenarioInfoScopeAccessDefault() throws ScenarioScopeNotFoundException {
        // verify that a regular non-scoped user should have access to a scoped scenario
        when(userSessionContext.isUserScoped()).thenReturn(false);
        when(groupServiceMole.getGroups(any())).thenReturn(Collections.singletonList(
            Grouping.newBuilder().setId(1).build()));
        ScenarioInfo scenarioInfo = ScenarioInfo.newBuilder()
                .setScope(PlanScope.newBuilder()
                        .addScopeEntries(PlanScopeEntry.newBuilder()
                            .setScopeObjectOid(1)
                            .setClassName("Group")))
                .build();
        scenarioScopeAccessChecker.checkScenarioAccessAndValidateScopes(scenarioInfo);
    }

    @Test
    public void testScenarioInfoScopeAccessScopeMatches() throws ScenarioScopeNotFoundException {
        // verify that a scoped user will have access as long as the scope objects are in scope
        EntityAccessScope accessScope = new EntityAccessScope(null, null,
                new ArrayOidSet(Arrays.asList(1L)), null);
        when(userSessionContext.isUserScoped()).thenReturn(true);
        when(userSessionContext.getUserAccessScope()).thenReturn(accessScope);
        when(searchServiceMole.searchEntityOids(any())).thenReturn(
            SearchEntityOidsResponse.newBuilder().addEntities(1L).build());
        ScenarioInfo scenarioInfo = ScenarioInfo.newBuilder()
                .setScope(PlanScope.newBuilder()
                        .addScopeEntries(PlanScopeEntry.newBuilder()
                                .setClassName("Entity")
                                .setScopeObjectOid(1)))
                .build();

        scenarioScopeAccessChecker.checkScenarioAccessAndValidateScopes(scenarioInfo);
    }

    @Test(expected = StatusRuntimeException.class)
    public void testScenarioInfoScopeAccessGroupOutOfScope() throws ScenarioScopeNotFoundException {
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
        scenarioScopeAccessChecker.checkScenarioAccessAndValidateScopes(scenarioInfo);
    }

    /**
     * Tests that Billing Family scope is correctly resolved by
     *  scenarioScopeAccessChecker.checkScenarioAccessAndValidateScopes(scenarioInfo).
     */
    @Test
    public void testScenarioInfoBillingFamilyScopeResolution() {
        PlanScope planScope = PlanScope.newBuilder()
                        .addScopeEntries(PlanScopeEntry.newBuilder()
                                .setClassName(StringConstants.BILLING_FAMILY)
                                .setScopeObjectOid(1))
                        .addScopeEntries(PlanScopeEntry.newBuilder()
                         .setClassName(StringConstants.BILLING_FAMILY)
                         .setScopeObjectOid(2))
                        .build();
        final ScenarioInfo scenarioInfo = ScenarioInfo.newBuilder()
                .setScope(planScope)
                .build();

        //groups is a group of 2 Billing Families that are part of the scenario scopes.
        final List<Long> scopeGroupIds = new ArrayList<>();
        scopeGroupIds.add(1L);
        scopeGroupIds.add(2L);
        final List<Grouping> groups = new ArrayList<>();
        groups.add(Grouping.newBuilder().setId(1L).build());
        groups.add(Grouping.newBuilder().setId(2L).build());

        // This test mimics code called by scenarioScopeAccessChecker.checkScenarioAccessAndValidateScopes(scenarioInfo)
        // to compare the number of Billing Families in the plan scenario to the number of resolved groups .
        when(groupServiceMole.getGroups(GetGroupsRequest.newBuilder()
                                        .setGroupFilter(GroupFilter.newBuilder()
                                                        .addAllId(scopeGroupIds))
                                        .build())).thenReturn(groups);
        assertEquals(scenarioInfo.getScope().getScopeEntriesCount(), groupServiceMole.getGroups(GetGroupsRequest.newBuilder()
                                                        .setGroupFilter(GroupFilter.newBuilder()
                                                                        .addAllId(scopeGroupIds))
                                                        .build()).size());
    }

    /**
     * Verify that scenario scope is invalid if the scope entry is a group which doesn't exist.
     *
     * @throws ScenarioScopeNotFoundException exception thrown if scenario scope can not be found
     */
    @Test(expected = ScenarioScopeNotFoundException.class)
    public void testScenarioInfoScopeAccessGroupDoesNotExist()
            throws ScenarioScopeNotFoundException {
        when(userSessionContext.isUserScoped()).thenReturn(false);
        // mock that a group doesn't exist
        when(groupServiceMole.getGroups(any())).thenReturn(Collections.emptyList());

        ScenarioInfo scenarioInfo = ScenarioInfo.newBuilder()
            .setScope(PlanScope.newBuilder()
                .addScopeEntries(PlanScopeEntry.newBuilder()
                    .setClassName("Group")
                    .setScopeObjectOid(1)))
            .build();
        // this should trigger a ScenarioScopeNotFoundException
        scenarioScopeAccessChecker.checkScenarioAccessAndValidateScopes(scenarioInfo);
    }

    /**
     * Verify that scenario scope is invalid if the scope entry is an entity which doesn't exist.
     *
     * @throws ScenarioScopeNotFoundException exception thrown if scenario scope can not be found
     */
    @Test(expected = ScenarioScopeNotFoundException.class)
    public void testScenarioInfoScopeAccessEntityDoesNotExist()
            throws ScenarioScopeNotFoundException {
        when(userSessionContext.isUserScoped()).thenReturn(false);
        // mock that an entity doesn't exist
        when(searchServiceMole.searchEntityOids(any())).thenReturn(
            SearchEntityOidsResponse.getDefaultInstance());

        ScenarioInfo scenarioInfo = ScenarioInfo.newBuilder()
            .setScope(PlanScope.newBuilder()
                .addScopeEntries(PlanScopeEntry.newBuilder()
                    .setClassName("DataCenter")
                    .setScopeObjectOid(1)))
            .build();
        // this should trigger a ScenarioScopeNotFoundException
        scenarioScopeAccessChecker.checkScenarioAccessAndValidateScopes(scenarioInfo);
    }
}
