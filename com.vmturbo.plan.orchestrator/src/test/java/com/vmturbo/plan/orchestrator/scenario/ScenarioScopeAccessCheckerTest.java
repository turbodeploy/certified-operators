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

import com.google.common.collect.ImmutableList;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.auth.api.authorization.AuthorizationException.UserAccessScopeException;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.auth.api.authorization.scoping.EntityAccessScope;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.PlanScope;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.PlanScopeEntry;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioInfo;
import com.vmturbo.common.protobuf.repository.RepositoryDTOMoles.RepositoryServiceMole;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetSupplyChainResponse;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChain;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChainProtoMoles;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;
import com.vmturbo.common.protobuf.search.Search.SearchEntityOidsResponse;
import com.vmturbo.common.protobuf.search.SearchMoles.SearchServiceMole;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntityBatch;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.identity.ArrayOidSet;

/**
 *
 */
public class ScenarioScopeAccessCheckerTest {
    private static final ApiEntityType VM = ApiEntityType.VIRTUAL_MACHINE;
    private static final ApiEntityType REGION = ApiEntityType.REGION;

    private GroupServiceMole groupServiceMole = spy(GroupServiceMole.class);

    private GroupServiceBlockingStub groupServiceClient;

    private GrpcTestServer groupGrpcServer;

    private SearchServiceMole searchServiceMole = spy(SearchServiceMole.class);

    private SearchServiceBlockingStub searchServiceClient;

    private GrpcTestServer searchGrpcServer;

    private RepositoryServiceBlockingStub repositoryServiceClient;

    private RepositoryServiceMole repositoryServiceMole = spy(RepositoryServiceMole.class);

    private GrpcTestServer repositoryGrpcServer;

    private SupplyChainProtoMoles.SupplyChainServiceMole supplyChainMole =
            spy(new SupplyChainProtoMoles.SupplyChainServiceMole());

    private SupplyChainServiceBlockingStub supplyChainServiceClient;

    private GrpcTestServer supplyChainGrpcServer;

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

        repositoryGrpcServer = GrpcTestServer.newServer(repositoryServiceMole);
        repositoryGrpcServer.start();
        repositoryServiceClient = RepositoryServiceGrpc.newBlockingStub(repositoryGrpcServer.getChannel());

        supplyChainGrpcServer = GrpcTestServer.newServer(supplyChainMole);
        supplyChainGrpcServer.start();
        supplyChainServiceClient = SupplyChainServiceGrpc.newBlockingStub(supplyChainGrpcServer.getChannel());

        scenarioScopeAccessChecker = new ScenarioScopeAccessChecker(userSessionContext,
                groupServiceClient, searchServiceClient, supplyChainServiceClient, repositoryServiceClient);
    }

    @Test
    public void testScenarioInfoScopeAccessDefault() throws ScenarioScopeNotFoundException {
        // verify that a regular non-scoped user should have access to a scoped scenario
        when(userSessionContext.isUserScoped()).thenReturn(false);
        when(groupServiceMole.getMembers(any())).thenReturn(ImmutableList.of(
                GetMembersResponse.newBuilder().setGroupId(1L).addMemberId(100L).build()));
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

        final SupplyChainNode virtualMachine = SupplyChainNode.newBuilder()
                .setEntityType(VM.typeNumber())
                .putMembersByState(TopologyDTO.EntityState.POWERED_ON_VALUE,
                        SupplyChainNode.MemberList.newBuilder().addMemberOids(1L).build())
                .build();
        when(supplyChainMole.getSupplyChain(any()))
                .thenReturn(GetSupplyChainResponse.newBuilder()
                        .setSupplyChain(SupplyChain.newBuilder()
                                .addSupplyChainNodes(virtualMachine))
                        .build());

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
        when(groupServiceMole.getMembers(any())).thenThrow(new StatusRuntimeException(Status.PERMISSION_DENIED));

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

    /**
     * Verify that a scenario scope set to a Region is updated based on the User scope,
     * not including the VMs he doesn't have access to.
     *
     * @throws ScenarioScopeNotFoundException exception thrown if scenario scope can not be found
     */
    @Test
    public void testScenarioInfoScopeAccessWithEntitySupplyChainIntersect()
            throws ScenarioScopeNotFoundException {
        long regionOid = 1L;
        long vmOid = 100L;
        long vmOutOfScopeOid = 101L;

        // Setup the scenario scope
        ScenarioInfo scenarioInfo = ScenarioInfo.newBuilder()
                .setScope(PlanScope.newBuilder()
                        .addScopeEntries(PlanScopeEntry.newBuilder()
                                .setClassName(REGION.apiStr())
                                .setScopeObjectOid(regionOid)))
                .build();
        // Setup the User scope with 1 VM and the its Region
        when(userSessionContext.isUserScoped()).thenReturn(true);
        EntityAccessScope accessScope = new EntityAccessScope(null, null,
                new ArrayOidSet(Arrays.asList(regionOid, vmOid)), null);
        when(userSessionContext.getUserAccessScope()).thenReturn(accessScope);
        // Setup the Search to return the Region
        when(searchServiceMole.searchEntityOids(any())).thenReturn(
                SearchEntityOidsResponse.newBuilder().addEntities(regionOid).build());
        // Setup the supply chain to return the VMs on the Region
        final SupplyChainNode virtualMachineNode = SupplyChainNode.newBuilder()
                .setEntityType(VM.typeNumber())
                .putMembersByState(TopologyDTO.EntityState.POWERED_ON_VALUE,
                        SupplyChainNode.MemberList.newBuilder()
                                .addMemberOids(vmOid)
                                .addMemberOids(vmOutOfScopeOid)
                                .build())
                .build();
        when(supplyChainMole.getSupplyChain(any()))
                .thenReturn(GetSupplyChainResponse.newBuilder()
                        .setSupplyChain(SupplyChain.newBuilder()
                                .addSupplyChainNodes(virtualMachineNode))
                        .build());
        when(repositoryServiceMole.retrieveTopologyEntities(any()))
                .thenReturn(Collections.singletonList(PartialEntityBatch.newBuilder().addEntities(
                        PartialEntity.newBuilder().setMinimal(
                                MinimalEntity.newBuilder().setOid(vmOid).build())).build()));

        // Act
        ScenarioInfo info = scenarioScopeAccessChecker.checkScenarioAccessAndValidateScopes(scenarioInfo);

        // Assert
        assertEquals(1, info.getScope().getScopeEntriesList().size());
        assertEquals(vmOid, info.getScope().getScopeEntriesList().get(0).getScopeObjectOid());
    }

    /**
     * Verify that an error is thrown when the scenario scope set to a Region
     * where the User doesn't have access to any VMs.
     *
     * @throws ScenarioScopeNotFoundException exception thrown if scenario scope can not be found
     */
    @Test(expected = UserAccessScopeException.class)
    public void testScenarioInfoScopeAccessWithNoEntitySupplyChainIntersect()
            throws ScenarioScopeNotFoundException {
        long regionOid = 1L;
        long vmOid = 100L;
        long vmOutOfScopeOid = 101L;

        // Setup the scenario scope
        ScenarioInfo scenarioInfo = ScenarioInfo.newBuilder()
                .setScope(PlanScope.newBuilder()
                        .addScopeEntries(PlanScopeEntry.newBuilder()
                                .setClassName(REGION.apiStr())
                                .setScopeObjectOid(regionOid)))
                .build();
        // Setup the User scope with 1 VM and the a Region
        when(userSessionContext.isUserScoped()).thenReturn(true);
        EntityAccessScope accessScope = new EntityAccessScope(null, null,
                new ArrayOidSet(Arrays.asList(regionOid, vmOid)), null);
        when(userSessionContext.getUserAccessScope()).thenReturn(accessScope);
        // Setup the Search to return the Region
        when(searchServiceMole.searchEntityOids(any())).thenReturn(
                SearchEntityOidsResponse.newBuilder().addEntities(regionOid).build());
        // Setup the supply chain to return the VMs on the Region
        final SupplyChainNode virtualMachineNode = SupplyChainNode.newBuilder()
                .setEntityType(VM.typeNumber())
                .putMembersByState(TopologyDTO.EntityState.POWERED_ON_VALUE,
                        SupplyChainNode.MemberList.newBuilder()
                                .addMemberOids(vmOutOfScopeOid)
                                .build())
                .build();
        when(supplyChainMole.getSupplyChain(any()))
                .thenReturn(GetSupplyChainResponse.newBuilder()
                        .setSupplyChain(SupplyChain.newBuilder()
                                .addSupplyChainNodes(virtualMachineNode))
                        .build());

        // Act
        scenarioScopeAccessChecker.checkScenarioAccessAndValidateScopes(scenarioInfo);
    }

    /**
     * Verify that a scenario scope set to a Group is updated based on the User scope,
     * not including the VMs he doesn't have access to.
     *
     * @throws ScenarioScopeNotFoundException exception thrown if scenario scope can not be found
     */
    @Test
    public void testScenarioInfoScopeAccessWithGroupSupplyChainIntersect()
            throws ScenarioScopeNotFoundException {
        long groupOid = 1L;
        long vmOid = 100L;
        long vmOutOfScopeOid = 101L;

        // Setup the scenario scope
        ScenarioInfo scenarioInfo = ScenarioInfo.newBuilder()
                .setScope(PlanScope.newBuilder()
                        .addScopeEntries(PlanScopeEntry.newBuilder()
                                .setClassName(StringConstants.GROUP)
                                .setScopeObjectOid(groupOid)))
                .build();
        // Setup the User scope with 1 VM
        when(userSessionContext.isUserScoped()).thenReturn(true);
        EntityAccessScope accessScope = new EntityAccessScope(null, null,
                new ArrayOidSet(Arrays.asList(vmOid)), null);
        when(userSessionContext.getUserAccessScope()).thenReturn(accessScope);
        // Setup the request for the Group members
        when(groupServiceMole.getMembers(any())).thenReturn(ImmutableList.of(
                GetMembersResponse.newBuilder().setGroupId(groupOid)
                        .addMemberId(vmOid)
                        .addMemberId(vmOutOfScopeOid)
                        .build()));
        // Setup the supply chain to return the VMs
        final SupplyChainNode virtualMachineNode = SupplyChainNode.newBuilder()
                .setEntityType(VM.typeNumber())
                .putMembersByState(TopologyDTO.EntityState.POWERED_ON_VALUE,
                        SupplyChainNode.MemberList.newBuilder()
                                .addMemberOids(vmOid)
                                .addMemberOids(vmOutOfScopeOid)
                                .build())
                .build();
        when(supplyChainMole.getSupplyChain(any()))
                .thenReturn(GetSupplyChainResponse.newBuilder()
                        .setSupplyChain(SupplyChain.newBuilder()
                                .addSupplyChainNodes(virtualMachineNode))
                        .build());
        when(repositoryServiceMole.retrieveTopologyEntities(any()))
                .thenReturn(Collections.singletonList(PartialEntityBatch.newBuilder().addEntities(
                        PartialEntity.newBuilder().setMinimal(
                                MinimalEntity.newBuilder().setOid(vmOid).build())).build()));


        // Act
        ScenarioInfo info = scenarioScopeAccessChecker.checkScenarioAccessAndValidateScopes(scenarioInfo);

        // Assert
        assertEquals(1, info.getScope().getScopeEntriesList().size());
        assertEquals(vmOid, info.getScope().getScopeEntriesList().get(0).getScopeObjectOid());
    }

    /**
     * If user has access to all workloads in the scenario scope, there is no need to
     * rewrite the scenario scope. Verify that the scope entity list is not changed.
     *
     * @throws ScenarioScopeNotFoundException exception thrown if scenario scope can not be found
     */
    @Test
    public void testScenarioInfoScopeAccessWithGroupSupplyChainNoScopeRewrite()
            throws ScenarioScopeNotFoundException {
        long groupOid = 1L;
        long vmOid1 = 100L;
        long vmOid2 = 101L;

        // Setup the scenario scope
        ScenarioInfo scenarioInfo = ScenarioInfo.newBuilder()
                .setScope(PlanScope.newBuilder()
                        .addScopeEntries(PlanScopeEntry.newBuilder()
                                .setClassName(StringConstants.GROUP)
                                .setScopeObjectOid(groupOid)))
                .build();
        // Setup the User scope with 1 VM
        when(userSessionContext.isUserScoped()).thenReturn(true);
        EntityAccessScope accessScope = new EntityAccessScope(null, null,
                new ArrayOidSet(Arrays.asList(vmOid1, vmOid2)), null);
        when(userSessionContext.getUserAccessScope()).thenReturn(accessScope);
        // Setup the request for the Group members
        when(groupServiceMole.getMembers(any())).thenReturn(ImmutableList.of(
                GetMembersResponse.newBuilder().setGroupId(groupOid)
                        .addMemberId(vmOid1)
                        .build(),
                GetMembersResponse.newBuilder().setGroupId(groupOid)
                        .addMemberId(vmOid2)
                        .build()));
        // Setup the supply chain to return the VMs
        final SupplyChainNode virtualMachineNode = SupplyChainNode.newBuilder()
                .setEntityType(VM.typeNumber())
                .putMembersByState(TopologyDTO.EntityState.POWERED_ON_VALUE,
                        SupplyChainNode.MemberList.newBuilder()
                                .addAllMemberOids(ImmutableList.of(vmOid1, vmOid2))
                                .build())
                .build();
        when(supplyChainMole.getSupplyChain(any()))
                .thenReturn(GetSupplyChainResponse.newBuilder()
                        .setSupplyChain(SupplyChain.newBuilder()
                                .addSupplyChainNodes(virtualMachineNode))
                        .build());

        // Act
        ScenarioInfo info = scenarioScopeAccessChecker.checkScenarioAccessAndValidateScopes(scenarioInfo);

        // Assert: Scope entity list should not be changed.
        assertEquals(1, info.getScope().getScopeEntriesList().size());
        assertEquals(groupOid, info.getScope().getScopeEntriesList().get(0).getScopeObjectOid());
    }

    /**
     * If user does not have access to any workloads of the plan scope, expect an exception thrown
     * by checkScenarioAccessAndValidateScopes.
     *
     * @throws ScenarioScopeNotFoundException exception thrown if scenario scope can not be found
     */
    @Test(expected = UserAccessScopeException.class)
    public void testScenarioInfoScopeAccessWithGroupSupplyChainNoAccess()
            throws ScenarioScopeNotFoundException {
        long groupOid = 1L;
        long vmOid1 = 100L;
        long vmOid2 = 101L;
        long vmOid3 = 102L;

        // Setup the scenario scope
        ScenarioInfo scenarioInfo = ScenarioInfo.newBuilder()
                .setScope(PlanScope.newBuilder()
                        .addScopeEntries(PlanScopeEntry.newBuilder()
                                .setClassName(StringConstants.GROUP)
                                .setScopeObjectOid(groupOid)))
                .build();
        // Setup the User scope with 1 VM
        when(userSessionContext.isUserScoped()).thenReturn(true);
        EntityAccessScope accessScope = new EntityAccessScope(null, null,
                new ArrayOidSet(Arrays.asList(vmOid3)), null);
        when(userSessionContext.getUserAccessScope()).thenReturn(accessScope);
        // Setup the request for the Group members
        when(groupServiceMole.getMembers(any())).thenReturn(ImmutableList.of(
                GetMembersResponse.newBuilder().setGroupId(groupOid)
                        .addMemberId(vmOid1)
                        .addMemberId(vmOid2)
                        .build()));
        // Setup the supply chain to return the VMs
        final SupplyChainNode virtualMachineNode = SupplyChainNode.newBuilder()
                .setEntityType(VM.typeNumber())
                .putMembersByState(TopologyDTO.EntityState.POWERED_ON_VALUE,
                        SupplyChainNode.MemberList.newBuilder()
                                .addMemberOids(vmOid1)
                                .addMemberOids(vmOid2)
                                .build())
                .build();
        when(supplyChainMole.getSupplyChain(any()))
                .thenReturn(GetSupplyChainResponse.newBuilder()
                        .setSupplyChain(SupplyChain.newBuilder()
                                .addSupplyChainNodes(virtualMachineNode))
                        .build());

        // Act
        ScenarioInfo info = scenarioScopeAccessChecker.checkScenarioAccessAndValidateScopes(scenarioInfo);
    }

    /**
     * If scenario is for OCP, throw exception if user does not have access to all workloads of the
     * plan scope.
     *
     * @throws ScenarioScopeNotFoundException exception thrown if scenario scope can not be found
     */
    @Test(expected = UserAccessScopeException.class)
    public void testScenarioInfoScopeAccessWithGroupSupplyChainOCP()
            throws ScenarioScopeNotFoundException {
        long groupOid = 1L;
        long vmOid = 100L;
        long vmOutOfScopeOid = 101L;

        // Setup the scenario scope
        ScenarioInfo scenarioInfo = ScenarioInfo.newBuilder()
                .setType(StringConstants.OPTIMIZE_CLOUD_PLAN)
                .setScope(PlanScope.newBuilder()
                        .addScopeEntries(PlanScopeEntry.newBuilder()
                                .setClassName(StringConstants.GROUP)
                                .setScopeObjectOid(groupOid)))
                .build();
        // Setup the User scope with 1 VM
        when(userSessionContext.isUserScoped()).thenReturn(true);
        EntityAccessScope accessScope = new EntityAccessScope(null, null,
                new ArrayOidSet(Arrays.asList(vmOid)), null);
        when(userSessionContext.getUserAccessScope()).thenReturn(accessScope);
        // Setup the request for the Group members
        when(groupServiceMole.getMembers(any())).thenReturn(ImmutableList.of(
                GetMembersResponse.newBuilder().setGroupId(groupOid)
                        .addMemberId(vmOid)
                        .addMemberId(vmOutOfScopeOid)
                        .build()));
        // Setup the supply chain to return the VMs
        final SupplyChainNode virtualMachineNode = SupplyChainNode.newBuilder()
                .setEntityType(VM.typeNumber())
                .putMembersByState(TopologyDTO.EntityState.POWERED_ON_VALUE,
                        SupplyChainNode.MemberList.newBuilder()
                                .addMemberOids(vmOid)
                                .addMemberOids(vmOutOfScopeOid)
                                .build())
                .build();
        when(supplyChainMole.getSupplyChain(any()))
                .thenReturn(GetSupplyChainResponse.newBuilder()
                        .setSupplyChain(SupplyChain.newBuilder()
                                .addSupplyChainNodes(virtualMachineNode))
                        .build());

        // Act
        ScenarioInfo info = scenarioScopeAccessChecker.checkScenarioAccessAndValidateScopes(scenarioInfo);
    }


}
