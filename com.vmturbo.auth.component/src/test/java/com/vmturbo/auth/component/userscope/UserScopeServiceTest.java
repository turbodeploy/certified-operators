package com.vmturbo.auth.component.userscope;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.time.Clock;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;

import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;

import io.grpc.stub.StreamObserver;

import com.vmturbo.api.enums.EntityState;
import com.vmturbo.auth.api.authorization.jwt.JwtClientInterceptor;
import com.vmturbo.auth.api.authorization.scoping.UserScopeUtils;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse.Members;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetSupplyChainRequest;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetSupplyChainResponse;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChain;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode.MemberList;
import com.vmturbo.common.protobuf.repository.SupplyChainProtoMoles.SupplyChainServiceMole;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;
import com.vmturbo.common.protobuf.userscope.UserScope.CurrentUserEntityAccessScopeRequest;
import com.vmturbo.common.protobuf.userscope.UserScope.EntityAccessScopeContents;
import com.vmturbo.common.protobuf.userscope.UserScope.EntityAccessScopeRequest;
import com.vmturbo.common.protobuf.userscope.UserScope.EntityAccessScopeResponse;
import com.vmturbo.common.protobuf.userscope.UserScope.OidSetDTO;
import com.vmturbo.common.protobuf.userscope.UserScopeServiceGrpc;
import com.vmturbo.common.protobuf.userscope.UserScopeServiceGrpc.UserScopeServiceBlockingStub;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 *
 */
public class UserScopeServiceTest {

    private static final long GROUP_ID = 1;

    private static final List<Long> TEST_SUPPLY_CHAIN_OIDS = Arrays.asList(10L, 11L, 12L);

    private static final SupplyChain TEST_SUPPLY_CHAIN = SupplyChain.newBuilder()
        .addSupplyChainNodes(SupplyChainNode.newBuilder()
            .setEntityType("TestType")
            .putMembersByState(EntityState.ACTIVE.ordinal(), MemberList.newBuilder()
                .addAllMemberOids(TEST_SUPPLY_CHAIN_OIDS)
                .build()))
        .build();

    private static final SupplyChain TEST_NON_INFRASTRUCTURE_SUPPLY_CHAIN = SupplyChain.newBuilder()
            .addSupplyChainNodes(SupplyChainNode.newBuilder()
                    .setEntityType(EntityType.APPLICATION.name())
                    .putMembersByState(EntityState.ACTIVE.ordinal(), MemberList.newBuilder()
                            .addMemberOids(10L)
                            .build()))
            .addSupplyChainNodes(SupplyChainNode.newBuilder()
                    .setEntityType(EntityType.VIRTUAL_MACHINE.name())
                    .putMembersByState(EntityState.ACTIVE.ordinal(), MemberList.newBuilder()
                            .addMemberOids(11L)
                            .build()))
            .build();


    private GroupServiceMole groupService = spy(new GroupServiceMole());

    private SupplyChainServiceMole supplyChainService = spy(new SupplyChainServiceMole());

    @Rule
    public GrpcTestServer mockServer = GrpcTestServer.newServer(groupService, supplyChainService);

    private GroupServiceBlockingStub groupServiceClient;

    private SupplyChainServiceBlockingStub supplyChainServiceClient;

    private UserScopeService userScopeService;

    // not using @Rule here because of circular dependencies with the user scope service relying on
    // client stubs that won't be ready until after the mockServer has started.
    public GrpcTestServer testServer = GrpcTestServer.newServer(userScopeService);

    private UserScopeServiceBlockingStub userScopeServiceClient;

    private Clock clock = Clock.systemUTC();

    @Before
    public void setup() throws Exception {

        final long memberId = 2;

        doReturn(GetMembersResponse.newBuilder()
            .setMembers(Members.newBuilder()
                .addIds(memberId))
            .build())
                .when(groupService).getMembers(GetMembersRequest.newBuilder()
                    .setId(GROUP_ID)
                    .setExpectPresent(false)
                    .setEnforceUserScope(false)
                    .setExpandNestedGroups(true)
                    .build());

        doReturn(GetSupplyChainResponse.newBuilder()
            .setSupplyChain(TEST_SUPPLY_CHAIN)
            .build()).when(supplyChainService).getSupplyChain(GetSupplyChainRequest.newBuilder()
                .addStartingEntityOid(memberId)
                .build());

        // if asked for a "shared" user, return a subset.
        doReturn(GetSupplyChainResponse.newBuilder()
            .setSupplyChain(TEST_NON_INFRASTRUCTURE_SUPPLY_CHAIN)
            .build()).when(supplyChainService).getSupplyChain(GetSupplyChainRequest.newBuilder()
                .addStartingEntityOid(memberId)
                .addAllEntityTypesToInclude(UserScopeUtils.SHARED_USER_ENTITY_TYPES)
                .build());

        groupServiceClient = GroupServiceGrpc.newBlockingStub(mockServer.getChannel());
        supplyChainServiceClient = SupplyChainServiceGrpc.newBlockingStub(mockServer.getChannel());

        userScopeService = new UserScopeService(groupServiceClient, supplyChainServiceClient, clock);

        testServer = GrpcTestServer.newServer(userScopeService);
        testServer.start();

        userScopeServiceClient = UserScopeServiceGrpc.newBlockingStub(testServer.getChannel())
                .withInterceptors(new JwtClientInterceptor());
    }

    @After
    public void teardown() {
        if (testServer != null && testServer.getChannel() != null) {
            testServer.close();
        }
    }

    // verify the group calls and supply chain mechanisms are working
    @Test
    public void testGetEntityAccessScopeMembers() {
        EntityAccessScopeResponse response = userScopeServiceClient.getEntityAccessScopeMembers(
                EntityAccessScopeRequest.newBuilder()
                    .addGroupId(1)
                    .build());

        verifyScopedAccess(response.getEntityAccessScopeContents());
    }

    // verify that the entity access scope is unrestricted
    private void verifyFullAccess(EntityAccessScopeContents contents) {
        // verify the accessible oids list is "all oids"
        Assert.assertTrue(contents.getAccessibleOids().hasAllOids());

        // also verify the seed oids list is empty
        Assert.assertTrue(contents.getSeedOids().hasNoOids());
    }

    // verify that the entity access scope is restricted to the test supply chain
    private void verifyScopedAccess(EntityAccessScopeContents contents) {
        // verify the accessible oids list is our supply chain
        OidSetDTO accessibleOids = contents.getAccessibleOids();
        Assert.assertTrue(accessibleOids.hasArray());
        Assert.assertThat(accessibleOids.getArray().getOidsList(),
                Matchers.containsInAnyOrder(TEST_SUPPLY_CHAIN_OIDS.toArray()));

        // also verify the seed oids list
        OidSetDTO seedOids = contents.getSeedOids();
        Assert.assertTrue(seedOids.hasArray());
        Assert.assertThat(seedOids.getArray().getOidsList(),
                Matchers.containsInAnyOrder(2L));
    }

    // test that an empty group list results in an unrestricted access scope.
    @Test
    public void testGetEntityAccessScopeMembersUnscoped() {
        EntityAccessScopeResponse response = userScopeServiceClient.getEntityAccessScopeMembers(
                EntityAccessScopeRequest.getDefaultInstance());
        verifyFullAccess(response.getEntityAccessScopeContents());
    }

    // test using a JWT for a "regular" non-scoped user.
    @Test
    public void testGetCurrentUserEntityAccessScopeMembers() {
        // tests using the JWT plumbing for GRPC are a bit difficult to set up at the moment, so we
        // are going to populate the current context and make a direct call the service, without
        // going through the client stub.

        // set an unscoped user in the context
        SecurityContextHolder.getContext().setAuthentication(
                new UsernamePasswordAuthenticationToken(
                        new AuthUserDTO(null,
                                "unscopedUser",
                                "password",
                                "10.10.10.10",
                                "11111",
                                "token",
                                ImmutableList.of("ADMINISTRATOR"),
                                null),
                        "",
                        Collections.emptySet()));

        StreamObserver<EntityAccessScopeResponse> responseObserver = Mockito.mock(StreamObserver.class);
        userScopeService.getCurrentUserEntityAccessScopeMembers(
                CurrentUserEntityAccessScopeRequest.getDefaultInstance(),
                responseObserver);

        ArgumentCaptor<EntityAccessScopeResponse> responseCaptor = ArgumentCaptor.forClass(EntityAccessScopeResponse.class);
        Mockito.verify(responseObserver).onNext(responseCaptor.capture());

        EntityAccessScopeResponse response = responseCaptor.getValue();

        verifyFullAccess(response.getEntityAccessScopeContents());
    }

    // test using a JWT for a "regular" non-scoped user.
    @Test
    public void testGetCurrentUserEntityAccessScopeMembersWithScope() {
        // tests using the JWT plumbing for GRPC are a bit difficult to set up at the moment, so we
        // are going to populate the current context and make a direct call the service, without
        // going through the client stub.

        // set an scoped user in the context
        SecurityContextHolder.getContext().setAuthentication(
                new UsernamePasswordAuthenticationToken(
                        new AuthUserDTO(null,
                                "scopedUser",
                                "password",
                                "10.10.10.10",
                                "11111",
                                "token",
                                ImmutableList.of("OBSERVER"),
                                Arrays.asList(1L)),
                        "",
                        Collections.emptySet()));

        StreamObserver<EntityAccessScopeResponse> responseObserver = Mockito.mock(StreamObserver.class);
        userScopeService.getCurrentUserEntityAccessScopeMembers(
                CurrentUserEntityAccessScopeRequest.getDefaultInstance(),
                responseObserver);

        ArgumentCaptor<EntityAccessScopeResponse> responseCaptor = ArgumentCaptor.forClass(EntityAccessScopeResponse.class);
        Mockito.verify(responseObserver).onNext(responseCaptor.capture());

        EntityAccessScopeResponse response = responseCaptor.getValue();

        verifyScopedAccess(response.getEntityAccessScopeContents());
    }

    // test using a JWT for a "shared"scoped user.
    @Test
    public void testGetCurrentUserEntityAccessScopeMembersShared() {
        // set an scoped user in the context
        SecurityContextHolder.getContext().setAuthentication(
                new UsernamePasswordAuthenticationToken(
                        new AuthUserDTO(null,
                                "scopedUser",
                                "password",
                                "10.10.10.10",
                                "11111",
                                "token",
                                ImmutableList.of("SHARED_OBSERVER"),
                                Arrays.asList(1L)),
                        "",
                        Collections.emptySet()));

        StreamObserver<EntityAccessScopeResponse> responseObserver = Mockito.mock(StreamObserver.class);
        userScopeService.getCurrentUserEntityAccessScopeMembers(
                CurrentUserEntityAccessScopeRequest.getDefaultInstance(),
                responseObserver);

        ArgumentCaptor<EntityAccessScopeResponse> responseCaptor = ArgumentCaptor.forClass(EntityAccessScopeResponse.class);
        Mockito.verify(responseObserver).onNext(responseCaptor.capture());

        EntityAccessScopeResponse response = responseCaptor.getValue();
        // verify the accessible oids list is our supply chain
        OidSetDTO accessibleOids = response.getEntityAccessScopeContents().getAccessibleOids();
        Assert.assertTrue(accessibleOids.hasArray());
        Assert.assertThat(accessibleOids.getArray().getOidsList(),
                Matchers.containsInAnyOrder(10L, 11L));
    }

    // test the "get current user access scope" when no local session exists.
    @Test
    public void testGetCurrentUserEntityAccessScopeMembersNoSession() {
        // set an scoped user in the context
        SecurityContextHolder.getContext().setAuthentication(null);

        StreamObserver<EntityAccessScopeResponse> responseObserver = Mockito.mock(StreamObserver.class);
        userScopeService.getCurrentUserEntityAccessScopeMembers(
                CurrentUserEntityAccessScopeRequest.getDefaultInstance(),
                responseObserver);

        ArgumentCaptor<EntityAccessScopeResponse> responseCaptor = ArgumentCaptor.forClass(EntityAccessScopeResponse.class);
        Mockito.verify(responseObserver).onNext(responseCaptor.capture());

        EntityAccessScopeResponse response = responseCaptor.getValue();

        verifyFullAccess(response.getEntityAccessScopeContents());
    }
}
