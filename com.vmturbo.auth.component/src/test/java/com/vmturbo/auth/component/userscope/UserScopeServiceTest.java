package com.vmturbo.auth.component.userscope;

import java.time.Clock;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

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
import jdk.nashorn.internal.ir.debug.ObjectSizeCalculator;

import com.vmturbo.api.enums.EntityState;
import com.vmturbo.auth.api.authorization.jwt.JwtClientInterceptor;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse.Members;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceImplBase;
import com.vmturbo.common.protobuf.repository.SupplyChain.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChain.SupplyChainNode.MemberList;
import com.vmturbo.common.protobuf.repository.SupplyChain.SupplyChainRequest;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceImplBase;
import com.vmturbo.common.protobuf.userscope.UserScope.CurrentUserEntityAccessScopeRequest;
import com.vmturbo.common.protobuf.userscope.UserScope.EntityAccessScopeContents;
import com.vmturbo.common.protobuf.userscope.UserScope.EntityAccessScopeRequest;
import com.vmturbo.common.protobuf.userscope.UserScope.EntityAccessScopeResponse;
import com.vmturbo.common.protobuf.userscope.UserScope.OidSetDTO;
import com.vmturbo.common.protobuf.userscope.UserScope.OidSetDTO.AllOids;
import com.vmturbo.common.protobuf.userscope.UserScope.OidSetDTO.OidArray;
import com.vmturbo.common.protobuf.userscope.UserScopeServiceGrpc;
import com.vmturbo.common.protobuf.userscope.UserScopeServiceGrpc.UserScopeServiceBlockingStub;
import com.vmturbo.components.api.test.GrpcTestServer;

/**
 *
 */
public class UserScopeServiceTest {

    private static List<Long> TEST_SUPPLY_CHAIN_OIDS = Arrays.asList(10L, 11L, 12L);

    private GroupServiceImplBase groupService = new TestGroupService();

    private TestSupplyChainService supplyChainService = new TestSupplyChainService();

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

    // we are going to create some services that return test data
    private static class TestGroupService extends GroupServiceImplBase {

        @Override
        public void getMembers(final GetMembersRequest request, final StreamObserver<GetMembersResponse> responseObserver) {
            long groupId = request.getId();
            if (groupId == 1) {
                responseObserver.onNext(GetMembersResponse.newBuilder()
                        .setMembers(Members.newBuilder()
                                .addIds(2L))
                        .build());

            } else {
                // default -- no members
                responseObserver.onNext(GetMembersResponse.getDefaultInstance());
            }
            responseObserver.onCompleted();
        }
    }

    private static class TestSupplyChainService extends SupplyChainServiceImplBase {

        private Map<Integer, MemberList> testSupplyChainMap = Collections.singletonMap(
                EntityState.ACTIVE.ordinal(), MemberList.newBuilder()
                        .addAllMemberOids(TEST_SUPPLY_CHAIN_OIDS)
                        .build()
        );

        @Override
        public void getSupplyChain(final SupplyChainRequest request, final StreamObserver<SupplyChainNode> responseObserver) {
            if (request.getStartingEntityOid(0) == 2L) {
                responseObserver.onNext(SupplyChainNode.newBuilder()
                        .putAllMembersByState(testSupplyChainMap)
                        .build());
            }
            responseObserver.onCompleted();
        }
    }

}
