package com.vmturbo.api.component.external.api.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.google.common.collect.ImmutableList;

import io.grpc.Context;
import io.grpc.Status;

import junit.framework.TestCase;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.security.core.context.SecurityContextHolder;

import com.vmturbo.api.component.external.api.mapper.SuspensionMapper;
import com.vmturbo.api.dto.suspension.BulkActionRequestApiDTO;
import com.vmturbo.api.dto.suspension.BulkActionRequestInputDTO;
import com.vmturbo.api.dto.suspension.SuspendableEntityInputDTO;
import com.vmturbo.api.enums.SuspensionActionType;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.pagination.SuspensionEntitiesPaginationRequest;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.auth.api.authorization.jwt.SecurityConstant;
import com.vmturbo.common.protobuf.suspension.SuspensionEntityMoles.SuspensionEntityServiceMole;
import com.vmturbo.common.protobuf.suspension.SuspensionEntityOuterClass;
import com.vmturbo.common.protobuf.suspension.SuspensionEntityServiceGrpc;
import com.vmturbo.common.protobuf.suspension.SuspensionToggle;
import com.vmturbo.common.protobuf.suspension.SuspensionToggleMoles.SuspensionToggleServiceMole;
import com.vmturbo.common.protobuf.suspension.SuspensionToggleServiceGrpc;
import com.vmturbo.components.api.test.GrpcTestServer;

/**
 * Verifies the services functions in {@link SuspensionService}.
 */
public class SuspensionServiceTest {

    private final SuspensionMapper suspensionMapper = Mockito.mock(SuspensionMapper.class);
    private final UserSessionContext userSessionContext = Mockito.mock(UserSessionContext.class);
    private final SuspensionEntityServiceMole entityService = Mockito.spy(new SuspensionEntityServiceMole());
    private final SuspensionToggleServiceMole toggleService = Mockito.spy(new SuspensionToggleServiceMole());

    private SuspensionService suspensionService;


    /**
     * Testing gRPC server.
     */
    @Rule
    public final GrpcTestServer grpcServer = GrpcTestServer.newServer(entityService, toggleService);

    /**
     * Set up tests.
     *
     * @throws OperationFailedException To satisfy compiler.
     */
    @Before
    public void setUp() throws OperationFailedException {
        MockitoAnnotations.initMocks(this);
        suspensionService = new SuspensionService(SuspensionEntityServiceGrpc.newBlockingStub(grpcServer.getChannel()),
                SuspensionToggleServiceGrpc.newBlockingStub(grpcServer.getChannel()),
                userSessionContext, 10, 10);
    }

    /**
     * verifies the BulkAction method call.
     */
    @Test
    public void testBulkAction() {

        BulkActionRequestInputDTO bulkActionRequestInputDTO = new BulkActionRequestInputDTO();
        bulkActionRequestInputDTO.setAction(SuspensionActionType.STOP);
        List<String> entityUuids = new ArrayList<String>();
        entityUuids.add("1234");
        bulkActionRequestInputDTO.setEntityUuids(entityUuids);

        when(toggleService.toggle(any())).thenReturn(SuspensionToggle.SuspensionToggleEntityResponse.newBuilder().build());
        List<BulkActionRequestApiDTO> resp;

        // clear the security context
        SecurityContextHolder.getContext().setAuthentication(null);
        // set a test grpc context
        Context testContext = Context.current().withValue(SecurityConstant.USER_ID_CTX_KEY, "USER")
                .withValue(SecurityConstant.USER_UUID_KEY, "1")
                .withValue(SecurityConstant.USER_ROLES_KEY, ImmutableList.of("ADMIN", "TEST"));
        Context previous = testContext.attach();
        try {
            resp = suspensionService.bulkAction(bulkActionRequestInputDTO);
        } catch ( Exception e) {
            TestCase.fail("OperationFailedException should not be thrown");
            return;
        }
        List<BulkActionRequestApiDTO> expected = new ArrayList<BulkActionRequestApiDTO>();
        assertEquals(expected, resp);
        testContext.detach(previous);
    }

    /**
     * verifies the BulkAction method call.
     */
    @Test
    public void testBulkActionThrowException() throws Exception {


        when(toggleService.toggleError(any())).thenReturn(Optional.of(Status.NOT_FOUND
                .withDescription("test").asException()));
        BulkActionRequestInputDTO bulkActionRequestInputDTO = new BulkActionRequestInputDTO();
        bulkActionRequestInputDTO.setAction(SuspensionActionType.STOP);
        List<String> entityUuids = new ArrayList<String>();
        entityUuids.add("1234");
        bulkActionRequestInputDTO.setEntityUuids(entityUuids);
        // clear the security context
        SecurityContextHolder.getContext().setAuthentication(null);
        // set a test grpc context
        Context testContext = Context.current().withValue(SecurityConstant.USER_ID_CTX_KEY, "USER")
                .withValue(SecurityConstant.USER_UUID_KEY, "1")
                .withValue(SecurityConstant.USER_ROLES_KEY, ImmutableList.of("ADMIN", "TEST"));
        Context previous = testContext.attach();
        try {
            suspensionService.bulkAction(bulkActionRequestInputDTO);
            TestCase.fail("OperationFailedException should be thrown");
        } catch ( Exception e) {
            assertTrue(e.getMessage().contains("Retrieval of toggle entities failed"));
        }
        testContext.detach(previous);
    }

    /**
     * verifies the GetEntities method call.
     */
    @Test
    public void testGetEntities() throws Exception {
        when(userSessionContext.isUserScoped()).thenReturn(false);
        when(entityService.list(any())).thenReturn(SuspensionEntityOuterClass.SuspensionEntityResponse.newBuilder().build());
        SuspendableEntityInputDTO entityInputDTO = new SuspendableEntityInputDTO();
        SuspensionEntitiesPaginationRequest paginationRequest = new SuspensionEntitiesPaginationRequest();
        try {
            suspensionService.getEntities(entityInputDTO, paginationRequest);
        } catch ( Exception e) {
            TestCase.fail("OperationFailedException should not be thrown");
            return;
        }
    }

    /**
     * verifies the GetEntities method call with empty pagination request.
     */
    @Test
    public void testGetEntitiesEmptyPage() throws Exception {
        when(userSessionContext.isUserScoped()).thenReturn(false);
        when(entityService.list(any())).thenReturn(SuspensionEntityOuterClass.SuspensionEntityResponse.newBuilder().build());
        SuspendableEntityInputDTO entityInputDTO = new SuspendableEntityInputDTO();
        try {
            suspensionService.getEntities(entityInputDTO, null);
        } catch ( Exception e) {
            TestCase.fail("OperationFailedException should not be thrown");
            return;
        }
    }

    /**
     * verifies the GetEntities method call with ResolvedScopeError.
     */
    @Test
    public void testGetEntitiesResolvedScopeError() throws Exception {
        when(userSessionContext.isUserScoped()).thenReturn(false);
        when(entityService.listError(any())).thenReturn(Optional.of(Status.NOT_FOUND
                .withDescription("unknown scopes").asException()));
        SuspendableEntityInputDTO entityInputDTO = new SuspendableEntityInputDTO();
        try {
            suspensionService.getEntities(entityInputDTO, null);
            TestCase.fail("OperationFailedException should have been thrown");
        } catch ( Exception e) {
            return;
        }
    }

    /**
     * verifies the GetEntities method call with non ResolvedScopeError.
     */
    @Test
    public void testGetEntitiesError() throws Exception {
        when(userSessionContext.isUserScoped()).thenReturn(false);
        when(entityService.listError(any())).thenReturn(Optional.of(Status.NOT_FOUND
                .withDescription("test").asException()));
        SuspendableEntityInputDTO entityInputDTO = new SuspendableEntityInputDTO();
        try {
            suspensionService.getEntities(entityInputDTO, null);
            TestCase.fail("OperationFailedException should have been thrown");
        } catch ( Exception e) {
            return;
        }
    }
}