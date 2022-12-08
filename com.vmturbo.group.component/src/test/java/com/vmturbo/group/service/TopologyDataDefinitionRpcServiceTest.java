package com.vmturbo.group.service;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;

import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.CreateTopologyDataDefinitionRequest;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.CreateTopologyDataDefinitionResponse;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.DeleteTopologyDataDefinitionResponse;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.GetTopologyDataDefinitionResponse;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.GetTopologyDataDefinitionsRequest;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinition;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinitionEntry;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinitionID;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.UpdateTopologyDataDefinitionRequest;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.UpdateTopologyDataDefinitionResponse;
import com.vmturbo.components.api.test.GrpcExceptionMatcher;
import com.vmturbo.group.topologydatadefinition.TopologyDataDefinitionStore;
import com.vmturbo.group.topologydatadefinition.TopologyDataDefinitionTestUtils;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Tests for the TopologyDataDefinitionRpcService.
 */
public class TopologyDataDefinitionRpcServiceTest {

    private TopologyDataDefinitionRpcService topologyDataDefinitionRpcService;

    private TopologyDataDefinitionStore topologyDataDefinitionStore =
        mock(TopologyDataDefinitionStore.class);

    private static final String NAME = "EntityName";

    private static final long GROUP_ID = 111L;

    private static final long GROUP_ID2 = 123L;


    private final TopologyDataDefinition emptyDefinition =
        TopologyDataDefinitionTestUtils.createManualTopologyDataDefinition(EntityType.SERVICE, NAME,
            true, GROUP_ID, EntityType.VIRTUAL_MACHINE);

    private static final long OID = 1001L;

    private static final long OID2 = 1002L;

    private static final long MISSING_OID = 1003L;

    private final TopologyDataDefinitionEntry emptyDefinitionEntry =
        TopologyDataDefinitionEntry.newBuilder()
            .setId(OID)
            .setDefinition(emptyDefinition)
            .build();

    /**
     * Create the TopologyDataDefinitionRpcService.
     */
    @Before
    public void setup() {
        topologyDataDefinitionRpcService =
            new TopologyDataDefinitionRpcService(topologyDataDefinitionStore);
    }

    /**
     * Test create.
     *
     * @throws Exception when topologyDataDefinitionStore throws one.
     */
    @Test
    public void testCreate() throws Exception {
        when(topologyDataDefinitionStore.createTopologyDataDefinition(emptyDefinition))
            .thenReturn(emptyDefinitionEntry);
        final StreamObserver<CreateTopologyDataDefinitionResponse>
            mockCreateTopologyDataDefResponseObserver = Mockito.mock(StreamObserver.class);
        final CreateTopologyDataDefinitionResponse response =
            CreateTopologyDataDefinitionResponse.newBuilder()
                .setTopologyDataDefinition(emptyDefinitionEntry)
                .build();
        topologyDataDefinitionRpcService.createTopologyDataDefinition(CreateTopologyDataDefinitionRequest.newBuilder()
            .setTopologyDataDefinition(emptyDefinition)
            .build(), mockCreateTopologyDataDefResponseObserver);
        verify(mockCreateTopologyDataDefResponseObserver).onNext(response);
        verify(mockCreateTopologyDataDefResponseObserver).onCompleted();
    }

    /**
     * Test that when TopologyDataDefinitionStore throws a DuplicateNameException then the GRPC
     * service returns an exception.
     *
     * @throws Exception if TopologyDataDefinitionStore throws something besides
     * DuplicateNameException.
     */
    @Test
    public void testCreateDuplicateGetsException() throws Exception {
        final ArgumentCaptor<StatusException> exceptionCaptor =
            ArgumentCaptor.forClass(StatusException.class);
        final String errMessage = "Foo";
        when(topologyDataDefinitionStore.createTopologyDataDefinition(emptyDefinition))
            .thenThrow(new StoreOperationException(Status.ALREADY_EXISTS, errMessage));
        final StreamObserver<CreateTopologyDataDefinitionResponse>
            mockCreateTopologyDataDefResponseObserver = Mockito.mock(StreamObserver.class);
        topologyDataDefinitionRpcService.createTopologyDataDefinition(CreateTopologyDataDefinitionRequest.newBuilder()
            .setTopologyDataDefinition(emptyDefinition)
            .build(), mockCreateTopologyDataDefResponseObserver);
        Mockito.verify(mockCreateTopologyDataDefResponseObserver).onError(exceptionCaptor.capture());
        final StatusException exception = exceptionCaptor.getValue();
        assertThat(exception, GrpcExceptionMatcher.hasCode(Code.ALREADY_EXISTS)
            .descriptionContains(errMessage));
    }

    /**
     * Test get.
     */
    @Test
    public void testGetTopologyDataDefinition() {
        when(topologyDataDefinitionStore.getTopologyDataDefinition(OID))
            .thenReturn(Optional.of(emptyDefinition));
        final StreamObserver<GetTopologyDataDefinitionResponse>
            mockGetTopologyDataDefResponseObserver = Mockito.mock(StreamObserver.class);
        final GetTopologyDataDefinitionResponse response =
            GetTopologyDataDefinitionResponse.newBuilder()
                .setTopologyDataDefinition(emptyDefinitionEntry)
                .build();
        topologyDataDefinitionRpcService.getTopologyDataDefinition(TopologyDataDefinitionID
            .newBuilder()
            .setId(OID)
            .build(), mockGetTopologyDataDefResponseObserver);
        verify(mockGetTopologyDataDefResponseObserver).onNext(response);
        verify(mockGetTopologyDataDefResponseObserver).onCompleted();
    }

    /**
     * Test get.
     */
    @Test
    public void testGetNoTopologyDataDefinition() {
        final ArgumentCaptor<StatusException> exceptionCaptor =
            ArgumentCaptor.forClass(StatusException.class);
        final long missingOid = 1002L;
        when(topologyDataDefinitionStore.getTopologyDataDefinition(missingOid))
            .thenReturn(Optional.empty());
        final StreamObserver<GetTopologyDataDefinitionResponse>
            mockGetTopologyDataDefResponseObserver = Mockito.mock(StreamObserver.class);
        topologyDataDefinitionRpcService.getTopologyDataDefinition(TopologyDataDefinitionID.newBuilder()
            .setId(missingOid)
            .build(), mockGetTopologyDataDefResponseObserver);
        Mockito.verify(mockGetTopologyDataDefResponseObserver).onError(exceptionCaptor.capture());
        final StatusException exception = exceptionCaptor.getValue();
        assertThat(exception, GrpcExceptionMatcher.hasCode(Code.NOT_FOUND)
            .descriptionContains("No TopologyDataDefinition exists with ID " + missingOid));
    }

    /**
     * Test getAll.
     */
    @Test
    public void testGetAllTopologyDataDefinitions() {
        final ArgumentCaptor<GetTopologyDataDefinitionResponse> getResponseCaptor =
            ArgumentCaptor.forClass(GetTopologyDataDefinitionResponse.class);
        final TopologyDataDefinition definition2 =
            TopologyDataDefinitionTestUtils.createManualTopologyDataDefinition(
                EntityType.BUSINESS_TRANSACTION, "Bar", true, GROUP_ID, EntityType.VIRTUAL_MACHINE);
        final TopologyDataDefinitionEntry entry2 = TopologyDataDefinitionEntry.newBuilder()
            .setDefinition(definition2)
            .setId(OID2)
            .build();
        when(topologyDataDefinitionStore.getAllTopologyDataDefinitions())
            .thenReturn(Lists.newArrayList(emptyDefinitionEntry, entry2));
        final StreamObserver<GetTopologyDataDefinitionResponse>
            mockGetTopologyDataDefResponseObserver = Mockito.mock(StreamObserver.class);
        topologyDataDefinitionRpcService
            .getAllTopologyDataDefinitions(GetTopologyDataDefinitionsRequest.getDefaultInstance(),
                mockGetTopologyDataDefResponseObserver);
        verify(mockGetTopologyDataDefResponseObserver, times(2))
            .onNext(getResponseCaptor.capture());
        verify(mockGetTopologyDataDefResponseObserver).onCompleted();
        assertThat(getResponseCaptor.getAllValues().stream()
                .map(GetTopologyDataDefinitionResponse::getTopologyDataDefinition)
                .collect(Collectors.toList()),
            containsInAnyOrder(emptyDefinitionEntry, entry2));
    }

    /**
     * Test update.
     *
     * @throws Exception when topologyDataDefinitionStore does.
     */
    @Test
    public void testUpdateTopologyDataDefinition() throws Exception {
        final TopologyDataDefinition updatedDef = TopologyDataDefinitionTestUtils
            .createManualTopologyDataDefinition(EntityType.SERVICE, NAME, true, GROUP_ID2,
                    EntityType.APPLICATION_COMPONENT);
        final TopologyDataDefinitionEntry updatedDefEntry = TopologyDataDefinitionEntry.newBuilder()
            .setId(OID)
            .setDefinition(updatedDef)
            .build();
        final StreamObserver<UpdateTopologyDataDefinitionResponse>
            mockUpdateTopologyDataDefResponseObserver = Mockito.mock(StreamObserver.class);
        final UpdateTopologyDataDefinitionResponse response = UpdateTopologyDataDefinitionResponse
            .newBuilder()
            .setUpdatedTopologyDataDefinition(updatedDefEntry)
            .build();

        when(topologyDataDefinitionStore.updateTopologyDataDefinition(OID, updatedDef))
            .thenReturn(Optional.of(updatedDef));
        final UpdateTopologyDataDefinitionRequest request = UpdateTopologyDataDefinitionRequest
            .newBuilder()
            .setId(OID)
            .setTopologyDataDefinition(updatedDef)
            .build();
        topologyDataDefinitionRpcService.updateTopologyDataDefinition(request,
            mockUpdateTopologyDataDefResponseObserver);
        verify(mockUpdateTopologyDataDefResponseObserver).onNext(response);
        verify(mockUpdateTopologyDataDefResponseObserver).onCompleted();
    }

    /**
     * Test an update where the OID does not match any existing definitions.
     *
     * @throws Exception when topologyDataDefinitionStore does.
     */
    @Test
    public void testFailedUpdateBadOid() throws Exception {
        final ArgumentCaptor<StatusException> exceptionCaptor =
            ArgumentCaptor.forClass(StatusException.class);
        when(topologyDataDefinitionStore.updateTopologyDataDefinition(MISSING_OID, emptyDefinition))
            .thenReturn(Optional.empty());
        final StreamObserver<UpdateTopologyDataDefinitionResponse>
            mockUpdateTopologyDataDefResponseObserver = Mockito.mock(StreamObserver.class);
        final UpdateTopologyDataDefinitionRequest request = UpdateTopologyDataDefinitionRequest
            .newBuilder()
            .setId(MISSING_OID)
            .setTopologyDataDefinition(emptyDefinition)
            .build();
        topologyDataDefinitionRpcService.updateTopologyDataDefinition(request,
            mockUpdateTopologyDataDefResponseObserver);
        Mockito.verify(mockUpdateTopologyDataDefResponseObserver).onError(exceptionCaptor.capture());
        final StatusException exception = exceptionCaptor.getValue();
        assertThat(exception, GrpcExceptionMatcher.hasCode(Code.NOT_FOUND)
            .descriptionContains("No topology data definition found with ID " + MISSING_OID));
    }

    /**
     * Test an update where the request fails to specify a new TopologyDataDefinition.
     */
    @Test
    public void testFailedUpdateMissingDefinition() {
        final ArgumentCaptor<StatusException> exceptionCaptor =
            ArgumentCaptor.forClass(StatusException.class);
        final StreamObserver<UpdateTopologyDataDefinitionResponse>
            mockUpdateTopologyDataDefResponseObserver = Mockito.mock(StreamObserver.class);
        final UpdateTopologyDataDefinitionRequest request = UpdateTopologyDataDefinitionRequest
            .newBuilder()
            .setId(OID)
            .build();
        topologyDataDefinitionRpcService.updateTopologyDataDefinition(request,
            mockUpdateTopologyDataDefResponseObserver);
        Mockito.verify(mockUpdateTopologyDataDefResponseObserver).onError(exceptionCaptor.capture());
        final StatusException exception = exceptionCaptor.getValue();
        assertThat(exception, GrpcExceptionMatcher.hasCode(Code.INVALID_ARGUMENT)
            .descriptionContains("Invalid new topology data definition for update:"
                + " No topology data definition was provided"));
    }

    /**
     * Test an update where the request fails to specify an Id for the updated definition.
     */
    @Test
    public void testFailedUpdateMissingId() {
        final ArgumentCaptor<StatusException> exceptionCaptor =
            ArgumentCaptor.forClass(StatusException.class);
        final StreamObserver<UpdateTopologyDataDefinitionResponse>
            mockUpdateTopologyDataDefResponseObserver = Mockito.mock(StreamObserver.class);
        final UpdateTopologyDataDefinitionRequest request = UpdateTopologyDataDefinitionRequest
            .newBuilder()
            .setTopologyDataDefinition(emptyDefinition)
            .build();
        topologyDataDefinitionRpcService.updateTopologyDataDefinition(request,
            mockUpdateTopologyDataDefResponseObserver);
        Mockito.verify(mockUpdateTopologyDataDefResponseObserver).onError(exceptionCaptor.capture());
        final StatusException exception = exceptionCaptor.getValue();
        assertThat(exception, GrpcExceptionMatcher.hasCode(Code.INVALID_ARGUMENT)
            .descriptionContains("Invalid TopologyDataDefinitionID input for topology data definition"
                + " update: No TopologyDataDefinition ID specified"));
    }

    /**
     * Test 2 delete cases: when the definition to be deleted exists and when it does not.
     */
    @Test
    public void testDeleteTopologyDataDefinition() throws StoreOperationException {
        verifyDelete(true);
        verifyDelete(false);
    }

    /**
     * Helper function that checks if the deleteTopologyDataDefinition call works as expected.
     *
     * @param definitionExists flag indicating whether the definition we're deleting exists or not.
     */
    private void verifyDelete(boolean definitionExists) throws StoreOperationException {
        final StreamObserver<DeleteTopologyDataDefinitionResponse>
            mockDeleteTopologyDataDefResponseObserver = Mockito.mock(StreamObserver.class);
        final DeleteTopologyDataDefinitionResponse response = DeleteTopologyDataDefinitionResponse
            .newBuilder()
            .setDeleted(definitionExists)
            .build();

        when(topologyDataDefinitionStore.deleteTopologyDataDefinition(OID))
            .thenReturn(definitionExists);
        final TopologyDataDefinitionID request = TopologyDataDefinitionID
            .newBuilder()
            .setId(OID)
            .build();
        topologyDataDefinitionRpcService.deleteTopologyDataDefinition(request,
            mockDeleteTopologyDataDefResponseObserver);
        verify(mockDeleteTopologyDataDefResponseObserver).onNext(response);
        verify(mockDeleteTopologyDataDefResponseObserver).onCompleted();
    }

    /**
     * Test a delete where the request fails to specify an Id for the updated definition.
     */
    @Test
    public void testFailedDeleteMissingId() {
        final ArgumentCaptor<StatusException> exceptionCaptor =
            ArgumentCaptor.forClass(StatusException.class);
        final StreamObserver<DeleteTopologyDataDefinitionResponse>
            mockDeleteTopologyDataDefResponseObserver = Mockito.mock(StreamObserver.class);
        final TopologyDataDefinitionID request = TopologyDataDefinitionID
            .newBuilder()
            .build();
        topologyDataDefinitionRpcService.deleteTopologyDataDefinition(request,
            mockDeleteTopologyDataDefResponseObserver);
        Mockito.verify(mockDeleteTopologyDataDefResponseObserver).onError(exceptionCaptor.capture());
        final StatusException exception = exceptionCaptor.getValue();
        assertThat(exception, GrpcExceptionMatcher.hasCode(Code.INVALID_ARGUMENT)
            .descriptionContains("Invalid TopologyDataDefinitionID input for topology data definition"
                + " delete: No TopologyDataDefinition ID specified"));
    }
}
