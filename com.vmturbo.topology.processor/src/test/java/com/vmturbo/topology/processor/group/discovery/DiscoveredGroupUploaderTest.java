package com.vmturbo.topology.processor.group.discovery;

import static com.vmturbo.topology.processor.group.discovery.DiscoveredGroupConstants.PLACEHOLDER_CLUSTER_INFO;
import static com.vmturbo.topology.processor.group.discovery.DiscoveredGroupConstants.PLACEHOLDER_GROUP_INFO;
import static com.vmturbo.topology.processor.group.discovery.DiscoveredGroupConstants.PLACEHOLDER_INTERPRETED_CLUSTER;
import static com.vmturbo.topology.processor.group.discovery.DiscoveredGroupConstants.PLACEHOLDER_INTERPRETED_GROUP;
import static com.vmturbo.topology.processor.group.discovery.DiscoveredGroupConstants.SELECTION_DTO;
import static com.vmturbo.topology.processor.group.discovery.DiscoveredGroupConstants.STATIC_MEMBER_DTO;
import static com.vmturbo.topology.processor.group.discovery.DiscoveredGroupConstants.TARGET_ID;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.group.DiscoveredCollectionsServiceGrpc.DiscoveredCollectionsServiceImplBase;
import com.vmturbo.common.protobuf.group.GroupDTO.StoreDiscoveredCollectionsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.StoreDiscoveredCollectionsResponse;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.topology.processor.group.discovery.DiscoveredGroupInterpreter.InterpretedGroup;

public class DiscoveredGroupUploaderTest {

    private DiscoveredGroupUploader recorderSpy;

    private final TestDiscoveredService uploadServiceSpy = spy(new TestDiscoveredService());

    private final DiscoveredGroupInterpreter converter = mock(DiscoveredGroupInterpreter.class);

    private InterpretedGroup interpretedGroup = mock(InterpretedGroup.class);

    private Map<Long, List<InterpretedGroup>> queuedGroup =
                ImmutableMap.of(TARGET_ID, Collections.singletonList(interpretedGroup));

    @Rule
    public GrpcTestServer server = GrpcTestServer.newServer(uploadServiceSpy);

    @Before
    public void setup() throws Exception {
        recorderSpy = spy(new DiscoveredGroupUploader(server.getChannel(), converter));
        when(interpretedGroup.getDtoAsCluster()).thenReturn(Optional.empty());
        when(interpretedGroup.getDtoAsGroup()).thenReturn(Optional.empty());
    }

    @Test
    public void testQueueDiscoveredGroups() {
        assertTrue(recorderSpy.pollQueuedGroups().isEmpty());
        when(converter.interpretSdkGroupList(any(), eq(TARGET_ID)))
                .thenReturn(Collections.singletonList(PLACEHOLDER_INTERPRETED_GROUP));

        recorderSpy.queueDiscoveredGroups(TARGET_ID, Collections.singletonList(STATIC_MEMBER_DTO));

        Map<Long, List<InterpretedGroup>> groups = recorderSpy.pollQueuedGroups();
        // Any NPE's in this chain of gets means the data structure didn't get filled in properly.
        assertEquals(PLACEHOLDER_INTERPRETED_GROUP, groups.get(TARGET_ID).get(0));
    }

    @Test
    public void testDiscoveredGroupsQueueGetEmptied() {
        assertTrue(recorderSpy.pollQueuedGroups().isEmpty());
        when(converter.interpretSdkGroupList(any(), eq(TARGET_ID)))
                .thenReturn(Collections.singletonList(PLACEHOLDER_INTERPRETED_GROUP));

        recorderSpy.queueDiscoveredGroups(TARGET_ID, Collections.singletonList(STATIC_MEMBER_DTO));
        // The first call to pollQueuedGroups should return the groups we just added.
        assertFalse(recorderSpy.pollQueuedGroups().isEmpty());
        // The second call should return nothing.
        assertTrue(recorderSpy.pollQueuedGroups().isEmpty());
    }

    @Test
    public void testDiscoveredGroupRequeue() {
        when(converter.interpretSdkGroupList(eq(Collections.singletonList(STATIC_MEMBER_DTO)),
                eq(TARGET_ID))).thenReturn(Collections.singletonList(PLACEHOLDER_INTERPRETED_GROUP));
        assertTrue(recorderSpy.pollQueuedGroups().isEmpty());

        recorderSpy.queueDiscoveredGroups(TARGET_ID, Collections.singletonList(STATIC_MEMBER_DTO));

        Map<Long, List<InterpretedGroup>> groups = recorderSpy.pollQueuedGroups();
        assertTrue(recorderSpy.pollQueuedGroups().isEmpty());
        recorderSpy.requeueGroups(groups);
        assertEquals(groups, recorderSpy.pollQueuedGroups());
    }

    @Test
    public void testTargetRemoved() {
        when(converter.interpretSdkGroupList(eq(Collections.singletonList(STATIC_MEMBER_DTO)),
                eq(TARGET_ID))).thenReturn(Collections.singletonList(PLACEHOLDER_INTERPRETED_GROUP));
        assertTrue(recorderSpy.pollQueuedGroups().isEmpty());

        recorderSpy.targetRemoved(TARGET_ID);

        Map<Long, List<InterpretedGroup>> groups = recorderSpy.pollQueuedGroups();

        assertTrue(recorderSpy.pollQueuedGroups().isEmpty());
        assertThat(groups.size(), is(1));
        assertTrue(groups.containsKey(TARGET_ID));
        assertThat(groups.get(TARGET_ID).size(), is(0));
    }

    @Test
    public void testProcessGroupsPutback() throws Exception {
        when(recorderSpy.pollQueuedGroups()).thenReturn(queuedGroup);
        uploadServiceSpy.enableError();

        try {
            recorderSpy.processQueuedGroups();
            Assert.fail("Expected runtime exception.");
        } catch (RuntimeException e) {
            verify(recorderSpy).requeueGroups(eq(queuedGroup));
        }
    }

    @Test
    public void testProcessUnsuccessfulInterpretation() throws Exception {
        when(recorderSpy.pollQueuedGroups()).thenReturn(queuedGroup);
        recorderSpy.processQueuedGroups();

        verify(uploadServiceSpy).storeDiscoveredCollections(
                eq(StoreDiscoveredCollectionsRequest.newBuilder()
                        .setTargetId(TARGET_ID)
                        .build()), any());
    }

    @Test
    public void testProcessClusterSuccess() throws Exception {
        when(interpretedGroup.getDtoAsCluster()).thenReturn(Optional.of(PLACEHOLDER_CLUSTER_INFO));
        when(recorderSpy.pollQueuedGroups()).thenReturn(queuedGroup);
        recorderSpy.processQueuedGroups();

        verify(uploadServiceSpy).storeDiscoveredCollections(
            eq(StoreDiscoveredCollectionsRequest.newBuilder()
                .addDiscoveredCluster(PLACEHOLDER_CLUSTER_INFO)
                .setTargetId(TARGET_ID)
                .build()), any());
    }

    @Test
    public void testProcessGroupSuccess() throws Exception {
        when(interpretedGroup.getDtoAsGroup()).thenReturn(Optional.of(PLACEHOLDER_GROUP_INFO));
        when(recorderSpy.pollQueuedGroups()).thenReturn(queuedGroup);

        recorderSpy.processQueuedGroups();

        verify(uploadServiceSpy).storeDiscoveredCollections(
                eq(StoreDiscoveredCollectionsRequest.newBuilder()
                        .addDiscoveredGroup(PLACEHOLDER_GROUP_INFO)
                        .setTargetId(TARGET_ID)
                        .build()), any());
    }

    /**
     * Test that even if no groups/clusters got successfully converted we still
     * update the group definitions.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testProcessEmptySuccess() throws Exception {
        when(recorderSpy.pollQueuedGroups()).thenReturn(queuedGroup);
        when(converter.sdkToCluster(eq(STATIC_MEMBER_DTO), eq(TARGET_ID))).thenReturn(Optional.empty());
        when(converter.sdkToGroup(eq(STATIC_MEMBER_DTO), eq(TARGET_ID))).thenReturn(Optional.empty());

        recorderSpy.processQueuedGroups();

        verify(uploadServiceSpy).storeDiscoveredCollections(
                eq(StoreDiscoveredCollectionsRequest.newBuilder()
                        .setTargetId(TARGET_ID)
                        .build()), any());
    }

    @Test
    public void testDiscoveredGroupsPutbackNotOverwrite() {
        assertTrue(recorderSpy.pollQueuedGroups().isEmpty());
        when(converter.interpretSdkGroupList(
                eq(Collections.singletonList(STATIC_MEMBER_DTO)),
                eq(TARGET_ID)))
            .thenReturn(Collections.singletonList(PLACEHOLDER_INTERPRETED_GROUP));
        when(converter.interpretSdkGroupList(
                eq(Collections.singletonList(SELECTION_DTO)),
                eq(TARGET_ID)))
                .thenReturn(Collections.singletonList(PLACEHOLDER_INTERPRETED_CLUSTER));

        recorderSpy.queueDiscoveredGroups(TARGET_ID, Collections.singletonList(STATIC_MEMBER_DTO));

        Map<Long, List<InterpretedGroup>> groups = recorderSpy.pollQueuedGroups();
        assertTrue(recorderSpy.pollQueuedGroups().isEmpty());

        recorderSpy.queueDiscoveredGroups(TARGET_ID, Collections.singletonList(SELECTION_DTO));

        recorderSpy.requeueGroups(groups);

        // Verify that the conversion associated with SELECTION_DTO gets returned from the next
        // poll, not the conversion associated with STATIC_MEMBER_DTO.
        //
        // Any NPE's in this chain of gets means the data structure didn't get filled in properly.
        assertEquals(PLACEHOLDER_INTERPRETED_CLUSTER,
                recorderSpy.pollQueuedGroups().get(TARGET_ID).get(0));
    }


    public static class TestDiscoveredService extends DiscoveredCollectionsServiceImplBase {

        private boolean error = false;

        public void enableError() {
            error = true;
        }

        public void storeDiscoveredCollections(StoreDiscoveredCollectionsRequest request,
                   StreamObserver<StoreDiscoveredCollectionsResponse> responseObserver) {
            if (error) {
                responseObserver.onError(Status.INTERNAL.asException());
            } else {
                responseObserver.onNext(StoreDiscoveredCollectionsResponse.getDefaultInstance());
                responseObserver.onCompleted();
            }
        }
    }
}
