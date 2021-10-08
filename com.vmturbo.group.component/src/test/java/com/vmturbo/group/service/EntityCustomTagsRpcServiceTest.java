package com.vmturbo.group.service;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import io.grpc.stub.StreamObserver;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.vmturbo.common.protobuf.group.EntityCustomTagsOuterClass;
import com.vmturbo.common.protobuf.group.EntityCustomTagsOuterClass.EntityCustomTagsCreateResponse;
import com.vmturbo.common.protobuf.tag.Tag.TagValuesDTO;
import com.vmturbo.common.protobuf.tag.Tag.Tags;
import com.vmturbo.group.entitytags.EntityCustomTagsStore;

/**
 * This class tests {@link EntityCustomTagsRpcService}.
 */
@RunWith(MockitoJUnitRunner.class)
public class EntityCustomTagsRpcServiceTest {

    @Mock
    private EntityCustomTagsStore entityCustomTagsStore;

    private static final long ENTITY_ID = 107L;
    private static final String tagName1 = "tag1";
    private static final String tagValue1 = "value1";
    private static final String tagName2 = "tag2";
    private static final String tagValue2 = "value2";

    private static final Tags tags = Tags.newBuilder().putTags(tagName1, TagValuesDTO.newBuilder()
            .addAllValues(Arrays.asList(tagValue1, tagValue2))
            .build()).putTags(tagName2, TagValuesDTO.newBuilder()
            .addAllValues(Collections.singletonList(tagValue1))
            .build()).build();

    private EntityCustomTagsRpcService entityCustomTagsService;

    /**
     * Initializes the user defined tags rpc service.
     */
    @Before
    public void setup() {
        entityCustomTagsService = new EntityCustomTagsRpcService(entityCustomTagsStore);
    }

    /**
     * Tests the creation of tags using the gRPC service.
     *
     * @throws StoreOperationException should not happen.
     */
    @Test
    public void testCreate() throws StoreOperationException {

        final EntityCustomTagsOuterClass.EntityCustomTagsCreateRequest request =
                EntityCustomTagsOuterClass.EntityCustomTagsCreateRequest.newBuilder().setEntityId(
                        ENTITY_ID).setTags(tags).build();
        final StreamObserver<EntityCustomTagsOuterClass.EntityCustomTagsCreateResponse> mockObserver =
                Mockito.mock(StreamObserver.class);

        when(entityCustomTagsStore.insertTags(ENTITY_ID, tags)).thenReturn(3);

        entityCustomTagsService.createTags(request, mockObserver);

        verify(mockObserver).onNext(EntityCustomTagsCreateResponse.newBuilder().build());
        verify(mockObserver).onCompleted();
    }

    /**
     * Tests the creation of tags request without entity id. It should return an error matching
     * IllegalArgumentException.
     */
    @Test
    public void testCreateNoEntityId() {
        final EntityCustomTagsOuterClass.EntityCustomTagsCreateRequest request =
                EntityCustomTagsOuterClass.EntityCustomTagsCreateRequest.newBuilder().setTags(tags).build();

        final StreamObserver<EntityCustomTagsOuterClass.EntityCustomTagsCreateResponse> mockObserver =
                Mockito.mock(StreamObserver.class);

        entityCustomTagsService.createTags(request, mockObserver);

        Mockito.verify(mockObserver).onError(Matchers.any(IllegalArgumentException.class));
    }

    /**
     * Tests the creation of tags request without tags. It should return an error matching
     * IllegalArgumentException.
     */
    @Test
    public void testCreateNoTags() {

        final EntityCustomTagsOuterClass.EntityCustomTagsCreateRequest request =
                EntityCustomTagsOuterClass.EntityCustomTagsCreateRequest.newBuilder().setEntityId(ENTITY_ID).build();
        final StreamObserver<EntityCustomTagsOuterClass.EntityCustomTagsCreateResponse> mockObserver =
                Mockito.mock(StreamObserver.class);

        entityCustomTagsService.createTags(request, mockObserver);

        Mockito.verify(mockObserver).onError(Matchers.any(IllegalArgumentException.class));
    }

    /**
     * Tests the getting of tags by entity id using the gRPC service.
     */
    @Test
    public void testGetTags() {

        final EntityCustomTagsOuterClass.GetEntityCustomTagsRequest request =
                EntityCustomTagsOuterClass.GetEntityCustomTagsRequest.newBuilder().setEntityId(
                        ENTITY_ID).build();
        final StreamObserver<EntityCustomTagsOuterClass.GetEntityCustomTagsResponse> mockObserver =
                Mockito.mock(StreamObserver.class);

        when(entityCustomTagsStore.getTags(ENTITY_ID)).thenReturn(tags);

        entityCustomTagsService.getTags(request, mockObserver);

        verify(mockObserver).onNext(
                EntityCustomTagsOuterClass.GetEntityCustomTagsResponse.newBuilder()
                        .setTags(tags)
                        .build()
        );
        verify(mockObserver).onCompleted();
    }

    /**
     * Tests the getting of all tags using the gRPC service.
     */
    @Test
    public void testGetAllTags() {

        final EntityCustomTagsOuterClass.GetAllEntityCustomTagsRequest request =
                EntityCustomTagsOuterClass.GetAllEntityCustomTagsRequest.newBuilder().build();
        final StreamObserver<EntityCustomTagsOuterClass.GetAllEntityCustomTagsResponse> mockObserver =
                Mockito.mock(StreamObserver.class);

        List<EntityCustomTagsOuterClass.EntityCustomTags> list = new ArrayList<>();
        EntityCustomTagsOuterClass.EntityCustomTags tag =
                EntityCustomTagsOuterClass.EntityCustomTags.newBuilder()
                        .setEntityId(ENTITY_ID)
                        .setTags(tags)
                        .build();
        list.add(tag);

        when(entityCustomTagsStore.getAllTags()).thenReturn(list);
        entityCustomTagsService.getAllTags(request, mockObserver);

        verify(mockObserver).onNext(
                EntityCustomTagsOuterClass.GetAllEntityCustomTagsResponse.newBuilder()
                        .addEntityCustomTags(tag)
                        .build()
        );
        verify(mockObserver).onCompleted();
    }
}