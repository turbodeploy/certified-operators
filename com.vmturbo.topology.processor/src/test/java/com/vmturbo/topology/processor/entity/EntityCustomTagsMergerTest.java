package com.vmturbo.topology.processor.entity;

import static com.vmturbo.topology.processor.topology.TopologyEntityUtils.topologyEntityBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import io.grpc.StatusRuntimeException;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.common.protobuf.group.EntityCustomTagsMoles.EntityCustomTagsServiceMole;
import com.vmturbo.common.protobuf.group.EntityCustomTagsOuterClass.EntityCustomTags;
import com.vmturbo.common.protobuf.group.EntityCustomTagsOuterClass.GetAllEntityCustomTagsRequest;
import com.vmturbo.common.protobuf.group.EntityCustomTagsOuterClass.GetAllEntityCustomTagsResponse;
import com.vmturbo.common.protobuf.group.EntityCustomTagsServiceGrpc;
import com.vmturbo.common.protobuf.group.EntityCustomTagsServiceGrpc.EntityCustomTagsServiceBlockingStub;
import com.vmturbo.common.protobuf.tag.TagPOJO.TagValuesImpl;
import com.vmturbo.common.protobuf.tag.TagPOJO.TagValuesView;
import com.vmturbo.common.protobuf.tag.TagPOJO.TagsImpl;
import com.vmturbo.common.protobuf.tag.TagPOJO.TagsView;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;

/**
 * Unit test for {@link EntityCustomTagsMerger}.
 */
public class EntityCustomTagsMergerTest {

    private EntityCustomTagsMerger recorderSpy;

    private EntityCustomTagsServiceBlockingStub customTagsService;

    private final EntityCustomTagsServiceMole customTagsServiceMole =
            spy(new EntityCustomTagsServiceMole());

    private final long entityID = 42L;

    private final String discoveredKey = "discovered_key";

    private final String discoveredValueStr = "discovered_value";

    private final TagValuesImpl discoveredValue = new TagValuesImpl()
            .addValues(discoveredValueStr);

    private TagsImpl discoveredTags = new TagsImpl()
            .putTags(discoveredKey, discoveredValue);

    private TopologyEntityDTO.Builder hostInDatacenter = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
            .setDisplayName("ENTITY")
            .setOid(entityID)
            .setTags(discoveredTags.toProto());

    private TopologyEntityDTO.Builder datacenter = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.DATACENTER_VALUE)
            .setDisplayName("DATACENTER")
            .setOid(100L);

    private TopologyEntityDTO.Builder storage = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.STORAGE_VALUE)
            .setDisplayName("STORAGE")
            .setOid(101L);

    // create a topology for those entities
    private Map<Long, TopologyEntity.Builder> topologyMap = ImmutableMap.of(
            datacenter.getOid(), topologyEntityBuilder(datacenter),
            hostInDatacenter.getOid(), topologyEntityBuilder(hostInDatacenter),
            storage.getOid(), topologyEntityBuilder(storage)
    );

    /**
     * Grpc server mock.
     */
    @Rule
    public GrpcTestServer server = GrpcTestServer.newServer(customTagsServiceMole);

    /**
     * Basic mock initialisations.
     * @throws Exception should not happen.
     */
    @Before
    public void setup() throws Exception {
        customTagsService = EntityCustomTagsServiceGrpc.newBlockingStub(server.getChannel());
        recorderSpy = spy(new EntityCustomTagsMerger(customTagsService));
    }

    /**
     * Test the case of merging one tag, into a map with an already existing tag, with different key.
     *
     * @throws OperationFailedException should not happen.
     */
    @Test
    public void testMergeEntityCustomTags() throws OperationFailedException {

        final String customKey = "custom_key";
        final String customValueStr = "custom_value";
        final TagValuesImpl customValue = new TagValuesImpl().addValues(customValueStr);
        EntityCustomTags customTags = EntityCustomTags.newBuilder()
            .setTags(new TagsImpl().putTags(customKey, customValue).toProto())
            .setEntityId(42L).build();

        List<EntityCustomTags> customTagsList = new ArrayList<>();
        customTagsList.add(customTags);

        GetAllEntityCustomTagsRequest request =
                GetAllEntityCustomTagsRequest.newBuilder().build();
        GetAllEntityCustomTagsResponse response =
                GetAllEntityCustomTagsResponse.newBuilder().addEntityCustomTags(customTags).build();

        doReturn(response).when(customTagsServiceMole).getAllTags(request);
        recorderSpy.mergeEntityCustomTags(topologyMap);

        TagsView finalTags = topologyMap.get(entityID).getTopologyEntityImpl().getTags();
        TagsImpl expectedTags = new TagsImpl()
                .putTags(customKey, customValue)
                .putTags(discoveredKey, discoveredValue);
        assertThat(finalTags, is(expectedTags));
    }

    /**
     * Test the case of merging one tag, into a map with an already existing tag, with same key.
     *
     * @throws OperationFailedException should not happen.
     */
    @Test
    public void testMergeTagsWithSameKey() throws OperationFailedException {

        final String customValueStr = "custom_value";
        final TagValuesImpl customValue = new TagValuesImpl().addValues(customValueStr);
        EntityCustomTags customTags = EntityCustomTags.newBuilder()
            .setTags(new TagsImpl().putTags(discoveredKey, customValue).toProto())
            .setEntityId(42L).build();

        List<EntityCustomTags> customTagsList = new ArrayList<>();
        customTagsList.add(customTags);

        GetAllEntityCustomTagsRequest request =
                GetAllEntityCustomTagsRequest.newBuilder().build();
        GetAllEntityCustomTagsResponse response =
                GetAllEntityCustomTagsResponse.newBuilder().addEntityCustomTags(customTags).build();

        doReturn(response).when(customTagsServiceMole).getAllTags(request);
        recorderSpy.mergeEntityCustomTags(topologyMap);

        TagsView finalTags = topologyMap.get(entityID).getTopologyEntityImpl().getTags();

        TagValuesView finalTagValues = finalTags.getTagsMap().get(discoveredKey);
        assertThat(finalTagValues, is(notNullValue()));

        List<String> finalValuesList = finalTagValues.getValuesList();
        assertThat(finalValuesList, containsInAnyOrder(discoveredValueStr, customValueStr));
    }

    /**
     * Test the case of failing to retrieve data from database.
     *
     * @throws OperationFailedException because of RPC call failure.
     */
    @Test(expected = OperationFailedException.class)
    public void testMergeTagsRPCFail() throws OperationFailedException {

        final String customValueStr = "custom_value";
        final TagValuesImpl customValue = new TagValuesImpl().addValues(customValueStr);
        EntityCustomTags customTags = EntityCustomTags.newBuilder()
            .setTags(new TagsImpl().putTags(discoveredKey, customValue).toProto())
            .setEntityId(entityID).build();

        List<EntityCustomTags> customTagsList = new ArrayList<>();
        customTagsList.add(customTags);

        GetAllEntityCustomTagsRequest request =
                GetAllEntityCustomTagsRequest.newBuilder().build();

        doThrow(StatusRuntimeException.class).when(customTagsServiceMole).getAllTags(request);
        recorderSpy.mergeEntityCustomTags(topologyMap);
    }

    /**
     * Test the case of merging tags for an entity that does not exist. The custom tag should be
     * ignored.
     *
     * @throws OperationFailedException should not happen.
     */
    @Test
    public void testMergeTagsMissingEntity() throws OperationFailedException {

        final String customKey = "custom_key";
        final String customValueStr = "custom_value";
        final TagValuesImpl customValue = new TagValuesImpl().addValues(customValueStr);
        EntityCustomTags customTags = EntityCustomTags.newBuilder()
            .setTags(new TagsImpl().putTags(customKey, customValue).toProto())
            .setEntityId(entityID + 1).build();

        List<EntityCustomTags> customTagsList = new ArrayList<>();
        customTagsList.add(customTags);

        GetAllEntityCustomTagsRequest request =
                GetAllEntityCustomTagsRequest.newBuilder().build();
        GetAllEntityCustomTagsResponse response =
                GetAllEntityCustomTagsResponse.newBuilder().addEntityCustomTags(customTags).build();

        doReturn(response).when(customTagsServiceMole).getAllTags(request);
        recorderSpy.mergeEntityCustomTags(topologyMap);

        assertThat(topologyMap.get(entityID + 1), is(nullValue()));

        // entityID tags should remain unchanged
        TagsView finalTags = topologyMap.get(entityID).getTopologyEntityImpl().getTags();
        TagsImpl expectedTags = new TagsImpl()
                .putTags(discoveredKey, discoveredValue);
        assertThat(finalTags, is(expectedTags));
    }
}
