package com.vmturbo.topology.processor.entity;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;

import io.grpc.StatusRuntimeException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.common.protobuf.group.EntityCustomTagsOuterClass.EntityCustomTags;
import com.vmturbo.common.protobuf.group.EntityCustomTagsOuterClass.GetAllEntityCustomTagsRequest;
import com.vmturbo.common.protobuf.group.EntityCustomTagsOuterClass.GetAllEntityCustomTagsResponse;
import com.vmturbo.common.protobuf.group.EntityCustomTagsServiceGrpc.EntityCustomTagsServiceBlockingStub;
import com.vmturbo.common.protobuf.tag.Tag.TagValuesDTO;
import com.vmturbo.common.protobuf.tag.Tag.Tags;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.stitching.TopologyEntity.Builder;

/**
 * The {@link com.vmturbo.topology.processor.entity.EntityCustomTagsMerger} is used for merging the
 * user defined tags with the discovered tags, and inject them into the topology.
 */
public class EntityCustomTagsMerger {

    private static final Logger logger = LogManager.getLogger();

    private final EntityCustomTagsServiceBlockingStub customTagsService;

    /**
     * Construct an {@link com.vmturbo.topology.processor.entity.EntityCustomTagsMerger} which
     * is used to upload user defined tags for entities.
     *
     * @param customTagsService service for retrieving the user defined tags for entities.
     */
    EntityCustomTagsMerger(@Nonnull final EntityCustomTagsServiceBlockingStub customTagsService) {
        this.customTagsService = Objects.requireNonNull(customTagsService);
    }

    /**
     * This method is used to upload user defined tags to entities.
     *
     * @param topologyMap is the topology map builder, upon which will update the entity tags.
     * @throws OperationFailedException if RPC call fails.
     */
    public void mergeEntityCustomTags(@Nonnull final Map<Long, Builder> topologyMap) throws
            OperationFailedException {

        GetAllEntityCustomTagsResponse response;
        try {
            response = customTagsService.getAllTags(GetAllEntityCustomTagsRequest.newBuilder().build());
        } catch (StatusRuntimeException e) {
            String msg = "Entity custom tags service RPC call failed to complete request: "
                    + e.getMessage();
            logger.error(msg);
            throw new OperationFailedException(msg, e);
        }

        for (EntityCustomTags entry : response.getEntityCustomTagsList()) {
            Builder entity =  topologyMap.get(entry.getEntityId());

            if (entity == null) {
                logger.warn("There is no entity in the topology map with oid: "
                        + entry.getEntityId());
                continue;
            }
            TopologyEntityDTO.Builder entityBuilder = entity.getEntityBuilder();

            Tags tags = merge(entityBuilder.getTags(), entry.getTags());
            entityBuilder.setTags(tags);
        }
    }

    /**
     * Merges tags t1 and t2 as follows: if two tags has the same key, then it's values will be
     * unified as a set of unique values.
     *
     * @param t1 the tags to merge
     * @param t2 the tags to merge with t1
     *
     * @return the merged tags
     */
    private Tags merge(Tags t1, Tags t2) {
        Tags.Builder result = Tags.newBuilder();

        result.putAllTags(t1.getTagsMap());
        for (Map.Entry<String, TagValuesDTO> entry : t2.getTagsMap().entrySet()) {
            TagValuesDTO currentValues = result.getTagsMap().get(entry.getKey());
            Set<String> newValuesSet = new HashSet<>(entry.getValue().getValuesList());

            if (currentValues != null) {
                newValuesSet.addAll(currentValues.getValuesList());
            }

            result.putTags(entry.getKey(),
                    TagValuesDTO.newBuilder().addAllValues(newValuesSet).build()
            ).build();
        }

        return result.build();
    }
}