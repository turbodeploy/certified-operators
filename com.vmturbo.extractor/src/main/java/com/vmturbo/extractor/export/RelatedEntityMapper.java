package com.vmturbo.extractor.export;

import java.util.Collections;
import java.util.Map;

import javax.annotation.Nonnull;

import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.extractor.schema.json.export.RelatedEntity;
import com.vmturbo.extractor.topology.SupplyChainEntity;

/**
 * Converts topology objects to {@link RelatedEntity} objects for extraction.
 * Caches the {@link RelatedEntity} objects to conserve memory.
 */
public class RelatedEntityMapper {

    // cache of relatedEntity by id, so it can be reused to reduce memory footprint
    private final Map<Long, RelatedEntity> relatedEntityById =
            Collections.synchronizedMap(new Long2ObjectOpenHashMap<>());

    /**
     * Create a related entity object out of an entity.
     *
     * @param supplyChainEntity The {@link SupplyChainEntity}.
     * @return The {@link RelatedEntity}.
     */
    @Nonnull
    public RelatedEntity createRelatedEntity(@Nonnull final SupplyChainEntity supplyChainEntity) {
        return getOrCreateRelatedEntity(supplyChainEntity.getOid(), supplyChainEntity.getDisplayName());
    }

    /**
     * Create a related entity object out of a group.
     *
     * @param group The group.
     * @return The {@link RelatedEntity}.
     */
    @Nonnull
    public RelatedEntity createRelatedEntity(@Nonnull final Grouping group) {
        return getOrCreateRelatedEntity(group.getId(), group.getDefinition().getDisplayName());
    }

    /**
     * Get or create a new instance of {@link RelatedEntity}.
     *
     * @param id id of the entity
     * @param displayName Supplier for the entity if it doesn't exist.
     * @return instance of {@link RelatedEntity}
     */
    @Nonnull
    private RelatedEntity getOrCreateRelatedEntity(final long id, final String displayName) {
        return relatedEntityById.computeIfAbsent(id, k -> {
            RelatedEntity relatedEntity = new RelatedEntity();
            relatedEntity.setName(displayName);
            relatedEntity.setOid(id);
            return relatedEntity;
        });
    }
}
