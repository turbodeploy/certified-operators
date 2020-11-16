package com.vmturbo.api.component.external.api.mapper;

import javax.annotation.Nullable;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;

import com.vmturbo.common.protobuf.common.Pagination.OrderBy.SearchOrderBy;
import com.vmturbo.extractor.schema.enums.EntityType;

/**
 * Utility for mapping between ENUMs {@link com.vmturbo.api.pagination.SearchOrderBy} and {@link SearchOrderBy}.
 */
public class SearchOrderByMapper {

    /**
     * Mappings between {@link com.vmturbo.api.pagination.SearchOrderBy} and {@link SearchOrderBy}.
     */
    protected static final BiMap<com.vmturbo.api.pagination.SearchOrderBy, SearchOrderBy> SEARCH_ORDER_BY_MAPPINGS =
            new ImmutableBiMap.Builder()
                    .put(com.vmturbo.api.pagination.SearchOrderBy.NAME, SearchOrderBy.ENTITY_NAME)
                    .put(com.vmturbo.api.pagination.SearchOrderBy.COST, SearchOrderBy.ENTITY_COST)
                    .put(com.vmturbo.api.pagination.SearchOrderBy.SEVERITY, SearchOrderBy.ENTITY_SEVERITY)
                    .put(com.vmturbo.api.pagination.SearchOrderBy.UTILIZATION, SearchOrderBy.ENTITY_UTILIZATION)
                    .build();

    /**
     * Private constructor, never initialized, pattern for a utility class.
     */
    private SearchOrderByMapper() {}

    /**
     * Get the {@link com.vmturbo.api.pagination.SearchOrderBy} associated with a {@link
     * SearchOrderBy}.
     *
     * @param searchOrderBy The {@link SearchOrderBy}.
     * @return The associated {@link com.vmturbo.api.pagination.SearchOrderBy}, or null
     */
    public static com.vmturbo.api.pagination.SearchOrderBy fromProtoToApiEnum(@Nullable final SearchOrderBy searchOrderBy) {
        return SEARCH_ORDER_BY_MAPPINGS.inverse().get(searchOrderBy);
    }

    /**
     * Get the {@link SearchOrderBy} associated with a {@link com.vmturbo.api.enums.EntityType}.
     *
     * @param searchOrderBy The {@link com.vmturbo.api.pagination.SearchOrderBy}.
     * @return The associated {@link EntityType}, or null.
     */
    public static SearchOrderBy fromApiToProtoEnum(@Nullable final com.vmturbo.api.pagination.SearchOrderBy searchOrderBy) {
        return SEARCH_ORDER_BY_MAPPINGS.get(searchOrderBy);
    }
}
