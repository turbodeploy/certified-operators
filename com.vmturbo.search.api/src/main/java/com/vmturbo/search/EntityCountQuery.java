package com.vmturbo.search;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import org.jooq.Condition;
import org.jooq.DSLContext;

import com.vmturbo.api.dto.searchquery.EntityCountRequestApiDTO;
import com.vmturbo.api.dto.searchquery.EntityCountRequestApiDTO.GroupByCriterion;
import com.vmturbo.api.dto.searchquery.FieldApiDTO;
import com.vmturbo.api.dto.searchquery.PrimitiveFieldApiDTO;
import com.vmturbo.extractor.schema.tables.SearchEntity;
import com.vmturbo.search.mappers.EntityTypeMapper;
import com.vmturbo.search.metadata.SearchEntityMetadata;
import com.vmturbo.search.metadata.SearchMetadataMapping;

/**
 * A representation of a single API entities count query, mapped to a SQL query.
 */
public class EntityCountQuery extends AbstractCountQuery {

    /**
     * Converts input (GroupByCriterion) into an API field.
     *
     * <p>Another option would be to just take the API field as input. We're going with the enum for
     * now, as a way to guide the user as to what's available/makes sense. However, we could open
     * this wide open in the future by replacing GroupByCriterion with FieldApiDTOs in the input.</p>
     */
    private static final Map<GroupByCriterion, FieldApiDTO> GROUP_BY_CRITERION_MAPPING =
        ImmutableMap.of(GroupByCriterion.ENTITY_TYPE, PrimitiveFieldApiDTO.entityType(),
            GroupByCriterion.ENVIRONMENT_TYPE, PrimitiveFieldApiDTO.environmentType());

    /**
     * Gets a list of all supported entity types.
     */
    private static final List<com.vmturbo.extractor.schema.enums.EntityType> ALL_SUPPORTED_ENTITY_TYPES =
        Arrays.stream(SearchEntityMetadata.values())
            .map(SearchEntityMetadata::getEntityType)
            .map(EntityTypeMapper::fromApiToSearchSchema)
            .collect(Collectors.toList());

    /**
     * The API request being mapped to a DB query.
     */
    private final EntityCountRequestApiDTO request;

    /**
     * Create a search query to retrieve counts of entities.
     *
     * @param request the API request being mapped to a DB query
     * @param readOnlyDSLContext a context for making read-only database queries
     */
    public EntityCountQuery(@Nonnull final EntityCountRequestApiDTO request,
                            @Nonnull final DSLContext readOnlyDSLContext) {
        super(Objects.requireNonNull(readOnlyDSLContext));
        this.request = Objects.requireNonNull(request);
    }

    @Override
    protected Map<FieldApiDTO, SearchMetadataMapping> lookupMetadataMapping() {
        return SearchEntityMetadata.getEntityCommonFieldsMappingMap();
    }

    @Override
    protected Condition buildWhereClause() {
        return SearchEntity.SEARCH_ENTITY.TYPE.in(ALL_SUPPORTED_ENTITY_TYPES);
    }

    @Override
    protected Stream<FieldApiDTO> getGroupByFields() {
        return request.getGroupBy().stream()
            .map(groupByCriterion -> GROUP_BY_CRITERION_MAPPING.get(groupByCriterion));
    }

}
