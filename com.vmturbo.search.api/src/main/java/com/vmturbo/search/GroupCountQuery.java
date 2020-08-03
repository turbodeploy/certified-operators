package com.vmturbo.search;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Condition;
import org.jooq.DSLContext;

import com.vmturbo.api.dto.searchquery.FieldApiDTO;
import com.vmturbo.api.dto.searchquery.GroupCountRequestApiDTO;
import com.vmturbo.api.dto.searchquery.GroupCountRequestApiDTO.GroupByCriterion;
import com.vmturbo.api.dto.searchquery.PrimitiveFieldApiDTO;
import com.vmturbo.extractor.schema.enums.EntityType;
import com.vmturbo.extractor.schema.tables.SearchEntity;
import com.vmturbo.search.mappers.GroupTypeMapper;
import com.vmturbo.search.metadata.SearchGroupMetadata;
import com.vmturbo.search.metadata.SearchMetadataMapping;

/**
 * A representation of a single API groups count query, mapped to a SQL query.
 */
public class GroupCountQuery extends AbstractCountQuery {

    private static final Logger logger = LogManager.getLogger();

    /**
     * Converts input (GroupByCriterion) into an API field.
     *
     * <p>Another option would be to just take the API field as input. We're going with the enum for
     * now, as a way to guide the user as to what's available/makes sense. However, we could open
     * this wide open in the future by replacing GroupByCriterion with FieldApiDTOs in the input.</p>
     */
    private static final Map<GroupCountRequestApiDTO.GroupByCriterion, FieldApiDTO> GROUP_BY_CRITERION_MAPPING =
        ImmutableMap.of(GroupByCriterion.GROUP_TYPE, PrimitiveFieldApiDTO.groupType(),
            GroupByCriterion.ORIGIN, PrimitiveFieldApiDTO.origin());

    /**
     * Gets a list of all supported group types.
     */
    private static final List<EntityType> ALL_SUPPORTED_GROUP_TYPES =
        Arrays.stream(SearchGroupMetadata.values())
            .map(SearchGroupMetadata::getGroupType)
            .map(GroupTypeMapper::fromApiToSearchSchema)
            .collect(Collectors.toList());

    /**
     * The API request being mapped to a DB query.
     */
    private final GroupCountRequestApiDTO request;

    /**
     * Create a search query to retrieve counts of groups.
     *
     * @param request the API request being mapped to a DB query
     * @param readOnlyDSLContext a context for making read-only database queries
     */
    public GroupCountQuery(@Nonnull final GroupCountRequestApiDTO request,
                            @Nonnull final DSLContext readOnlyDSLContext) {
        super(Objects.requireNonNull(readOnlyDSLContext));
        this.request = Objects.requireNonNull(request);
    }

    @Override
    protected Map<FieldApiDTO, SearchMetadataMapping> lookupMetadataMapping() {
        return SearchGroupMetadata.getGroupCommonFieldsMappingMap();
    }

    @Override
    protected Condition buildWhereClause() {
        return SearchEntity.SEARCH_ENTITY.TYPE.in(ALL_SUPPORTED_GROUP_TYPES);
    }

    @Override
    protected Stream<FieldApiDTO> getGroupByFields() {
        return request.getGroupBy().stream()
            .map(groupByCriterion -> GROUP_BY_CRITERION_MAPPING.get(groupByCriterion));
    }
}
