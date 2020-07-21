package com.vmturbo.search;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;

import com.vmturbo.api.dto.searchquery.FieldApiDTO;
import com.vmturbo.api.dto.searchquery.GroupQueryApiDTO;
import com.vmturbo.api.dto.searchquery.PaginationApiDTO;
import com.vmturbo.api.dto.searchquery.PrimitiveFieldApiDTO;
import com.vmturbo.api.dto.searchquery.SelectGroupApiDTO;
import com.vmturbo.api.dto.searchquery.WhereApiDTO;
import com.vmturbo.api.enums.GroupType;
import com.vmturbo.extractor.schema.tables.SearchEntity;
import com.vmturbo.search.mappers.GroupTypeMapper;
import com.vmturbo.search.metadata.SearchGroupMetadata;
import com.vmturbo.search.metadata.SearchMetadataMapping;

/**
 * A representation of a single API group query, mapped to a SQL query.
 */
public class GroupQuery extends AbstractSearchQuery {

    private final GroupQueryApiDTO request;

    /**
     * Create a group query instance.
     *
     * @param groupQueryApiDTO the API input payload
     * @param readOnlyDSLContext a context for creating database connections
     * @param apiPaginationDefaultLimit default limit of results to return
     * @param apiPaginationMaxLimit max number of results to return
     */
    public GroupQuery(@NonNull GroupQueryApiDTO groupQueryApiDTO,
            @NonNull final DSLContext readOnlyDSLContext,
            @NonNull final int apiPaginationDefaultLimit,
            @NonNull final int apiPaginationMaxLimit) {
        super(readOnlyDSLContext, apiPaginationDefaultLimit, apiPaginationMaxLimit);

        this.request = groupQueryApiDTO;
    }

    @Override
    protected List<Condition> buildTypeSpecificConditions() {
        SelectGroupApiDTO selectGroup = getRequest().getSelect();
        GroupType type = selectGroup.getGroupType();
        return Lists.newArrayList(
            SearchEntity.SEARCH_ENTITY.TYPE.eq(GroupTypeMapper.fromApiToSearchSchema(type)));
    }

    @Override
    protected Map<FieldApiDTO, SearchMetadataMapping> lookupMetadataMapping() {
        String groupType = this.request.getSelect().getGroupType().name();
        return SEARCH_GROUP_METADATA_ENUM_MAPPER.valueOf(groupType)
            .map(SearchGroupMetadata::getMetadataMappingMap)
            .orElseThrow(() -> new IllegalArgumentException(
                "No data for metadataMappingKey: " + groupType));
    }

    @Override
    protected Set<Field> buildCommonFields() {
        Set<Field> commonFields = super.buildCommonFields();
        commonFields.add(buildAndTrackSelectFieldFromEntityType(PrimitiveFieldApiDTO.groupType()));
        return commonFields;
    }

    @Override
    protected List<FieldApiDTO> getSelectedFields() {
        return getRequest().getSelect().getFields();
    }

    @Override
    protected WhereApiDTO getWhere() {
        return this.getRequest().getWhere();
    }

    @Override
    protected PaginationApiDTO getPaginationApiDto() {
        return this.getRequest().getPagination();
    }

    private GroupQueryApiDTO getRequest() {
        return request;
    }
}
