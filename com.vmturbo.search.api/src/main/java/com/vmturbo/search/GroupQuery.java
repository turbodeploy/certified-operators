package com.vmturbo.search;

import java.util.List;
import java.util.Objects;

import com.google.common.collect.Lists;

import org.jooq.Condition;
import org.jooq.DSLContext;

import com.vmturbo.api.dto.searchquery.FieldApiDTO;
import com.vmturbo.api.dto.searchquery.GroupQueryApiDTO;
import com.vmturbo.api.dto.searchquery.SelectGroupApiDTO;
import com.vmturbo.api.dto.searchquery.WhereApiDTO;
import com.vmturbo.api.enums.GroupType;
import com.vmturbo.extractor.schema.tables.SearchEntity;
import com.vmturbo.search.mappers.GroupTypeMapper;

/**
 * A representation of a single API group query, mapped to a SQL query.
 */
public class GroupQuery extends AbstractQuery {

    private final GroupQueryApiDTO request;

    /**
     * Create a group query instance.
     *
     * @param groupQueryApiDTO the API input payload
     * @param readOnlyDSLContext a context for creating database connections
     */
    public GroupQuery(GroupQueryApiDTO groupQueryApiDTO,
                      final DSLContext readOnlyDSLContext) {
        super(readOnlyDSLContext);
        this.request = Objects.requireNonNull(groupQueryApiDTO);
    }

    @Override
    protected String getMetadataKey() {
        return getRequest().getSelect().getGroupType().name();
    }

    @Override
    protected List<Condition> buildTypeSpecificConditions() {
        SelectGroupApiDTO selectGroup = getRequest().getSelect();
        GroupType type = selectGroup.getGroupType();
        return Lists.newArrayList(
            SearchEntity.SEARCH_ENTITY.TYPE.eq(GroupTypeMapper.fromApiToSearchSchema(type)));
    }

    @Override
    protected List<FieldApiDTO> getSelectedFields() {
        return getRequest().getSelect().getFields();
    }

    @Override
    protected WhereApiDTO getWhere() {
        return this.getRequest().getWhere();
    }

    private GroupQueryApiDTO getRequest() {
        return request;
    }
}
