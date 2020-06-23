package com.vmturbo.search;

import java.util.List;
import java.util.Objects;
import java.util.Set;

import com.google.common.collect.Lists;

import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;

import com.vmturbo.api.dto.searchquery.EntityQueryApiDTO;
import com.vmturbo.api.dto.searchquery.FieldApiDTO;
import com.vmturbo.api.dto.searchquery.PrimitiveFieldApiDTO;
import com.vmturbo.api.dto.searchquery.SelectEntityApiDTO;
import com.vmturbo.api.dto.searchquery.WhereApiDTO;
import com.vmturbo.api.enums.EntityType;
import com.vmturbo.extractor.schema.tables.SearchEntity;
import com.vmturbo.search.mappers.EntityTypeMapper;

/**
 * A representation of a single API entity query, mapped to a SQL query.
 */
public class EntityQuery extends AbstractQuery {

    private final EntityQueryApiDTO request;

    /**
     * Create an entity query instance.
     *
     * @param entityQueryApiDTO the API input payload
     * @param readOnlyDSLContext a context for creating database connections
     */
    public EntityQuery(final EntityQueryApiDTO entityQueryApiDTO,
                       final DSLContext readOnlyDSLContext) {
        super(readOnlyDSLContext);
        this.request = Objects.requireNonNull(entityQueryApiDTO);
    }

    @Override
    protected String getMetadataKey() {
        return getRequest().getSelect().getEntityType().name();
    }

    @Override
    protected Set<Field> buildCommonFields() {
        Set<Field> commonFields = super.buildCommonFields();
        commonFields.add(buildAndTrackSelectFieldFromEntityType(PrimitiveFieldApiDTO.entityType()));
        commonFields.add(buildAndTrackSelectFieldFromEntityType(PrimitiveFieldApiDTO.entityState()));
        commonFields.add(buildAndTrackSelectFieldFromEntityType(PrimitiveFieldApiDTO.entitySeverity()));
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
    protected List<Condition> buildTypeSpecificConditions() {
        SelectEntityApiDTO selectEntity = getRequest().getSelect();
        EntityType type = selectEntity.getEntityType();
        return Lists.newArrayList(
            SearchEntity.SEARCH_ENTITY.TYPE.eq(EntityTypeMapper.fromApiToSearchSchema(type)));
    }

    private EntityQueryApiDTO getRequest() {
        return request;
    }
}
