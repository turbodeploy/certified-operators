package com.vmturbo.search;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.google.common.collect.Lists;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;

import com.vmturbo.api.dto.searchquery.EntityQueryApiDTO;
import com.vmturbo.api.dto.searchquery.FieldApiDTO;
import com.vmturbo.api.dto.searchquery.OrderByApiDTO;
import com.vmturbo.api.dto.searchquery.PaginationApiDTO;
import com.vmturbo.api.dto.searchquery.PrimitiveFieldApiDTO;
import com.vmturbo.api.dto.searchquery.SelectEntityApiDTO;
import com.vmturbo.api.dto.searchquery.WhereApiDTO;
import com.vmturbo.api.enums.EntityType;
import com.vmturbo.common.api.mappers.EnumMapper;
import com.vmturbo.extractor.schema.tables.SearchEntity;
import com.vmturbo.search.mappers.EntityTypeMapper;
import com.vmturbo.search.metadata.SearchEntityMetadata;
import com.vmturbo.search.metadata.SearchMetadataMapping;

/**
 * A representation of a single API entity query, mapped to a SQL query.
 */
public class EntityQuery extends AbstractQuery {

    private final EntityQueryApiDTO request;

    /**
     * Provides a mapping from String -> SearchEntityMetadata.
     */
    public static final EnumMapper<SearchEntityMetadata> SEARCH_ENTITY_METADATA_ENUM_MAPPER =
            new EnumMapper<>(SearchEntityMetadata.class);

    /**
     * Create an entity query instance.
     *
     * @param entityQueryApiDTO the API input payload
     * @param readOnlyDSLContext a context for creating database connections
     */
    public EntityQuery(@NonNull final EntityQueryApiDTO entityQueryApiDTO,
            @NonNull final DSLContext readOnlyDSLContext) {
        super(readOnlyDSLContext);
        this.request = Objects.requireNonNull(entityQueryApiDTO);
        this.setType(request.getSelect().getEntityType().name());
    }

    @Override
    protected Set<Field> buildCommonFields() {
        Set<Field> commonFields = super.buildCommonFields();
        commonFields.add(buildAndTrackSelectFieldFromEntityType(PrimitiveFieldApiDTO.entityType()));
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
    protected List<OrderByApiDTO> getOrderBy() {
        PaginationApiDTO pag = getPaginationApiDto();
        return pag == null ? Collections.emptyList() : pag.getOrderBy();
    }

    @Override
    protected PaginationApiDTO getPaginationApiDto() {
        return this.getRequest().getPagination();
    }

    @Override
    protected List<Condition> buildTypeSpecificConditions() {
        SelectEntityApiDTO selectEntity = getRequest().getSelect();
        EntityType type = selectEntity.getEntityType();
        return Lists.newArrayList(
            SearchEntity.SEARCH_ENTITY.TYPE.eq(EntityTypeMapper.fromApiToSearchSchema(type)));
    }

    @Override
    protected Map<FieldApiDTO, SearchMetadataMapping> lookupMetadataMapping() {
        return SEARCH_ENTITY_METADATA_ENUM_MAPPER.valueOf(getType())
                .map(SearchEntityMetadata::getMetadataMappingMap)
                .orElseThrow(() -> new IllegalArgumentException(
                        "No data for metadataMappingKey: " + getType()));
    }

    private EntityQueryApiDTO getRequest() {
        return request;
    }
}
