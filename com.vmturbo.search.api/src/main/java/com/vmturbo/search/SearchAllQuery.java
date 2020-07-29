package com.vmturbo.search;

import static com.vmturbo.search.metadata.SearchMetadataMapping.PRIMITIVE_ENTITY_TYPE;
import static com.vmturbo.search.metadata.SearchMetadataMapping.PRIMITIVE_NAME;
import static com.vmturbo.search.metadata.SearchMetadataMapping.PRIMITIVE_OID;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.SortField;

import com.vmturbo.api.dto.searchquery.ConditionApiDTO;
import com.vmturbo.api.dto.searchquery.FieldApiDTO;
import com.vmturbo.api.dto.searchquery.OrderByApiDTO;
import com.vmturbo.api.dto.searchquery.PaginationApiDTO;
import com.vmturbo.api.dto.searchquery.PrimitiveFieldApiDTO;
import com.vmturbo.api.dto.searchquery.SearchAllQueryApiDTO;
import com.vmturbo.api.dto.searchquery.SelectAllApiDTO;
import com.vmturbo.api.dto.searchquery.WhereApiDTO;
import com.vmturbo.extractor.schema.tables.SearchEntity;
import com.vmturbo.search.mappers.TypeMapper;
import com.vmturbo.search.metadata.SearchMetadataMapping;

/**
 * A representation of a single API search all query, mapped to a SQL query.
 */
public class SearchAllQuery extends AbstractSearchQuery {

    @VisibleForTesting
    static final HashMap<FieldApiDTO, SearchMetadataMapping> SEARCH_ALL_METADATA = new HashMap<FieldApiDTO, SearchMetadataMapping>() {{
                put(PrimitiveFieldApiDTO.oid(), PRIMITIVE_OID);
                put(PRIMITIVE_TYPE, PRIMITIVE_ENTITY_TYPE);
                put(PrimitiveFieldApiDTO.name(), PRIMITIVE_NAME);
            }};
    /**
     * The API input payload.
     */
    private final SearchAllQueryApiDTO request;

    /**
     * Creates a query to retrieve data from the search db.
     *
     * @param searchAllQueryApiDTO  the API input payload
     * @param readOnlyDSLContext a context for making read-only database queries
     * @param defaultLimit       default limit of results to return
     * @param maxLimit           max number of results to return
     */
    public SearchAllQuery(@NonNull final SearchAllQueryApiDTO searchAllQueryApiDTO, @NonNull DSLContext readOnlyDSLContext,
            int defaultLimit, int maxLimit) {
        super(readOnlyDSLContext, defaultLimit, maxLimit);
        this.request = searchAllQueryApiDTO;
    }

    @Override
    protected Map<FieldApiDTO, SearchMetadataMapping> lookupMetadataMapping() {
        return SEARCH_ALL_METADATA;
    }

    @Override
    protected List<Condition> buildTypeSpecificConditions() {
        SelectAllApiDTO selectAll = request.getSelect();
        if (selectAll == null) {
            return Collections.emptyList();
        }

        List<com.vmturbo.extractor.schema.enums.EntityType> jooqEntityAndGroupTypes =
                selectAll.getEntityTypes().stream()
                        .map(TypeMapper::fromApiToSearchSchema)
                        .collect(Collectors.toList());
        jooqEntityAndGroupTypes.addAll(
                selectAll.getGroupTypes().stream()
                        .map(TypeMapper::fromApiToSearchSchema)
                        .collect(Collectors.toList()));

        return Lists.newArrayList(SearchEntity.SEARCH_ENTITY.TYPE.in((jooqEntityAndGroupTypes)));
    }

    @Nonnull
    @Override
    protected List<FieldApiDTO> getSelectedFields() {
        return Collections.emptyList();
    }

    @Override
    protected Set<Field> buildCommonFields() {
        Set<Field> commonFields = super.buildCommonFields();
        commonFields.add(buildAndTrackSelectFieldFromEntityType(PRIMITIVE_TYPE));
        return commonFields;
    }

    @NonNull
    @Override
    protected WhereApiDTO getWhere() {
        return this.request.getWhere();
    }

    @Nullable
    @Override
    protected PaginationApiDTO getPaginationApiDto() {
       return this.request.getPagination();
    }

    @NonNull
    @Override
    protected List<Condition> buildGenericConditions() {
        final WhereApiDTO whereEntity = getWhere();

        if (whereEntity == null || whereEntity.getConditions().isEmpty()) {
            return Collections.EMPTY_LIST;
        }

        List<ConditionApiDTO> whereDtos = whereEntity.getConditions();

        if (whereDtos.size() > 1 || !whereDtos.get(0).getField().equals(PrimitiveFieldApiDTO.name())) {
            throw new IllegalArgumentException("Only filtering on Name field allowed");

        }

        return super.buildGenericConditions();
    }

    @NonNull
    @Override
    protected LinkedHashSet<SortField<?>> buildOrderByFields() {
        if (this.orderBy == null) {
            if (!getOrderBy().isEmpty()) {
                for (OrderByApiDTO orderBy: getOrderBy()) {
                    if (!orderBy.getField().equals(PrimitiveFieldApiDTO.name())) {
                        throw new IllegalArgumentException("Sorting only allowed on name/type/oid");
                    }
                }
            }
            super.buildOrderByFields();
        }
        return this.orderBy;
    }
}
