package com.vmturbo.extractor.patchers;

import static com.vmturbo.extractor.export.ExportUtils.TAGS_JSON_KEY_NAME;
import static com.vmturbo.extractor.models.ModelDefinitions.SEARCH_ENTITY_TABLE;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.collections4.ListUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.dto.searchquery.FieldApiDTO.FieldType;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.commons.Pair;
import com.vmturbo.extractor.export.ExportUtils;
import com.vmturbo.extractor.models.Column;
import com.vmturbo.extractor.schema.enums.EntityState;
import com.vmturbo.extractor.schema.enums.EntityType;
import com.vmturbo.extractor.schema.enums.EnvironmentType;
import com.vmturbo.extractor.search.EnumUtils.EntityStateUtils;
import com.vmturbo.extractor.search.EnumUtils.EntityTypeUtils;
import com.vmturbo.extractor.search.EnumUtils.EnvironmentTypeUtils;
import com.vmturbo.extractor.search.SearchEntityWriter.EntityRecordPatcher;
import com.vmturbo.extractor.search.SearchEntityWriter.PartialRecordInfo;
import com.vmturbo.extractor.search.SearchMetadataUtils;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.search.metadata.SearchMetadataMapping;

/**
 * Add basic fields which are available on TopologyEntityDTO, like name, state, type specific info,
 * etc.
 */
public class PrimitiveFieldsOnTEDPatcher implements EntityRecordPatcher<TopologyEntityDTO> {

    private static final Logger logger = LogManager.getLogger();

    /**
     * Metadata list for primitive fields on TopologyEntityDTO (there is topo function defined),
     * grouped by entity type, for normal columns.
     */
    private static final Map<Integer, List<SearchMetadataMapping>> NORMAL_COLUMN_METADATA_BY_ENTITY_TYPE =
            Arrays.stream(EntityDTO.EntityType.values()).map(entityType -> {
                List<SearchMetadataMapping> metadata = SearchMetadataUtils.getMetadata(
                        entityType.getNumber(), FieldType.PRIMITIVE).stream()
                        .filter(m -> m.getTopoFieldFunction() != null)
                        .filter(m -> m.getJsonKeyName() == null)
                        .collect(Collectors.toList());
                return new Pair<>(entityType.getNumber(), metadata);
            }).filter(pair -> !pair.second.isEmpty())
            .collect(Collectors.toMap(p -> p.first, p -> p.second));

    /**
     * Metadata list for primitive fields on TopologyEntityDTO (there is topo function defined),
     * grouped by entity type, for jsonb column.
     */
    private static final Map<Integer, List<SearchMetadataMapping>> JSONB_COLUMN_METADATA_BY_ENTITY_TYPE =
            Arrays.stream(EntityDTO.EntityType.values()).map(entityType -> {
                List<SearchMetadataMapping> metadata = SearchMetadataUtils.getMetadata(
                        entityType.getNumber(), FieldType.PRIMITIVE).stream()
                        .filter(m -> m.getTopoFieldFunction() != null)
                        .filter(m -> m.getJsonKeyName() != null)
                        .collect(Collectors.toList());
                return new Pair<>(entityType.getNumber(), metadata);
            }).filter(pair -> !pair.second.isEmpty())
                    .collect(Collectors.toMap(p -> p.first, p -> p.second));


    private static final List<SearchMetadataMapping> COMMON_PRIMITIVE_ON_TED_METADATA =
            SearchMetadataUtils.COMMON_PRIMITIVE_METADATA.stream()
                    .filter(m -> m.getTopoFieldFunction() != null)
                    .collect(Collectors.toList());

    private final boolean patchCommonFieldsIfNoMetadata;
    private final boolean includeTags;
    private final boolean concatTagKeyValue;

    /**
     * Create a new patcher.
     *  @param patchCommonFieldsIfNoMetadata if true, patch common fields even for entity types wholly
     *                                      lacking metadata
     * @param includeTags whether or not to patch tags
     * @param concatTagKeyValue whether or not to combine tag key and value using = as separator
     *                          and put all combinations into a list like: ["owner=alex","owner=bob"]
     */
    public PrimitiveFieldsOnTEDPatcher(boolean patchCommonFieldsIfNoMetadata, boolean includeTags,
            boolean concatTagKeyValue) {
        this.patchCommonFieldsIfNoMetadata = patchCommonFieldsIfNoMetadata;
        this.includeTags = includeTags;
        this.concatTagKeyValue = concatTagKeyValue;
    }

    @Override
    public void patch(PartialRecordInfo recordInfo, TopologyEntityDTO entity) {
        List<SearchMetadataMapping> normalColumnMetadataList =
                NORMAL_COLUMN_METADATA_BY_ENTITY_TYPE.get(entity.getEntityType());
        if (normalColumnMetadataList == null && patchCommonFieldsIfNoMetadata) {
            normalColumnMetadataList = COMMON_PRIMITIVE_ON_TED_METADATA;
        }

        // normal columns
        ListUtils.emptyIfNull(normalColumnMetadataList).forEach(metadata -> {
            Optional<Object> fieldValue = metadata.getTopoFieldFunction().apply(entity);
            Column<?> column = SEARCH_ENTITY_TABLE.getColumn(metadata.getColumnName());
            if (column == null) {
                logger.error("No column {} in table {}", metadata.getColumnName(),
                        SEARCH_ENTITY_TABLE.getName());
                return;
            }
            if (fieldValue.isPresent()) {
                final Object value = fieldValue.get();
                switch (column.getColType()) {
                    case ENTITY_TYPE:
                        recordInfo.getRecord().set((Column<EntityType>)column,
                                // don't use SearchEntityTypeUtils, since this
                                // patcher is also used in reporting, which is not
                                // subject to search's entity type whitelist
                                EntityTypeUtils.protoIntToDb((int)value, null));
                        break;
                    case ENTITY_STATE:
                        recordInfo.getRecord().set((Column<EntityState>)column,
                                EntityStateUtils.protoToDb((TopologyDTO.EntityState)value));
                        break;
                    case ENVIRONMENT_TYPE:
                        recordInfo.getRecord().set((Column<EnvironmentType>)column,
                                EnvironmentTypeUtils.protoToDb((EnvironmentTypeEnum.EnvironmentType)value));
                        break;
                    default:
                        // for other fields like oid, name.
                        recordInfo.getRecord().set((Column<Object>)column, value);
                }
            }
        });

        // jsonb column
        Map<String, Object> attrs = extractAttrs(entity);
        if (attrs != null) {
            recordInfo.putAllAttrs(attrs);
        }
    }

    /**
     * Extract attributes from given entity.
     *
     * @param e TopologyEntityDTO
     * @return mapping from attr name to attr value
     */
    @Nullable
    public Map<String, Object> extractAttrs(@Nonnull TopologyEntityDTO e) {
        final Map<String, Object> attrs = new HashMap<>();
        if (includeTags && e.hasTags()) {
            if (concatTagKeyValue) {
                attrs.put(TAGS_JSON_KEY_NAME, ExportUtils.tagsToKeyValueConcatSet(e.getTags()));
            } else {
                attrs.put(TAGS_JSON_KEY_NAME, ExportUtils.tagsToMap(e.getTags()));
            }
        }

        final List<SearchMetadataMapping> attrsMetadata =
                JSONB_COLUMN_METADATA_BY_ENTITY_TYPE.get(e.getEntityType());
        ListUtils.emptyIfNull(attrsMetadata).forEach(metadata ->
                metadata.getTopoFieldFunction().apply(e).ifPresent(value ->
                        attrs.put(metadata.getJsonKeyName(), value)));
        return attrs.isEmpty() ? null : attrs;
    }
}
