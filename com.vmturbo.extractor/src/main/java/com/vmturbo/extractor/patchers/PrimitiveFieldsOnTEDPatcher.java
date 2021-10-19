package com.vmturbo.extractor.patchers;

import static com.vmturbo.extractor.export.ExportUtils.PARTITION_MAP_JSON_KEY_NAME;
import static com.vmturbo.extractor.export.ExportUtils.TAGS_JSON_KEY_NAME;
import static com.vmturbo.extractor.export.ExportUtils.TARGETS_JSON_KEY_NAME;

import java.util.Arrays;
import java.util.Collections;
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
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.TypeCase;
import com.vmturbo.commons.Pair;
import com.vmturbo.extractor.export.ExportUtils;
import com.vmturbo.extractor.export.TargetsExtractor;
import com.vmturbo.extractor.models.Column;
import com.vmturbo.extractor.models.Table;
import com.vmturbo.extractor.schema.enums.EntityState;
import com.vmturbo.extractor.schema.enums.EntityType;
import com.vmturbo.extractor.schema.enums.EnvironmentType;
import com.vmturbo.extractor.schema.json.export.Target;
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

    public static Map<Integer, List<SearchMetadataMapping>> getJsonbColumnMetadataByEntityType() {
        return Collections.unmodifiableMap(JSONB_COLUMN_METADATA_BY_ENTITY_TYPE);
    }


    private static final List<SearchMetadataMapping> COMMON_PRIMITIVE_ON_TED_METADATA =
            SearchMetadataUtils.COMMON_PRIMITIVE_METADATA.stream()
                    .filter(m -> m.getTopoFieldFunction() != null)
                    .collect(Collectors.toList());

    private final PatchCase patchCase;
    private final TargetsExtractor targetsExtractor;

    /**
     * Create a new patcher.
     *
     * @param patchCase the use case for this patcher
     * @param targetsExtractor for extracting targets info
     */
    public PrimitiveFieldsOnTEDPatcher(@Nonnull PatchCase patchCase, @Nullable TargetsExtractor targetsExtractor) {
        this.patchCase = patchCase;
        this.targetsExtractor = targetsExtractor;
    }

    @Override
    public void patch(PartialRecordInfo recordInfo, TopologyEntityDTO entity) {
        List<SearchMetadataMapping> normalColumnMetadataList =
                NORMAL_COLUMN_METADATA_BY_ENTITY_TYPE.get(entity.getEntityType());
        if (normalColumnMetadataList == null && patchCase == PatchCase.REPORTING) {
            normalColumnMetadataList = COMMON_PRIMITIVE_ON_TED_METADATA;
        }

        // normal columns
        ListUtils.emptyIfNull(normalColumnMetadataList).forEach(metadata -> {
            Optional<Object> fieldValue = metadata.getTopoFieldFunction().apply(entity);
            Table destinationTable = recordInfo.getRecord().getTable();
            Column<?> column = destinationTable.getColumn(metadata.getColumnName());
            if (column == null) {
                logger.error("No column {} in table {}", metadata.getColumnName(),
                                destinationTable.getName());
                return;
            }
            if (fieldValue.isPresent()) {
                final Object value = fieldValue.get();
                if (patchCase == PatchCase.SEARCH) {
                    // TODO transform to db value using SearchMetadataMapping definitions
                } else {
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
        // tags
        if (e.hasTags()) {
            if (patchCase == PatchCase.EXPORTER) {
                attrs.put(TAGS_JSON_KEY_NAME, ExportUtils.tagsToKeyValueConcatSet(e.getTags()));
            } else if (patchCase == PatchCase.REPORTING) {
                attrs.put(TAGS_JSON_KEY_NAME, ExportUtils.tagsToMap(e.getTags()));
            }
        }

        // type specific info
        final List<SearchMetadataMapping> attrsMetadata =
                JSONB_COLUMN_METADATA_BY_ENTITY_TYPE.get(e.getEntityType());
        ListUtils.emptyIfNull(attrsMetadata).forEach(metadata ->
                metadata.getTopoFieldFunction().apply(e).ifPresent(value ->
                        attrs.put(metadata.getJsonKeyName(), value)));

        //TODO: Move to new search SearchMetadataMapping when map type is supported.
        if (e.getTypeSpecificInfo().getTypeCase() == TypeCase.VIRTUAL_MACHINE
                && !e.getTypeSpecificInfo().getVirtualMachine().getPartitionsMap().isEmpty()) {
            attrs.put(PARTITION_MAP_JSON_KEY_NAME,
                 e.getTypeSpecificInfo().getVirtualMachine().getPartitionsMap());
        }

        // targets info
        if (targetsExtractor != null
                && (patchCase == PatchCase.REPORTING || patchCase == PatchCase.EXPORTER)) {
            List<Target> targets = targetsExtractor.extractTargets(e);
            if (targets != null) {
                attrs.put(TARGETS_JSON_KEY_NAME, targets);
            }
        }

        return attrs.isEmpty() ? null : attrs;
    }

    /**
     * Enum for different use cases of patchers.
     */
    public enum PatchCase {
        /**
         * Embedded Reporting.
         */
        REPORTING,
        /**
         * Data Extraction.
         */
        EXPORTER,
        /**
         * New Search.
         */
        SEARCH
    }
}
