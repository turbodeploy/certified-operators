package com.vmturbo.extractor.patchers;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.collections4.ListUtils;

import com.vmturbo.api.dto.searchquery.FieldApiDTO.FieldType;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.TypeCase;
import com.vmturbo.commons.Pair;
import com.vmturbo.extractor.export.ExportUtils;
import com.vmturbo.extractor.export.TargetsExtractor;
import com.vmturbo.extractor.schema.json.export.Target;
import com.vmturbo.extractor.search.EnumUtils.EntityStateUtils;
import com.vmturbo.extractor.search.EnumUtils.EntityTypeUtils;
import com.vmturbo.extractor.search.EnumUtils.EnvironmentTypeUtils;
import com.vmturbo.extractor.search.SearchEntityWriter.PartialEntityInfo;
import com.vmturbo.extractor.search.SearchMetadataUtils;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.search.metadata.SearchMetadataMapping;

/**
 * Add basic fields which are available on TopologyEntityDTO, like name, state, type specific info,
 * etc.
 */
public class PrimitiveFieldsOnTEDPatcher extends AbstractPatcher<TopologyEntityDTO> {
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
    public void fetch(PartialEntityInfo recordInfo, TopologyEntityDTO entity) {
        List<SearchMetadataMapping> normalColumnMetadataList =
                NORMAL_COLUMN_METADATA_BY_ENTITY_TYPE.get(entity.getEntityType());
        if (normalColumnMetadataList == null && patchCase == PatchCase.REPORTING) {
            normalColumnMetadataList = COMMON_PRIMITIVE_ON_TED_METADATA;
        }

        // normal columns
        ListUtils.emptyIfNull(normalColumnMetadataList).forEach(metadata -> {
            Optional<Object> fieldValue = metadata.getTopoFieldFunction().apply(entity);
            if (fieldValue.isPresent()) {
                final Object value = fieldValue.get();
                switch (metadata) {
                    case PRIMITIVE_ENTITY_TYPE:
                        recordInfo.putAttr(metadata,
                                // don't use SearchEntityTypeUtils, since this
                                // patcher is also used in reporting, which is not
                                // subject to search's entity type whitelist
                                getEnumDbValue(value, patchCase,
                                                Integer.class,
                                                entityType -> EntityTypeUtils.protoIntToDb(entityType, null)));
                        break;
                    case PRIMITIVE_STATE:
                        recordInfo.putAttr(metadata,
                                getEnumDbValue(value, patchCase,
                                                TopologyDTO.EntityState.class,
                                                EntityStateUtils::protoToDb));
                        break;
                    case PRIMITIVE_ENVIRONMENT_TYPE:
                        recordInfo.putAttr(metadata,
                                getEnumDbValue(value, patchCase,
                                                EnvironmentTypeEnum.EnvironmentType.class,
                                                EnvironmentTypeUtils::protoToDb));
                        break;
                    default:
                        // for other fields like oid, name.
                        recordInfo.putAttr(metadata, value);
                }
            }
        });

        // other columns
        extractAttrs(recordInfo, entity);
    }

    @Override
    protected void extractAttrs(@Nonnull PartialEntityInfo recordInfo, @Nonnull TopologyEntityDTO e) {
        // tags
        if (e.hasTags()) {
            if (patchCase == PatchCase.EXPORTER) {
                recordInfo.putJsonAttr(ExportUtils.TAGS_JSON_KEY_NAME,
                                ExportUtils.tagsToKeyValueConcatSet(e.getTags()));
            } else if (patchCase == PatchCase.REPORTING) {
                recordInfo.putJsonAttr(ExportUtils.TAGS_JSON_KEY_NAME,
                                ExportUtils.tagsToMap(e.getTags()));
            }
        }

        // type specific info
        final List<SearchMetadataMapping> attrsMetadata =
                JSONB_COLUMN_METADATA_BY_ENTITY_TYPE.get(e.getEntityType());
        ListUtils.emptyIfNull(attrsMetadata).forEach(metadata ->
                metadata.getTopoFieldFunction().apply(e).ifPresent(value ->
                recordInfo.putAttr(metadata, value)));

        //TODO: Move to new search SearchMetadataMapping when map type is supported.
        if (e.getTypeSpecificInfo().getTypeCase() == TypeCase.VIRTUAL_MACHINE
                && !e.getTypeSpecificInfo().getVirtualMachine().getPartitionsMap().isEmpty()) {
            recordInfo.putJsonAttr(ExportUtils.PARTITION_MAP_JSON_KEY_NAME,
                 e.getTypeSpecificInfo().getVirtualMachine().getPartitionsMap());
        }

        // targets info
        if (targetsExtractor != null
                && (patchCase == PatchCase.REPORTING || patchCase == PatchCase.EXPORTER)) {
            List<Target> targets = targetsExtractor.extractTargets(e);
            if (targets != null) {
                recordInfo.putJsonAttr(ExportUtils.TARGETS_JSON_KEY_NAME, targets);
            }
        }
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
