package com.vmturbo.extractor.patchers;

import static com.vmturbo.extractor.export.ExportUtils.TAGS_JSON_KEY_NAME;
import static com.vmturbo.extractor.export.ExportUtils.TARGETS_JSON_KEY_NAME;
import static com.vmturbo.extractor.models.ModelDefinitions.SEARCH_ENTITY_TABLE;

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
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.commons.Pair;
import com.vmturbo.extractor.export.ExportUtils;
import com.vmturbo.extractor.export.TargetsExtractor;
import com.vmturbo.extractor.models.Column;
import com.vmturbo.extractor.patchers.PrimitiveFieldsOnTEDPatcher.PatchCase;
import com.vmturbo.extractor.schema.enums.EntityType;
import com.vmturbo.extractor.schema.json.export.Target;
import com.vmturbo.extractor.search.EnumUtils.GroupTypeUtils;
import com.vmturbo.extractor.search.SearchEntityWriter.EntityRecordPatcher;
import com.vmturbo.extractor.search.SearchEntityWriter.PartialRecordInfo;
import com.vmturbo.extractor.search.SearchMetadataUtils;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.search.metadata.SearchMetadataMapping;

/**
 * Add basic fields which are available on {@link Grouping}, like name, group type, origin, etc.
 */
public class GroupPrimitiveFieldsOnGroupingPatcher implements EntityRecordPatcher<Grouping> {

    private static final Logger logger = LogManager.getLogger();

    /**
     * Metadata list for primitive fields on Grouping (there is group field function defined),
     * grouped by group type, for normal columns.
     */
    private static final Map<GroupType, List<SearchMetadataMapping>> NORMAL_COLUMN_METADATA_BY_GROUP_TYPE =
            Arrays.stream(GroupType.values()).map(groupType -> {
                List<SearchMetadataMapping> metadata = SearchMetadataUtils.getMetadata(
                        groupType, FieldType.PRIMITIVE).stream()
                        .filter(m -> m.getGroupFieldFunction() != null)
                        .filter(m -> m.getJsonKeyName() == null)
                        .collect(Collectors.toList());
                return new Pair<>(groupType, metadata);
            }).filter(pair -> !pair.second.isEmpty())
            .collect(Collectors.toMap(p -> p.first, p -> p.second));

    /**
     * Metadata list for primitive fields on Grouping (there is group field function defined),
     * grouped by group type, for jsonb column.
     */
    private static final Map<GroupType, List<SearchMetadataMapping>> JSONB_COLUMN_METADATA_BY_GROUP_TYPE =
            Arrays.stream(GroupType.values()).map(groupType -> {
                List<SearchMetadataMapping> metadata = SearchMetadataUtils.getMetadata(
                        groupType, FieldType.PRIMITIVE).stream()
                        .filter(m -> m.getGroupFieldFunction() != null)
                        .filter(m -> m.getJsonKeyName() != null)
                        .collect(Collectors.toList());
                return new Pair<>(groupType, metadata);
            }).filter(pair -> !pair.second.isEmpty())
            .collect(Collectors.toMap(p -> p.first, p -> p.second));

    private final PatchCase patchCase;
    private final TargetsExtractor targetsExtractor;

    /**
     * Constructor.
     *
     * @param patchCase the use case for this patcher
     * @param targetsExtractor for extracting targets info
     */
    public GroupPrimitiveFieldsOnGroupingPatcher(@Nonnull PatchCase patchCase,
            @Nullable TargetsExtractor targetsExtractor) {
        this.patchCase = patchCase;
        this.targetsExtractor = targetsExtractor;
    }

    @Override
    public void patch(PartialRecordInfo recordInfo, Grouping group) {
        // normal columns
        ListUtils.emptyIfNull(NORMAL_COLUMN_METADATA_BY_GROUP_TYPE.get(group.getDefinition().getType()))
                .forEach(metadata -> {
                    Column<?> column = SEARCH_ENTITY_TABLE.getColumn(metadata.getColumnName());
                    if (column == null) {
                        logger.error("No column {} in table {}", metadata.getColumnName(),
                                SEARCH_ENTITY_TABLE.getName());
                        return;
                    }
                    Optional<Object> fieldValue = metadata.getGroupFieldFunction().apply(group);
                    if (fieldValue.isPresent()) {
                        final Object value = fieldValue.get();
                        switch (column.getColType()) {
                            case ENTITY_TYPE:
                                // convert to database enum
                                recordInfo.getRecord().set((Column<EntityType>)column,
                                        GroupTypeUtils.protoToDb((GroupDTO.GroupType)value));
                                break;
                            default:
                                // for other fields like oid, name, origin.
                                recordInfo.getRecord().set((Column<Object>)column, value);
                        }
                    }
                });

        // jsonb column
        Map<String, Object> attrs = extractAttrs(group);
        if (attrs != null) {
            recordInfo.putAllAttrs(attrs);
        }
    }

    /**
     * Extract attributes from given group.
     *
     * @param group group
     * @return mapping from attr name to attr value
     */
    @Nullable
    public Map<String, Object> extractAttrs(@Nonnull Grouping group) {
        final Map<String, Object> attrs = new HashMap<>();
        if (group.getDefinition().hasTags()) {
            if (patchCase == PatchCase.EXPORTER) {
                attrs.put(TAGS_JSON_KEY_NAME, ExportUtils.tagsToKeyValueConcatSet(group.getDefinition().getTags()));
            } else if (patchCase == PatchCase.REPORTING) {
                attrs.put(TAGS_JSON_KEY_NAME, ExportUtils.tagsToMap(group.getDefinition().getTags()));
            }
        }

        ListUtils.emptyIfNull(JSONB_COLUMN_METADATA_BY_GROUP_TYPE.get(group.getDefinition().getType()))
                .forEach(metadata -> metadata.getGroupFieldFunction().apply(group).ifPresent(value ->
                        attrs.put(metadata.getJsonKeyName(), value)));

        // targets info
        if (targetsExtractor != null
                && (patchCase == PatchCase.REPORTING || patchCase == PatchCase.EXPORTER)) {
            List<Target> targets = targetsExtractor.extractTargets(group);
            if (targets != null) {
                attrs.put(TARGETS_JSON_KEY_NAME, targets);
            }
        }

        return attrs.isEmpty() ? null : attrs;
    }

    public static Map<GroupType, List<SearchMetadataMapping>> getJsonbColumnMetadataByGroupType() {
        return Collections.unmodifiableMap(JSONB_COLUMN_METADATA_BY_GROUP_TYPE);
    }
}
