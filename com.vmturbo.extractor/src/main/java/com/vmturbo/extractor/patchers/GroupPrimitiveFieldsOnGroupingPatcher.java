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
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.commons.Pair;
import com.vmturbo.extractor.export.ExportUtils;
import com.vmturbo.extractor.export.TargetsExtractor;
import com.vmturbo.extractor.patchers.PrimitiveFieldsOnTEDPatcher.PatchCase;
import com.vmturbo.extractor.schema.json.export.Target;
import com.vmturbo.extractor.search.EnumUtils.GroupTypeUtils;
import com.vmturbo.extractor.search.SearchEntityWriter.PartialEntityInfo;
import com.vmturbo.extractor.search.SearchMetadataUtils;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.search.metadata.SearchMetadataMapping;

/**
 * Add basic fields which are available on {@link Grouping}, like name, group type, origin, etc.
 */
public class GroupPrimitiveFieldsOnGroupingPatcher extends AbstractPatcher<Grouping> {
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
    public void fetch(PartialEntityInfo recordInfo, Grouping group) {
        // normal columns
        ListUtils.emptyIfNull(NORMAL_COLUMN_METADATA_BY_GROUP_TYPE.get(group.getDefinition().getType()))
                .forEach(metadata -> {
                    Optional<Object> fieldValue = metadata.getGroupFieldFunction().apply(group);
                    if (fieldValue.isPresent()) {
                        if (metadata == SearchMetadataMapping.PRIMITIVE_GROUP_TYPE) {
                            recordInfo.putAttr(metadata,
                                            getEnumDbValue(fieldValue.get(), patchCase,
                                                            GroupDTO.GroupType.class,
                                                            GroupTypeUtils::protoToDb));
                        } else {
                            recordInfo.putAttr(metadata, fieldValue.get());
                        }
                    }
                });

        if (patchCase == PatchCase.SEARCH) {
            // dummy values for groups going into same table as entities
            recordInfo.putAttr(SearchMetadataMapping.PRIMITIVE_STATE,
                            TopologyDTO.EntityState.UNKNOWN.ordinal());
            recordInfo.putAttr(SearchMetadataMapping.PRIMITIVE_ENVIRONMENT_TYPE,
                            EnvironmentType.UNKNOWN_ENV.ordinal());
        }

        // other columns
        extractAttrs(recordInfo, group);
    }

    @Override
    protected void extractAttrs(@Nonnull PartialEntityInfo recordInfo, @Nonnull Grouping group) {
        if (group.getDefinition().hasTags()) {
            if (patchCase == PatchCase.EXPORTER) {
                recordInfo.putJsonAttr(ExportUtils.TAGS_JSON_KEY_NAME, ExportUtils
                                .tagsToKeyValueConcatSet(group.getDefinition().getTags()));
            } else if (patchCase == PatchCase.REPORTING) {
                recordInfo.putJsonAttr(ExportUtils.TAGS_JSON_KEY_NAME,
                                ExportUtils.tagsToMap(group.getDefinition().getTags()));
            }
        }

        ListUtils.emptyIfNull(JSONB_COLUMN_METADATA_BY_GROUP_TYPE.get(group.getDefinition().getType()))
                .forEach(metadata -> metadata.getGroupFieldFunction().apply(group).ifPresent(value ->
                recordInfo.putAttr(metadata, value)));

        // targets info
        if (targetsExtractor != null
                && (patchCase == PatchCase.REPORTING || patchCase == PatchCase.EXPORTER)) {
            List<Target> targets = targetsExtractor.extractTargets(group);
            if (targets != null) {
                recordInfo.putJsonAttr(ExportUtils.TARGETS_JSON_KEY_NAME, targets);
            }
        }
    }

    public static Map<GroupType, List<SearchMetadataMapping>> getJsonbColumnMetadataByGroupType() {
        return Collections.unmodifiableMap(JSONB_COLUMN_METADATA_BY_GROUP_TYPE);
    }
}
