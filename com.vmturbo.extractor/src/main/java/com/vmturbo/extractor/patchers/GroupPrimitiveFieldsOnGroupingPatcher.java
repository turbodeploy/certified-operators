package com.vmturbo.extractor.patchers;

import static com.vmturbo.extractor.models.ModelDefinitions.SEARCH_ENTITY_TABLE;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.dto.searchquery.FieldApiDTO.FieldType;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.extractor.models.Column;
import com.vmturbo.extractor.schema.enums.EntityType;
import com.vmturbo.extractor.search.EnumUtils.GroupTypeUtils;
import com.vmturbo.extractor.search.SearchEntityWriter.EntityRecordPatcher;
import com.vmturbo.extractor.search.SearchEntityWriter.PartialRecordInfo;
import com.vmturbo.extractor.search.SearchMetadataUtils;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO;
import com.vmturbo.search.metadata.SearchMetadataMapping;

/**
 * Add basic fields which are available on {@link Grouping}, like name, group type, origin, etc.
 */
public class GroupPrimitiveFieldsOnGroupingPatcher implements EntityRecordPatcher<Grouping> {

    private static final Logger logger = LogManager.getLogger();

    @Override
    public void patch(PartialRecordInfo recordInfo, Grouping group) {
        // find metadata for primitive fields on Grouping (there is group field function defined)
        final List<SearchMetadataMapping> metadataList =
                SearchMetadataUtils.getMetadata(group.getDefinition().getType(), FieldType.PRIMITIVE).stream()
                        .filter(m -> m.getGroupFieldFunction() != null)
                        .collect(Collectors.toList());

        metadataList.forEach(metadata -> {
            Optional<Object> fieldValue = metadata.getGroupFieldFunction().apply(group);
            if (metadata.getJsonKeyName() != null) {
                // jsonb column, add to json map
                fieldValue.ifPresent(value -> recordInfo.putAttrs(metadata.getJsonKeyName(), value));
            } else {
                // normal columns
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
                            // convert to database enum
                            recordInfo.getRecord().set((Column<EntityType>)column,
                                    GroupTypeUtils.protoToDb((GroupDTO.GroupType)value));
                            break;
                        default:
                            // for other fields like oid, name, origin.
                            recordInfo.getRecord().set((Column<Object>)column, value);
                    }
                }
            }
        });
    }
}
