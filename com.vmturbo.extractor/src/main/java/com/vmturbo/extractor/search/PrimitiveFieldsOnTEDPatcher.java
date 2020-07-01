package com.vmturbo.extractor.search;

import static com.vmturbo.extractor.models.ModelDefinitions.SEARCH_ENTITY_TABLE;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.dto.searchquery.FieldApiDTO.FieldType;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.extractor.models.Column;
import com.vmturbo.extractor.schema.enums.EntityState;
import com.vmturbo.extractor.schema.enums.EntityType;
import com.vmturbo.extractor.schema.enums.EnvironmentType;
import com.vmturbo.extractor.search.SearchEntityWriter.EntityRecordPatcher;
import com.vmturbo.extractor.search.SearchEntityWriter.PartialRecordInfo;
import com.vmturbo.search.metadata.SearchMetadataMapping;

/**
 * Add basic fields which are available on TopologyEntityDTO, like name, state, type specific info, etc.
 */
public class PrimitiveFieldsOnTEDPatcher implements EntityRecordPatcher<TopologyEntityDTO> {

    private static final Logger logger = LogManager.getLogger();

    @Override
    public void patch(PartialRecordInfo recordInfo, TopologyEntityDTO entity) {
        // find metadata for primitive fields on TopologyEntityDTO (there is topo function defined)
        final List<SearchMetadataMapping> metadataList =
                SearchMetadataUtils.getMetadata(entity.getEntityType(), FieldType.PRIMITIVE).stream()
                        .filter(m -> m.getTopoFieldFunction() != null)
                        .collect(Collectors.toList());

        metadataList.forEach(metadata -> {
            Optional<Object> fieldValue = metadata.getTopoFieldFunction().apply(entity);
            if (metadata.getJsonKeyName() != null) {
                // jsonb column, add to json map
                fieldValue.ifPresent(value -> recordInfo.attrs.put(metadata.getJsonKeyName(), value));
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
                            recordInfo.record.set((Column<EntityType>)column,
                                    EnumUtils.entityTypeFromProtoIntToDb((int)value));
                            break;
                        case ENTITY_STATE:
                            recordInfo.record.set((Column<EntityState>)column,
                                    EnumUtils.entityStateFromProtoToDb(
                                            (TopologyDTO.EntityState)value));
                            break;
                        case ENVIRONMENT_TYPE:
                            recordInfo.record.set((Column<EnvironmentType>)column,
                                    EnumUtils.environmentTypeFromProtoToDb(
                                            (EnvironmentTypeEnum.EnvironmentType)value));
                            break;
                        default:
                            // for other fields like oid, name.
                            recordInfo.record.set((Column<Object>)column, value);
                    }
                }
            }
        });
    }
}
