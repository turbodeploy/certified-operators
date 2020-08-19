package com.vmturbo.extractor.patchers;

import static com.vmturbo.extractor.models.ModelDefinitions.SEARCH_ENTITY_TABLE;

import java.util.List;
import java.util.Optional;

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
import com.vmturbo.extractor.search.EnumUtils.EntityStateUtils;
import com.vmturbo.extractor.search.EnumUtils.EntityTypeUtils;
import com.vmturbo.extractor.search.EnumUtils.EnvironmentTypeUtils;
import com.vmturbo.extractor.search.SearchEntityWriter.EntityRecordPatcher;
import com.vmturbo.extractor.search.SearchEntityWriter.PartialRecordInfo;
import com.vmturbo.extractor.search.SearchMetadataUtils;
import com.vmturbo.search.metadata.SearchMetadataMapping;

/**
 * Add basic fields which are available on TopologyEntityDTO, like name, state, type specific info,
 * etc.
 */
public class PrimitiveFieldsOnTEDPatcher implements EntityRecordPatcher<TopologyEntityDTO> {

    private static final Logger logger = LogManager.getLogger();
    private final boolean alwaysPatchCommonFields;

    /**
     * Create a new patcher.
     */
    public PrimitiveFieldsOnTEDPatcher() {
        this(false);
    }

    /**
     * Create a new patcher.
     *
     * @param alwaysPatchCommonFields if true, patch common fields even for entity types wholly
     *                                lacking metadata
     */
    public PrimitiveFieldsOnTEDPatcher(boolean alwaysPatchCommonFields) {
        this.alwaysPatchCommonFields = alwaysPatchCommonFields;
    }

    @Override
    public void patch(PartialRecordInfo recordInfo, TopologyEntityDTO entity) {
        // find metadata for primitive fields on TopologyEntityDTO (there is topo function defined)
        List<SearchMetadataMapping> metadataList =
                SearchMetadataUtils.getMetadata(entity.getEntityType(), FieldType.PRIMITIVE);
        if (metadataList.isEmpty() && alwaysPatchCommonFields) {
            metadataList = SearchMetadataUtils.COMMON_PRIMITIVE_METADATA;
        }
        metadataList.stream()
                .filter(m -> m.getTopoFieldFunction() != null)
                .forEach(metadata -> {
                    Optional<Object> fieldValue = metadata.getTopoFieldFunction().apply(entity);
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
    }
}
