package com.vmturbo.extractor.search;

import static com.vmturbo.extractor.models.ModelDefinitions.ENTITY_SEVERITY_ENUM;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.dto.searchquery.FieldApiDTO.FieldType;
import com.vmturbo.extractor.search.SearchEntityWriter.EntityRecordPatcher;
import com.vmturbo.extractor.search.SearchEntityWriter.PartialRecordInfo;
import com.vmturbo.extractor.topology.DataProvider;
import com.vmturbo.search.metadata.SearchEntityMetadata;
import com.vmturbo.search.metadata.SearchEntityMetadataMapping;

/**
 * Add basic fields which are not available in TopologyEntityDTO, like: severity.
 */
public class PrimitiveFieldsNotOnTEDPatcher implements EntityRecordPatcher<DataProvider> {

    private static final Logger logger = LogManager.getLogger();

    @Override
    public void patch(PartialRecordInfo recordInfo, DataProvider dataProvider) {
        // find metadata for primitive fields not on TopologyEntityDTO (no topo function defined)
        final List<SearchEntityMetadataMapping> metadataList =
                SearchEntityMetadata.getMetadata(recordInfo.entityType, FieldType.PRIMITIVE).stream()
                        .filter(m -> m.getTopoFieldFunction() == null)
                        .collect(Collectors.toList());

        metadataList.forEach(metadata -> {
            switch (metadata) {
                case PRIMITIVE_SEVERITY:
                    recordInfo.record.set(ENTITY_SEVERITY_ENUM,
                            dataProvider.getSeverity(recordInfo.oid));
                    break;
                default:
                    logger.error("Unsupported primitive metadata: {}", metadata);
            }
        });
    }
}
