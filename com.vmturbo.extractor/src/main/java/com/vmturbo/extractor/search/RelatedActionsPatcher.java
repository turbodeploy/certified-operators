package com.vmturbo.extractor.search;

import static com.vmturbo.extractor.models.ModelDefinitions.NUM_ACTIONS;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.dto.searchquery.FieldApiDTO.FieldType;
import com.vmturbo.extractor.search.SearchEntityWriter.EntityRecordPatcher;
import com.vmturbo.extractor.search.SearchEntityWriter.PartialRecordInfo;
import com.vmturbo.extractor.topology.DataProvider;
import com.vmturbo.search.metadata.SearchEntityMetadata;

/**
 * Add related action data (only action count for now).
 */
public class RelatedActionsPatcher implements EntityRecordPatcher<DataProvider> {

    private static final Logger logger = LogManager.getLogger();

    @Override
    public void patch(PartialRecordInfo recordInfo, DataProvider dataProvider) {
        SearchEntityMetadata.getMetadata(recordInfo.entityType, FieldType.RELATED_ACTION)
                .forEach(metadata -> {
            // currently we only support action count, so there is supposed to be only ONE metadata
            switch (metadata) {
                case RELATED_ACTION:
                    recordInfo.record.set(NUM_ACTIONS, dataProvider.getActionCount(recordInfo.oid));
                    break;
                default:
                    logger.error("Unsupported related action metadata: {}", metadata);
            }
        });
    }
}
