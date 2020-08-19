package com.vmturbo.extractor.patchers;

import static com.vmturbo.extractor.models.ModelDefinitions.NUM_ACTIONS;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.dto.searchquery.FieldApiDTO.FieldType;
import com.vmturbo.extractor.search.SearchEntityWriter.EntityRecordPatcher;
import com.vmturbo.extractor.search.SearchEntityWriter.PartialRecordInfo;
import com.vmturbo.extractor.search.SearchMetadataUtils;
import com.vmturbo.extractor.topology.DataProvider;

/**
 * Add related action data (only action count for now) for entity.
 */
public class RelatedActionsPatcher implements EntityRecordPatcher<DataProvider> {

    private static final Logger logger = LogManager.getLogger();

    @Override
    public void patch(PartialRecordInfo recordInfo, DataProvider dataProvider) {
        SearchMetadataUtils.getMetadata(recordInfo.getEntityType(), FieldType.RELATED_ACTION)
                .forEach(metadata -> {
            // currently we only support action count, so there is supposed to be only ONE metadata
            switch (metadata) {
                case RELATED_ACTION_COUNT:
                    recordInfo.getRecord().set(NUM_ACTIONS, dataProvider.getActionCount(recordInfo.getOid()));
                    break;
                default:
                    logger.error("Unsupported related action metadata: {}", metadata);
            }
        });
    }
}
