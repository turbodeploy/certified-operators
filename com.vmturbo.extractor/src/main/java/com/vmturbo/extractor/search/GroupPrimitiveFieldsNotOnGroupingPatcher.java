package com.vmturbo.extractor.search;

import static com.vmturbo.extractor.models.ModelDefinitions.SEVERITY_ENUM;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.dto.searchquery.FieldApiDTO.FieldType;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.extractor.search.SearchEntityWriter.EntityRecordPatcher;
import com.vmturbo.extractor.search.SearchEntityWriter.PartialRecordInfo;
import com.vmturbo.extractor.topology.DataProvider;
import com.vmturbo.search.metadata.SearchMetadataMapping;

/**
 * Add basic fields which are NOT available on {@link Grouping}, like severity, etc.
 */
public class GroupPrimitiveFieldsNotOnGroupingPatcher implements EntityRecordPatcher<DataProvider> {

    private static final Logger logger = LogManager.getLogger();

    @Override
    public void patch(PartialRecordInfo recordInfo, DataProvider dataProvider) {
        // find metadata for primitive fields NOT on Grouping
        final List<SearchMetadataMapping> metadataList =
                SearchMetadataUtils.getMetadata(recordInfo.groupType, FieldType.PRIMITIVE).stream()
                        .filter(m -> m.getGroupFieldFunction() == null)
                        .collect(Collectors.toList());

        metadataList.forEach(metadata -> {
            switch (metadata) {
                case PRIMITIVE_SEVERITY:
                    recordInfo.record.set(SEVERITY_ENUM,
                            dataProvider.getSeverity(recordInfo.oid));
                    break;
                default:
                    logger.error("Unsupported group primitive metadata: {}", metadata);
            }
        });
    }
}
