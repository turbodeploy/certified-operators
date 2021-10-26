package com.vmturbo.extractor.patchers;

import com.vmturbo.extractor.schema.enums.EntityType;
import com.vmturbo.extractor.schema.enums.EnvironmentType;
import com.vmturbo.extractor.search.SearchEntityWriter.EntityRecordPatcher;
import com.vmturbo.extractor.search.SearchEntityWriter.PartialEntityInfo;
import com.vmturbo.search.metadata.SearchMetadataMapping;
import com.vmturbo.topology.processor.api.util.ThinTargetCache.ThinProbeInfo;
import com.vmturbo.topology.processor.api.util.ThinTargetCache.ThinTargetInfo;

/**
 * Set values in the entity record for a target.
 */
public class TargetPatcher implements EntityRecordPatcher<ThinTargetInfo> {
    @Override
    public void fetch(final PartialEntityInfo recordInfo, final ThinTargetInfo dataProvider) {
        final String targetName = dataProvider.displayName();

        if (dataProvider.probeInfo() != null) {
            ThinProbeInfo probeInfo = dataProvider.probeInfo();
            String targetCategory = probeInfo.category();
            recordInfo.putJsonAttr("category", targetCategory);
            recordInfo.putJsonAttr("type", probeInfo.type());
        }
        recordInfo.putJsonAttr("hidden", dataProvider.isHidden());

        recordInfo.putAttr(SearchMetadataMapping.PRIMITIVE_OID, recordInfo.getOid());
        recordInfo.putAttr(SearchMetadataMapping.PRIMITIVE_ENTITY_TYPE, EntityType.TARGET);
        recordInfo.putAttr(SearchMetadataMapping.PRIMITIVE_NAME, targetName);
        recordInfo.putAttr(SearchMetadataMapping.PRIMITIVE_ENVIRONMENT_TYPE,
                        EnvironmentType.UNKNOWN_ENV);
    }
}
