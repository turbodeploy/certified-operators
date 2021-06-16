package com.vmturbo.extractor.patchers;

import static com.vmturbo.extractor.models.ModelDefinitions.ENTITY_NAME;
import static com.vmturbo.extractor.models.ModelDefinitions.ENTITY_OID_AS_OID;
import static com.vmturbo.extractor.models.ModelDefinitions.ENTITY_TYPE_AS_TYPE_ENUM;
import static com.vmturbo.extractor.models.ModelDefinitions.ENVIRONMENT_TYPE_ENUM;

import java.util.HashMap;
import java.util.Map;

import com.vmturbo.extractor.models.Table.Record;
import com.vmturbo.extractor.schema.enums.EntityType;
import com.vmturbo.extractor.schema.enums.EnvironmentType;
import com.vmturbo.extractor.search.SearchEntityWriter.EntityRecordPatcher;
import com.vmturbo.extractor.search.SearchEntityWriter.PartialRecordInfo;
import com.vmturbo.topology.processor.api.util.ThinTargetCache.ThinProbeInfo;
import com.vmturbo.topology.processor.api.util.ThinTargetCache.ThinTargetInfo;

/**
 * Set values in the entity record for a target.
 */
public class TargetPatcher implements EntityRecordPatcher<ThinTargetInfo> {

    // Attribute names used in the json string for the ATTR column
    private static final String TARGET_ATTR_CATEGORY = "category";
    private static final String TARGET_ATTR_TYPE = "type";
    private static final String TARGET_ATTR_HIDDEN = "hidden";

    @Override
    public void patch(final PartialRecordInfo recordInfo, final ThinTargetInfo dataProvider) {
        final String targetName = dataProvider.displayName();
        Map<String, Object> attrs = new HashMap<>();

        if (dataProvider.probeInfo() != null) {
            ThinProbeInfo probeInfo = dataProvider.probeInfo();
            String targetCategory = probeInfo.category();
            attrs.put(TARGET_ATTR_CATEGORY, targetCategory);
            attrs.put(TARGET_ATTR_TYPE, probeInfo.type());
        }
        attrs.put(TARGET_ATTR_HIDDEN, dataProvider.isHidden());

        Record record = recordInfo.getRecord();
        record.set(ENTITY_OID_AS_OID, recordInfo.getOid());
        record.set(ENTITY_TYPE_AS_TYPE_ENUM, EntityType.TARGET);
        record.set(ENTITY_NAME, targetName);
        record.set(ENVIRONMENT_TYPE_ENUM, EnvironmentType.UNKNOWN_ENV);
        recordInfo.putAllAttrs(attrs);
    }
}
