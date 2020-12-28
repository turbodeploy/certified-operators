package com.vmturbo.extractor.action;

import java.util.Set;

import com.google.common.collect.ImmutableSet;

import com.vmturbo.extractor.RecordHashManager;
import com.vmturbo.extractor.models.ActionModel;
import com.vmturbo.extractor.models.Column;
import com.vmturbo.extractor.topology.WriterConfig;

/**
 * A {@link RecordHashManager} which is responsible for assigning and tracking hashes and first/last
 * seen timestamps for {@link com.vmturbo.extractor.schema.tables.ActionSpec} records.
 */
public class ActionHashManager extends RecordHashManager {
    private static final Set<Column<?>> INCLUDE_COLUMNS_FOR_ACTION_HASH = ImmutableSet.of(
            ActionModel.ActionSpec.SPEC_OID,
            ActionModel.ActionSpec.TYPE,
            ActionModel.ActionSpec.CATEGORY,
            ActionModel.ActionSpec.TARGET_ENTITY,
            ActionModel.ActionSpec.INVOLVED_ENTITIES,
            ActionModel.ActionSpec.DESCRIPTION,
            ActionModel.ActionSpec.SAVINGS);

    ActionHashManager(WriterConfig writerConfig) {
        super(INCLUDE_COLUMNS_FOR_ACTION_HASH,
                ActionModel.ActionSpec.SPEC_OID,
                ActionModel.ActionSpec.HASH,
                ActionModel.ActionSpec.FIRST_SEEN,
                ActionModel.ActionSpec.LAST_SEEN,
                writerConfig);
    }

}
