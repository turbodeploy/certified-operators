package com.vmturbo.extractor.topology;

import static com.vmturbo.extractor.models.ModelDefinitions.ENTITY_HASH_AS_HASH;
import static com.vmturbo.extractor.models.ModelDefinitions.ENTITY_OID_AS_OID;
import static com.vmturbo.extractor.models.ModelDefinitions.FIRST_SEEN;
import static com.vmturbo.extractor.models.ModelDefinitions.LAST_SEEN;

import java.util.Set;

import com.google.common.collect.ImmutableSet;

import com.vmturbo.extractor.RecordHashManager;
import com.vmturbo.extractor.models.Column;
import com.vmturbo.extractor.models.ModelDefinitions;

/**
 * A {@link RecordHashManager} which is responsible for assigning and tracking hashes and first/last
 * seen timestamps for entity records.
 */
public class EntityHashManager extends RecordHashManager {
    private static final Set<Column<?>> INCLUDE_COLUMNS_FOR_ENTITY_HASH = ImmutableSet.of(
            ENTITY_OID_AS_OID,
            ModelDefinitions.ENTITY_NAME,
            ModelDefinitions.ENTITY_TYPE_ENUM,
            ModelDefinitions.ENTITY_STATE_ENUM,
            ModelDefinitions.ENVIRONMENT_TYPE_ENUM,
            ModelDefinitions.ATTRS,
            ModelDefinitions.SCOPED_OIDS);

    EntityHashManager(WriterConfig config) {
        super(INCLUDE_COLUMNS_FOR_ENTITY_HASH,
                ENTITY_OID_AS_OID,
                ENTITY_HASH_AS_HASH,
                FIRST_SEEN,
                LAST_SEEN,
                config);
    }

}
