package com.vmturbo.topology.processor.identity;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Exception thrown when a probe provides no identity metadata for an entity that it sends.
 */
public class IdentityMetadataMissingException extends Exception {

    public IdentityMetadataMissingException(final long probeId, final EntityType entityType) {
        super("Probe " + probeId + " sends entities of type " + entityType + " but provides no related " +
            "identity metadata.");
    }

}
