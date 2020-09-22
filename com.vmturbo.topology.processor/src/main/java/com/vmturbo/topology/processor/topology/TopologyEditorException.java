package com.vmturbo.topology.processor.topology;

import java.util.Set;

import javax.annotation.Nonnull;

/**
 * Exception throws if there is a error when editing topology.
 */
public class TopologyEditorException extends RuntimeException {
    public TopologyEditorException(String message) {
        super(message);
    }

    public static TopologyEditorException notFoundEntityException(final long oid) {
        return new TopologyEditorException("Can not find entity " + oid + " in topology.");
    }

    public static TopologyEditorException notFoundTemplateException(@Nonnull final Set<Long> oids) {
        return new TopologyEditorException("Can not find templates: " + oids);
    }
}
