package com.vmturbo.group.policy;

import java.util.List;

import com.vmturbo.group.common.InvalidItemException;

/**
 * Exception thrown for invalid policies.
 */
public class InvalidPolicyException extends InvalidItemException {
    /**
     * Constructor.
     *
     * @param errors The errors.
     */
    public InvalidPolicyException(final List<String> errors) {
        super("Policy has " + errors.size() + " errors:\n" + String.join("\n", errors));
    }
}
