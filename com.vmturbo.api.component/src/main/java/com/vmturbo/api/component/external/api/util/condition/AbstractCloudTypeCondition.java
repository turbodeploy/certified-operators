package com.vmturbo.api.component.external.api.util.condition;

/**
 * Abstract class for Cloud type condition. It's to support different logical condition combination.
 * Currently only support OR (||) condition. E.g. condition support AWS || Azure
 */
public abstract class AbstractCloudTypeCondition implements CloudTypeCondition {
    public CloudTypeCondition or(CloudTypeCondition other) {
        return new OrCloudTypeCondition(this, other);
    }
}
