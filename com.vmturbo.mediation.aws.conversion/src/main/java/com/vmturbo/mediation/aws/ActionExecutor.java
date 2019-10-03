package com.vmturbo.mediation.aws;

import javax.annotation.Nonnull;

import com.vmturbo.mediation.aws.control.AwsActionExecutor;
import com.vmturbo.platform.sdk.probe.properties.IPropertyProvider;

/**
 * Register executors for allowed actions.
 */
public class ActionExecutor extends AwsActionExecutor {

    static {
        registerActionExecutor(new MoveVolumeExecutor());
    }

    /**
     * Constructor to register all classic-defined executors.
     * @param propertyProvider used by parent class
     */
    public ActionExecutor(@Nonnull final IPropertyProvider propertyProvider) {
        super(propertyProvider);
    }
}
