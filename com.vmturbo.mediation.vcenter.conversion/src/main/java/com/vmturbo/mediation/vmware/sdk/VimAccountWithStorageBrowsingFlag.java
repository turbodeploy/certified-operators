package com.vmturbo.mediation.vmware.sdk;

import com.vmturbo.platform.sdk.probe.AccountDefinition;
import com.vmturbo.platform.sdk.probe.AccountValue;
import com.vmturbo.platform.sdk.probe.AccountValue.Constraint;

/**
 * Account class which adds EnableStorageBrowsing flag to the VimAccount for use with the
 * VimSdkConversionProbe.
 */
@AccountDefinition
public class VimAccountWithStorageBrowsingFlag extends VimAccount {
    @AccountValue(displayName = "Enable Datastore Browsing",
        description = "Enable datastore browsing for this target.",
        constraint = Constraint.OPTIONAL,
        defaultValue = "true")
    private boolean isStorageBrowsingEnabled;

    public VimAccountWithStorageBrowsingFlag() {
        super();
        isStorageBrowsingEnabled = true;
    }

    public VimAccountWithStorageBrowsingFlag(String address, String username, String password,
                                             boolean storageBrowsingEnabled) {
        super(address, username, password);
        isStorageBrowsingEnabled = storageBrowsingEnabled;
    }

    public boolean isStorageBrowsingEnabled() {
        return isStorageBrowsingEnabled;
    }
}
