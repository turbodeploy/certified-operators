package com.vmturbo.mediation.actionscript;

import java.util.HashMap;
import java.util.Map;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import com.vmturbo.platform.sdk.probe.AccountDefinition;
import com.vmturbo.platform.sdk.probe.AccountValue;

/**
 * Account values for ActionScript probe.
 */
@AccountDefinition
public class ActionScriptProbeAccount {

    @AccountValue(targetId=true, displayName="Name",
        description = "Name of the ActionScript Probe")
    final String name;

    protected ActionScriptProbeAccount() {
        this.name = null;
    }

    /**
     * Account values for the ActionScript probe. As the ActionScript probe will be automatically
     * created, and hidden, there are no authentication values required for this probe.
     *
     * @param name the name for the actionscript probe. as the probe will be auto-created, this
     *             name will not be important.
     */
    public ActionScriptProbeAccount(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    /**
     * Create a map of field names to field values that define this ActionScriptProbeAccount.
     * @return a map of field name -> field value for this ActionScriptProbeAccount
     */
    public Map<String, Object> getFieldMap() {
        final HashMap<String, Object> fieldMap = new HashMap<>();

        fieldMap.put("name", name);

        return fieldMap;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ActionScriptProbeAccount)) {
            return false;
        }
        final ActionScriptProbeAccount other = (ActionScriptProbeAccount)obj;
        return Objects.equal(this.name, other.name);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(this.name);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("name", name)
                .toString();
    }
}
