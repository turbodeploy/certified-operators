package com.vmturbo.mediation.delegatingprobe;

import java.util.HashMap;
import java.util.Map;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import com.vmturbo.platform.sdk.probe.AccountDefinition;
import com.vmturbo.platform.sdk.probe.AccountValue;

/**
 * Account values for delegating probe.
 */
@AccountDefinition
public class DelegatingProbeAccount {

    @AccountValue(targetId = true, displayName = "Name", description = "Target name")
    final String targetId;
    @AccountValue(displayName = "driverRootUri", description = "The root uri for the driver for this probe. " +
        "The probe will connect to the driver via REST in order to get its discovery results. " +
        "Endpoint is appended to the rootURI to form the complete route. For example: rootUri = 'localhost' and " +
        "endpoint = 'test/discovery' will form the route localhost/test/discovery.")
    final String driverRootUri;
    @AccountValue(displayName = "driverEndpoint", description = "The endpoint of the driver to query for discovery. " +
        "The probe will connect to the driver via REST in order to get its discovery results. " +
        "Endpoint is appended to the rootURI to form the complete route. For example: rootUri = 'localhost' and " +
        "endpoint = 'test/discovery' will form the route localhost/test/discovery.")
    final String driverEndpoint;

    protected DelegatingProbeAccount() {
        this.targetId = null;
        this.driverRootUri = null;
        this.driverEndpoint = null;
    }

    /**
     * Account values for the delegating probe.
     * @param targetId the target ID
     * @param driverRootUri the host address for the driver of this probe. The probe will delegate discovery requests
     *                   to the driver and passthrough the results.
     * @param driverEndpoint the port number for the driver of this probe.
     */
    public DelegatingProbeAccount(String targetId, String driverRootUri, String driverEndpoint) {
        this.targetId = targetId;
        this.driverRootUri = driverRootUri;
        this.driverEndpoint = driverEndpoint;
    }

    public String getTargetId() {
        return targetId;
    }

    public String getDriverRootUri() {
        return driverRootUri;
    }

    public String getDriverEndpoint() {
        return driverEndpoint;
    }

    /**
     * Create a map of field names to field values that define this DelegatingAccount.
     * @return a map of field name -> field value for this DelegatingAccount
     */
    public Map<String, Object> getFieldMap() {
        final HashMap<String, Object> fieldMap = new HashMap<>();

        fieldMap.put("targetId", targetId);
        fieldMap.put("driverRootUri", driverRootUri);
        fieldMap.put("driverEndpoint", driverEndpoint);

        return fieldMap;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof DelegatingProbeAccount)) {
            return false;
        }
        final DelegatingProbeAccount other = (DelegatingProbeAccount)obj;
        return Objects.equal(this.targetId, other.targetId)
                && Objects.equal(this.driverRootUri, other.driverRootUri)
                && Objects.equal(this.driverEndpoint, other.driverEndpoint);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(this.targetId, this.driverRootUri, this.driverEndpoint);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("targetId", targetId)
                .add("driverRootUri", driverRootUri)
                .add("driverEndpoint", driverEndpoint)
                .toString();
    }
}
