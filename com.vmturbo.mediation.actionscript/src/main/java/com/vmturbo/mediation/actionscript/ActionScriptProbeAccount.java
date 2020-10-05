package com.vmturbo.mediation.actionscript;

import static com.vmturbo.platform.sdk.probe.AccountValue.Constraint.MANDATORY;
import static com.vmturbo.platform.sdk.probe.AccountValue.Constraint.OPTIONAL;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import com.vmturbo.platform.sdk.probe.AccountDefinition;
import com.vmturbo.platform.sdk.probe.AccountValue;

/**
 * Account values for ActionScript probe.
 */
@AccountDefinition
public class ActionScriptProbeAccount {

    private final static String DEFAULT_PORT = "22";

    @AccountValue(targetId = true, displayName = "Name or Address", constraint = MANDATORY,
        description = "IP or FQDNS for the Script Execution Server")
    final String nameOrAddress;

    @AccountValue(displayName = "Port", constraint = OPTIONAL, defaultValue = DEFAULT_PORT,
        description = "Port to use for the Script Execution Server")
    final String port;

    @AccountValue(displayName = "User ID",
        description = "Userid to use to execute command on the Script Execution Server")
    final String userid;

    @AccountValue(displayName = "Private Token", constraint = MANDATORY, secret = true,
        multiline = true,
        description = "SSH Private Token corresponding to the Userid")
    final String privateKeyString;

    @AccountValue(targetId = true, displayName = "Script Path", constraint = MANDATORY,
        description = "File Path to the Action Script manifest file on the Execution Server")
    final String manifestPath;

    @AccountValue(displayName = "Public Host Key", constraint = OPTIONAL,
        description = "Public key presented by the SSH server for host authenticaion; "
            + "if not provided, the presented key will be accepted and integrated into the target definition for future operations")
    final String hostKey;

    /**
     * Default, no-args constructor is required by the vmturbo-sdk-plugin during the build process.
     */
    public ActionScriptProbeAccount() {
        nameOrAddress = null;
        port = null;
        userid = null;
        privateKeyString = null;
        manifestPath = null;
        hostKey = null;
    }

    /**
     * Account values for the ActionScript probe. As the ActionScript probe will be automatically
     * created, and hidden, there are no authentication values required for this probe.
     *
     * @param nameOrAddress IP or FQDN for the server on which to discover the scripts
     * @param userid to use to access the ActionScript server
     * @param privateKeyString SSH private key corresponding to the {@code userid}
     * @param manifestPath filesystem path to the directory containing the scripts on the ActionScript
     *                   server
     */
    public ActionScriptProbeAccount(@Nonnull String nameOrAddress,
                                    @Nonnull String userid,
                                    @Nonnull String privateKeyString,
                                    @Nonnull String manifestPath) {
        this(nameOrAddress, userid, privateKeyString, manifestPath, DEFAULT_PORT, null);
    }

    /**
     * Account values for the ActionScript probe. As the ActionScript probe will be automatically
     * created, and hidden, there are no authentication values required for this probe.
     *
     * @param nameOrAddress IP or FQDN for the server on which to discover the scripts
     * @param userid to use to access the ActionScript server
     * @param privateKeyString SSH private key corresponding to the {@code userid}
     * @param manifestPath filesystem path to the directory containing the scripts on the ActionScript
     *                   server
     * @param port to use to access the ActionScript server
     * @param hostKey public key expected from host during authentication, or null to accept (and retain)
     *                whatever key is presented
     */
    public ActionScriptProbeAccount(@Nonnull String nameOrAddress,
                                    @Nonnull String userid,
                                    @Nonnull String privateKeyString,
                                    @Nonnull String manifestPath,
                                    @Nonnull String port,
                                    @Nullable String hostKey) {
        this.nameOrAddress = nameOrAddress;
        this.userid = userid;
        this.privateKeyString = privateKeyString;
        this.manifestPath = manifestPath;
        this.port = port;
        this.hostKey = hostKey;
    }

    public String getNameOrAddress() {
        return nameOrAddress;
    }

    public String getPort() {
        return port;
    }

    public String getUserid() {
        return userid;
    }

    public String getPrivateKeyString() {
        return privateKeyString;
    }

    public String getManifestPath() {
        return manifestPath;
    }

    public String getHostKey() { return hostKey; }

    /**
     * Create a map of field names to field values that define this ActionScriptProbeAccount.
     * @return a map of field name -> field value for this ActionScriptProbeAccount
     */
    public Map<String, Object> getFieldMap() {
        final HashMap<String, Object> fieldMap = new HashMap<>();

        fieldMap.put("nameOrAddress", nameOrAddress);
        fieldMap.put("port", port);
        fieldMap.put("userid", userid);
        fieldMap.put("privateKey", privateKeyString);
        fieldMap.put("manifestPath", manifestPath);
        fieldMap.put("hostKey", hostKey);

        return fieldMap;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ActionScriptProbeAccount)) {
            return false;
        }
        final ActionScriptProbeAccount other = (ActionScriptProbeAccount)obj;
        return Objects.equal(this.nameOrAddress, other.nameOrAddress) &&
            Objects.equal(this.port, other.port) &&
            Objects.equal(this.userid, other.userid) &&
            Objects.equal(this.privateKeyString, other.privateKeyString) &&
            Objects.equal(this.manifestPath, other.manifestPath) &&
            Objects.equal(this.hostKey, other.hostKey);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(this.nameOrAddress,
            this.port,
            this.userid,
            this.privateKeyString,
            this.manifestPath,
            this.hostKey);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("nameOrAddress", nameOrAddress)
            .add("port", port)
            .add("userid", userid)
            .add("manifestPath", manifestPath)
            .add("hostKey", hostKey)
            .toString();
    }
}
