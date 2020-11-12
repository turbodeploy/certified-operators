package com.vmturbo.mediation.actionstream.kafka;

import static com.vmturbo.platform.sdk.probe.AccountValue.Constraint.MANDATORY;
import static com.vmturbo.platform.sdk.probe.AccountValue.Constraint.OPTIONAL;

import javax.annotation.Nonnull;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import com.vmturbo.platform.sdk.probe.AccountDefinition;
import com.vmturbo.platform.sdk.probe.AccountValue;

/**
 * Account values for the Kafka probe.
 */
@AccountDefinition
public class ActionStreamKafkaProbeAccount {

    private static final String DEFAULT_PORT = "9092";

    @AccountValue(targetId = true, displayName = "Name or Address", constraint = MANDATORY,
        description = "IP or FQDNS for Kafka")
    private final String nameOrAddress;

    @AccountValue(targetId = true, displayName = "Port", constraint = OPTIONAL, defaultValue = DEFAULT_PORT,
        description = "Port to use for Kafka")
    private final String port;

    @AccountValue(targetId = true, displayName = "Topic", constraint = MANDATORY,
        description = "The kafka topic to send action events to")
    private final String topic;

    /**
     * Default, no-args constructor is required by the vmturbo-sdk-plugin during the build process.
     */
    public ActionStreamKafkaProbeAccount() {
        nameOrAddress = null;
        port = null;
        topic = null;
    }

    /**
     * Account values for the Kafka probe.
     *
     * @param nameOrAddress IP or FQDN for the Kafka server we forward the actions to.
     * @param port to use to access the Kafka server.
     * @param topic the kafka topic to send action events to.
     */
    public ActionStreamKafkaProbeAccount(
            @Nonnull String nameOrAddress,
            @Nonnull String port,
            @Nonnull String topic) {
        this.nameOrAddress = nameOrAddress;
        this.port = port;
        this.topic = topic;
    }

    public String getNameOrAddress() {
        return nameOrAddress;
    }

    public String getPort() {
        return port;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ActionStreamKafkaProbeAccount)) {
            return false;
        }
        final ActionStreamKafkaProbeAccount other = (ActionStreamKafkaProbeAccount)obj;
        return Objects.equal(this.nameOrAddress, other.nameOrAddress)
            && Objects.equal(this.port, other.port);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(this.nameOrAddress,
            this.port);
    }

    @Override
    public String toString() {
        // Make sure you do not place any customer secrets in toString()!!!
        return MoreObjects.toStringHelper(this)
            .add("nameOrAddress", nameOrAddress)
            .add("port", port)
            .add("topic", topic)
            .toString();
    }
}
