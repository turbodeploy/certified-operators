package com.vmturbo.cost.component.savings.bottomup;

import java.util.Optional;

import com.google.gson.annotations.SerializedName;

import org.immutables.gson.Gson;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Style;
import org.immutables.value.Value.Style.ImplementationVisibility;

import com.vmturbo.cost.component.savings.tem.ProviderInfo;

/**
 * Sole purpose of this class is to help efficiently serialize only the required fields out of
 * SavingsEvent and its underlying fields like ActionEvent and TopologyEvent.
 * Json serialized field names are really short in order to make sure it takes minimum DB space.
 */
@Style(visibility = ImplementationVisibility.PACKAGE, overshadowImplementation = true)
@Gson.TypeAdapters
@Immutable(lazyhash = true)
interface SerializableSavingsEvent {
    @SerializedName("xt")
    Optional<Long> getExpirationTime();
    class Builder extends ImmutableSerializableSavingsEvent.Builder {}

    @SerializedName("ae")
    Optional<SerializableActionEvent> getActionEvent();

    @SerializedName("te")
    Optional<SerializableTopologyEvent> getTopologyEvent();

    @SerializedName("pc")
    Optional<SerializableEntityPriceChange> getEntityPriceChange();

    @Style(visibility = ImplementationVisibility.PACKAGE, overshadowImplementation = true)
    @Gson.TypeAdapters
    @Immutable(lazyhash = true)
    interface SerializableActionEvent {
        @SerializedName("aid")
        long getActionId();

        @SerializedName("at")
        int getActionType();

        @SerializedName("ac")
        int getActionCategory();

        class Builder extends ImmutableSerializableActionEvent.Builder {}
    }

    @Style(visibility = ImplementationVisibility.PACKAGE, overshadowImplementation = true)
    @Gson.TypeAdapters
    @Immutable(lazyhash = true)
    interface SerializableTopologyEvent {

        @SerializedName("pinfo")
        Optional<ProviderInfo> getProviderInfo();

        @SerializedName("er")
        Optional<Boolean> getEntityRemoved();

        class Builder extends ImmutableSerializableTopologyEvent.Builder {}
    }

    @Style(visibility = ImplementationVisibility.PACKAGE, overshadowImplementation = true)
    @Gson.TypeAdapters
    @Immutable(lazyhash = true)
    interface SerializableEntityPriceChange {
        @SerializedName("sc")
        double getSourceCost();

        @SerializedName("dc")
        double getDestinationCost();

        @SerializedName("so")
        long getSourceOid();

        @SerializedName("do")
        long getDestinationOid();

        @SerializedName("act")
        int active();
        class Builder extends ImmutableSerializableEntityPriceChange.Builder {}
    }
}
