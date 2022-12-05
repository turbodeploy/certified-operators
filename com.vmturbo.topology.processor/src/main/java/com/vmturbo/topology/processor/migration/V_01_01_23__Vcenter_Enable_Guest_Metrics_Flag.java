package com.vmturbo.topology.processor.migration;

import java.util.Set;

import javax.annotation.Nonnull;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;

/**
 * This migration updates vCenters targets' info by setting the `guestMetricsEnabled` to true when
 * the current value is `false`.
 */
public class V_01_01_23__Vcenter_Enable_Guest_Metrics_Flag extends
        AbstractTargetForProbesMigration {

    static final String GUEST_METRICS = "guestMetricsEnabled";

    /**
     * Constructor.
     *
     * @param keyValueStore keyValue Store.
     */
    V_01_01_23__Vcenter_Enable_Guest_Metrics_Flag(@Nonnull final KeyValueStore keyValueStore, @Nonnull Set<SDKProbeType> scopedProbeTypes) {
        super(keyValueStore, scopedProbeTypes);
    }

    @Override
    String migrationPurpose() {
        return "Enable discovery of Guest metrics by the vCenter probe for targets that have it disabled";
    }

    @Override
    void updateProbe(String probeId, String probeType, JsonObject probeJson,
            String consulProbeKey) {}

    boolean processTargetInfoDiscoveredByProbe(JsonObject targetInfoObject) {
        final JsonObject spec = targetInfoObject.get(TARGET_SPEC).getAsJsonObject();
        final String targetId = targetInfoObject.get(TARGET_ID).getAsString();
        final String targetDisplayName = targetInfoObject.get(TARGET_NAME).getAsString();
        logger.info("Migrating guest metrics for vCenter target {} ({})", targetDisplayName, targetId);

        if (!spec.has(ACCOUNT_VALUE)) {
            logger.warn("Target {} spec doesn't have accountValue list", targetId);
            return false;
        }

        // Find the index of the account value that represents the guest metrics flag
        final JsonArray accountValues = spec.getAsJsonArray(ACCOUNT_VALUE);
        Integer guestMetricsIndex = null;
        int i = 0;
        for (JsonElement value : accountValues) {
            final JsonObject accV = value.getAsJsonObject();
            final JsonElement accKey = accV.get(ACCOUNT_VALUE_KEY);
            if (GUEST_METRICS.equals(accKey.getAsString())) {
                guestMetricsIndex = i;
                logger.debug("Guest metric flag index {}", guestMetricsIndex);
                break;
            }
            i++;
        }

        // If we do not have an account value corresponding to the guest metrics flag, there is nothing
        // to do since the default has already been changed to be true on the probe
        if (guestMetricsIndex == null) {
            logger.info(
                    "Guest metric flag not found in account values for target {} ({}). Adding guest metric account value and setting it to true.",
                    targetDisplayName, targetId);
            final JsonObject newGuestMetric = new JsonObject();
            newGuestMetric.addProperty(ACCOUNT_VALUE_KEY, GUEST_METRICS);
            newGuestMetric.addProperty(ACCOUNT_VALUE_VALUE, "true");
            accountValues.add(newGuestMetric);
            return true;
        }

        // Change the current set guest metric value to true if it is false. Otherwise, there is nothing to do
        final JsonObject guestMetricAccount = accountValues.get(guestMetricsIndex).getAsJsonObject();
        final boolean guestMetricEnabled = guestMetricAccount.getAsJsonPrimitive(ACCOUNT_VALUE_VALUE).getAsBoolean();
        if (guestMetricEnabled) {
            logger.info("Guest metric flag already enabled for target {} ({})", targetDisplayName, targetId);
            return false;
        }
        logger.info("Enabling guest metrics discovery for target {} ({})", targetDisplayName, targetId);
        guestMetricAccount.addProperty(ACCOUNT_VALUE_VALUE, "true");
        return true;
    }
}