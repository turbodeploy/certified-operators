package com.vmturbo.market.settings;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;

/**
 * Commodity settings.
 *
 */
public class CommoditySettings {

    private CommoditySettings() {}

    /**
     * Commodity settings with boolean values.
     */
    public enum BooleanKey {

        PLACEHOLDER(false); // remove when boolean keys are added

        private boolean defaultValue;

        BooleanKey(boolean val) {
            defaultValue = val;
        }

        public boolean value(CommodityType commType) {
            return defaultValue;
        }

    }

    /**
     * Commodity settings with numerical values.
     */
    public enum NumericKey {

        PLACEHOLDER(0.2f); // remove when numeric keys are added

        private float defaultValue;

        NumericKey(float val) {
            defaultValue = val;
        }

        public float value(CommodityType commType) {
            return defaultValue;
        }

    }
}
