package com.vmturbo.market.settings;

/**
 * Market settings.
 *
 */
public class MarketSettings {

    private MarketSettings() {}

    /**
     * Market settings with boolean values.
     */
    public enum BooleanKey {

        INCLUDE_GUARANTEED_BUYER(false),
        PROVISIONED_ENABLED(true),
        RIGHTSIZE_ENABLED(true);

        private boolean defaultValue;

        BooleanKey(boolean val) {
            defaultValue = val;
        }

        public boolean value() {
            return defaultValue;
        }
    }

    /**
     * Market settings with numerical values.
     */
    public enum NumericKey {

        RIGHTSIZE_LOWER(0.7f),
        RIGHTSIZE_UPPER(0.8f);

        private float defaultValue;

        NumericKey(float val) {
            defaultValue = val;
        }

        public float defaultValue() {
            return defaultValue;
        }
    }
}
