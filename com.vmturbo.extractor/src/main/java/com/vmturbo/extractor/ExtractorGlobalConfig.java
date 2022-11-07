package com.vmturbo.extractor;

import java.time.Clock;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.components.api.FormattedString;

/**
 * Configuration global to the extractor component, and shared by other configurations.
 */
@Configuration
public class ExtractorGlobalConfig {

    /**
     * Configuration used to enable/disable search data ingestion.
     */
    @Value("${enableSearchApi:false}")
    private boolean enableSearchApi;

    /**
     * Configuration used to enable/disable ingestion.
     *
     * <p/>This is only meaningful if "enableReporting" is true, and is a way to stop action ingestion
     * without disabling the rest of reporting.
     */
    @Value("${enableActionIngestion:true}")
    private boolean enableActionIngestion;

    /**
     * Configuration used to enable/disable reporting data ingestion.
     */
    @Value("${enableReporting:false}")
    private boolean enableReporting;

    /**
     * Configuration used to enable/disable data extraction. Disabled by default.
     */
    @Value("${enableDataExtraction:false}")
    private boolean enableDataExtraction;

    /**
     * Configuration used to enable/disable whitelisting og entity relations.
     */
    @Value("${enableEntityRelationWhitelist:false}")
    private boolean enableEntityRelationWhitelist;

    /**
     * Configuration used to enable/disable top-up billing account cost ingestion.
     */
    @Value("${enableBillingCost:true}")
    private boolean enableBillingCost;

    @Value("${enableIndividualVStorages:true}")
    private boolean enableIndividualVStorages;

    /**
     * The global interval for data exporter (extracting entity, group and action and sending to
     * Kafka. It's not set by default.
     */
    @Value("${globalExtractionIntervalMins:#{null}}")
    public Long globalExtractionIntervalMins;

    @Value("${enableKeysAsValues:false}")
    private boolean enableKeysAsValues;

    /**
     * Clock for the component.
     *
     * @return The clock.
     */
    @Bean
    public Clock clock() {
        return Clock.systemUTC();
    }

    /**
     * The feature flags, which control which extractor features are enabled.
     *
     * @return The {@link ExtractorFeatureFlags} object.
     */
    @Bean
    public ExtractorFeatureFlags featureFlags() {
        return new ExtractorFeatureFlags(enableSearchApi, enableReporting, enableActionIngestion,
                enableDataExtraction, enableEntityRelationWhitelist, enableBillingCost,
                enableIndividualVStorages, enableKeysAsValues);
    }

    /**
     * Return whether or not the extractor needs to connect to Postgres.
     *
     * @return True if we need a database connection.
     */
    public boolean requireDatabase() {
        return featureFlags().isSearchEnabled() || featureFlags().isReportingEnabled();
    }

    /**
     * Captures feature flags that control which extractor functionality is enabled/disabled.
     */
    public static class ExtractorFeatureFlags {
        private final boolean enableSearchApi;
        private final boolean enableReporting;
        private final boolean enableReportActionIngestion;
        private final boolean enableExtraction;
        private final boolean enableEntityRelationWhitelist;

        /**
         * Whether billing cost data collection and reporting is enabled.
         */
        private final boolean enableBillingCost;
        private final boolean enableIndividualVStorages;
        private final boolean enableKeysAsValues;

        private ExtractorFeatureFlags(boolean enableSearchApi, boolean enableReporting,
                boolean enableReportActionIngestion, boolean enableExtraction,
                boolean enableEntityRelationWhitelist, boolean enableBillingCost,
                boolean enableIndividualVStorages, boolean enableKeysAsValues) {
            this.enableSearchApi = enableSearchApi;
            this.enableReporting = enableReporting;
            this.enableReportActionIngestion = enableReportActionIngestion;
            this.enableExtraction = enableExtraction;
            this.enableEntityRelationWhitelist = enableEntityRelationWhitelist;
            this.enableBillingCost = enableBillingCost;
            this.enableIndividualVStorages = enableIndividualVStorages;
            this.enableKeysAsValues = enableKeysAsValues;
        }

        public boolean isSearchEnabled() {
            return enableSearchApi;
        }

        public boolean isReportingEnabled() {
            return enableReporting;
        }

        public boolean isReportingActionIngestionEnabled() {
            return enableReporting && enableReportActionIngestion;
        }

        public boolean isExtractionEnabled() {
            return enableExtraction;
        }

        public boolean isEntityRelationWhitelistEnabled() {
            return enableEntityRelationWhitelist;
        }

        public boolean isBillingCostEnabled() {
            return enableBillingCost;
        }

        public boolean isBillingCostReportingEnabled() {
            return isBillingCostEnabled() && isReportingEnabled();
        }

        public boolean isIndividualVStoragesEnabled() {
            return enableIndividualVStorages;
        }

        /**
         * Keys as values enabled.
         *
         * @return if keys as values is true
         */
        public boolean enableKeysAsValues() {
            return enableKeysAsValues;
        }

        @Override
        public String toString() {
            return FormattedString.format("Flags:\n"
                            + "Report Ingestion: {}\n"
                            + "Report Action Ingestion {}\n"
                            + "Search Ingestion {}\n"
                            + "Data Extraction {}\n"
                            + "Entity Relation Whitelist {}\n"
                            + "Billing Cost Ingestion {}\n"
                            + "Individual vStorage metrics {}", isReportingEnabled(),
                    isReportingActionIngestionEnabled(), isSearchEnabled(), isExtractionEnabled(),
                    isEntityRelationWhitelistEnabled(), isBillingCostEnabled(),
                    isIndividualVStoragesEnabled(), enableKeysAsValues());
        }
    }
}
