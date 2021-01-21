package com.vmturbo.extractor;

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
     * The feature flags, which control which extractor features are enabled.
     *
     * @return The {@link ExtractorFeatureFlags} object.
     */
    @Bean
    public ExtractorFeatureFlags featureFlags() {
        return new ExtractorFeatureFlags(enableSearchApi,
                enableReporting,
                enableActionIngestion,
                enableDataExtraction);
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

        private ExtractorFeatureFlags(boolean enableSearchApi, boolean enableReporting,
                boolean enableReportActionIngestion, boolean enableExtraction) {
            this.enableSearchApi = enableSearchApi;
            this.enableReporting = enableReporting;
            this.enableReportActionIngestion = enableReportActionIngestion;
            this.enableExtraction = enableExtraction;
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

        @Override
        public String toString() {
            return FormattedString.format("Flags:\n"
                + "Report Ingestion: {}\n"
                + "Report Action Ingestion {}\n"
                + "Search Ingestion {}\n"
                + "Data Extraction {}", isReportingEnabled(),
                    isReportingActionIngestionEnabled(), isSearchEnabled(), isExtractionEnabled());
        }
    }
}
