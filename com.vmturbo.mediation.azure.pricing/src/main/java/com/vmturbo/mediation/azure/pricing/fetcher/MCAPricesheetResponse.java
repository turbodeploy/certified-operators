package com.vmturbo.mediation.azure.pricing.fetcher;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

/**
 * The response to a price sheet download request.
 */
@Immutable
public class MCAPricesheetResponse {
    private String errorMessage;
    private PublishedEntity publishedEntity;

    /**
     * Create a price sheet download request response.
     *
     * @param errorMessage The error message, if any
     * @param publishedEntity describes the data for download
     */
    public MCAPricesheetResponse(String errorMessage, PublishedEntity publishedEntity) {
        this.errorMessage = errorMessage;
        this.publishedEntity = publishedEntity;
    }

    /**
     * Get the error message, if any.
     *
     * @return the error message, if any.
     */
    @Nullable
    public String getErrorMessage() {
        return errorMessage;
    }

    /**
     * Get the "Published Entity", which describes the download url.
     *
     * @return the entity
     */
    @Nullable
    public PublishedEntity getPublishedEntity() {
        return publishedEntity;
    }

    /**
     * Describes a "published entity" such as a price sheet download.
     */
    public static class PublishedEntity {
        private Properties properties;

        /**
         * Create a "Published Entity".
         *
         * @param properties the properties of the entity.
         */
        public PublishedEntity(Properties properties) {
            this.properties = properties;
        }

        /**
         * The properties of a "published entity".
         *
         * @return the properties
         */
        public Properties getProperties() {
            return properties;
        }

        /**
         * Properties of a published entity.
         */
        public static class Properties {
            private String downloadUrl;

            /**
             * Create entities of a published entity.
             *
             * @param downloadUrl the URL for downloading the data
             */
            public Properties(String downloadUrl) {
                this.downloadUrl = downloadUrl;
            }

            /**
             * Get the download URL for the data.
             *
             * @return the download URL
             */
            public String getDownloadUrl() {
                return downloadUrl;
            }
        }
    }
}
