package com.vmturbo.extractor.grafana.model;

import com.google.gson.annotations.SerializedName;

/**
 * The response returned by Grafana to a POST to the datasource endpoint.
 *
 * <p/>See: https://grafana.com/docs/grafana/latest/http_api/data_source/#create-a-data-source.
 */
public class CreateDatasourceResponse {

    @SerializedName("datasource")
    private Datasource datasource;

    @SerializedName("message")
    private String message;

    /**
     * Get the {@link Datasource} in the response.
     *
     * @return The {@link Datasource}.
     */
    public Datasource getDatasource() {
        return datasource;
    }

    /**
     * Get the message in the response.
     *
     * @return The message.
     */
    public String getMessage() {
        return message;
    }
}
