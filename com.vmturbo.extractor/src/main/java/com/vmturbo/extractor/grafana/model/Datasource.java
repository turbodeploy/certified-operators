package com.vmturbo.extractor.grafana.model;

import java.util.Optional;

import javax.annotation.Nonnull;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.annotations.SerializedName;

import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.api.FormattedString;

/**
 * Represents a data source. This is basically the same as the {@link DatasourceInput}, but
 * represents a source that exists on the server and has an id.
 *
 * <p/>See: https://grafana.com/docs/grafana/latest/http_api/data_source/
 */
public class Datasource extends DatasourceInput {

    @SerializedName("id")
    private long id;

    /**
     * Get the ID of the datasource.
     *
     * @return The id.
     */
    public long getId() {
        return id;
    }

    /**
     * Apply a new {@link DatasourceInput} to this datasource, returning the updated
     * {@link Datasource} object if there are any differences.
     *
     * @param newInput The new {@link DatasourceInput}.
     * @return If applying the new input changes this datasource, returns the new {@link Datasource}.
     *         Otherwise, returns an empty optional.
     */
    @Nonnull
    public Optional<Datasource> applyInput(DatasourceInput newInput) {
        final Gson gson = ComponentGsonFactory.createGson();

        // Being a bit lazy, and just using JSON comparison, since these are objects that are
        // meant to be serialized to JSON and sent to Grafana. This is something we do
        // once, at startup, so it's okay if it's expensive.
        final JsonObject thisObj = gson.toJsonTree(this).getAsJsonObject();
        thisObj.remove("id");

        final JsonObject inputObj = gson.toJsonTree(newInput).getAsJsonObject();
        if (!thisObj.equals(inputObj)) {
            final Datasource newDs = gson.fromJson(inputObj, Datasource.class);
            // Retain the ID.
            newDs.id = id;
            return Optional.of(newDs);
        } else {
            return Optional.empty();
        }
    }

    @Override
    public String toString() {
        return FormattedString.format("{} (id: {})", getDisplayName(), id);
    }
}
