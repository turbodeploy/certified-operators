package com.vmturbo.extractor.grafana.model;

import java.util.Optional;

import javax.annotation.Nonnull;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;

import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.api.FormattedString;

/**
 * A folder that exists on the server. The same as {@link FolderInput} with extra fields that are
 * unique to particular folder instances.
 *
 * <p/>See: https://grafana.com/docs/grafana/latest/http_api/folder/.
 */
public class Folder extends FolderInput {
    @SerializedName("id")
    private long id;

    @SerializedName("version")
    private int version;

    /**
     * Get the last version of the folder.
     *
     * @return The version.
     */
    public int getVersion() {
        return version;
    }

    /**
     * Get the ID of the folder.
     * @return The folder id.
     */
    public long getId() {
        return id;
    }

    @Override
    public String toString() {
        return FormattedString.format("{} (id: {}, uid: {})", getTitle(), id, getUid());
    }

    /**
     * Apply a new {@link FolderInput} to this folder, returning the updated
     * {@link Folder} object if there are any differences.
     *
     * @param newInput The new {@link FolderInput}.
     * @return If applying the new input changes this folder, returns the new {@link Folder}.
     *         Otherwise, returns an empty optional.
     */
    @Nonnull
    public Optional<Folder> applyInput(FolderInput newInput) {
        if (newInput.getUid().equals(getUid()) && newInput.getTitle().equals(getTitle())) {
            return Optional.empty();
        } else {
            final Gson gson = ComponentGsonFactory.createGson();
            final Folder clone = gson.fromJson(gson.toJson(newInput), Folder.class);
            clone.id = id;
            // The version is the current version of the folder on the server.
            clone.version = version;
            return Optional.of(clone);
        }
    }
}
