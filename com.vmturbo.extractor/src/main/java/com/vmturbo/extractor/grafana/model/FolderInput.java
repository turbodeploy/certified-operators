package com.vmturbo.extractor.grafana.model;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.annotations.SerializedName;

/**
 * See: https://grafana.com/docs/grafana/latest/http_api/folder/#create-folder.
 */
public class FolderInput {
    @SerializedName("uid")
    private String uid;

    @SerializedName("title")
    private String title;

    /**
     * The uid of the folder.
     * See: https://grafana.com/docs/grafana/latest/http_api/folder/#identifier-id-vs-unique-identifier-uid.
     *
     * @return The UID.
     */
    public String getUid() {
        return uid;
    }

    /**
     * The title for the folder - i.e. the display name.
     *
     * @return The title.
     */
    public String getTitle() {
        return title;
    }

    /**
     * Set title for the folder - i.e. the display name.
     * @param title title to set
     */
    public void setTitle(String title) {
        this.title = title;
    }

    /**
     * Set uid of folder.
     * @param uid uid to use for folder
     */
    @VisibleForTesting
    public void setUid(String uid) {
        this.uid = uid;
    }
}
