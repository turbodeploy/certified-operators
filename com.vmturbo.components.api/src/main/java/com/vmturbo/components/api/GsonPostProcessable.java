package com.vmturbo.components.api;

import com.google.gson.Gson;

/**
 * An object that requires post-processing after steps in the GSON lifecycle.
 * The purpose is to avoid relying on manual post-initialization methods
 * to set transient fields.
 * <p>
 * To have GSON actually detect the use of this interface, use the {@link Gson} objects
 * created by {@link ComponentGsonFactory}.
 */
public interface GsonPostProcessable {

    /**
     * Called after the object is deserialized. Deserialization will fail if
     * this method throws a runtime exception.
     */
    void postDeserialize();

}
