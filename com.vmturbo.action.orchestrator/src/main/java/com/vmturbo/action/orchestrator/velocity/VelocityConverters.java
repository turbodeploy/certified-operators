package com.vmturbo.action.orchestrator.velocity;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Contains all the methods for converting objects to other formats for Velocity.
 */
public class VelocityConverters {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /**
     * Converts the provided object to json.
     *
     * @param object the object to convert to json.
     * @return the object converted to json.
     */
    public String toJson(Object object) throws JsonProcessingException {
        return OBJECT_MAPPER.writeValueAsString(object);
    }
}
