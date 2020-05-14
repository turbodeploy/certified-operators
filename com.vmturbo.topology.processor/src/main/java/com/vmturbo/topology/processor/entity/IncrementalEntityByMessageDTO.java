package com.vmturbo.topology.processor.entity;

import javax.annotation.Nonnull;

import com.google.gson.Gson;

import io.swagger.annotations.ApiModelProperty;

import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;

/**
 * An EntityDTO as discovered by the probe during an incremental discovery, and the incremental
 * response message id.
 */
public class IncrementalEntityByMessageDTO {
    private final int messageId;

    private final EntityDTO entity;

    /**
     * Creates an IncrementalEntityByMessageDTO.
     * @param messageId the id of the message
     * @param entity the entity
     */
    public IncrementalEntityByMessageDTO(int messageId, @Nonnull final EntityDTO entity) {
        this.messageId = messageId;
        this.entity = entity;
    }

    private static final Gson GSON = ComponentGsonFactory.createGsonNoPrettyPrint();

    /**
     * Get the corresponding JSON.
     * @param dto the IncrementalEntityByMessageDTO
     * @return String
     */
    public static String toJson(IncrementalEntityByMessageDTO dto) {
        return GSON.toJson(dto, IncrementalEntityByMessageDTO.class);
    }

    /**
     * Get the IncrementalEntityByMessageDTO from a JSON object.
     * @param json for IncrementalEntityByMessageDTO
     * @return IncrementalEntityByMessageDTO
     */
    public static IncrementalEntityByMessageDTO fromJson(String json) {
        return GSON.fromJson(json, IncrementalEntityByMessageDTO.class);
    }
}
