package com.vmturbo.topology.processor.entity;

import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nonnull;

import com.google.gson.Gson;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;

/**
 * An EntityDTO as discovered by the probe together with its OID identifier.
 */
@ApiModel("IdentifiedEntityDTO")
public class IdentifiedEntityDTO {
    private final long oid;

    private final EntityDTO entity;

    private static final AtomicLong SUFFIX = new AtomicLong(0);

    public IdentifiedEntityDTO(long oid, @Nonnull final EntityDTO entity) {
        this.oid = oid;
        this.entity = entity;
    }

    @ApiModelProperty(value = "The oid for the entity", required = true)
    public long getOid() {
        return oid;
    }

    @ApiModelProperty(value = "The description of the entity as provided by the target",
                      required = true)
    public EntityDTO getEntity() {
        return entity;
    }

    private static final Gson GSON = ComponentGsonFactory.createGsonNoPrettyPrint();

    public static String toJson(IdentifiedEntityDTO dto) {
        return GSON.toJson(dto, IdentifiedEntityDTO.class);
    }

    public static IdentifiedEntityDTO fromJson(String json) {
        return GSON.fromJson(json, IdentifiedEntityDTO.class);
    }

    /**
     * Anonymizes the EntityDTO and returns its (together with the oid) JSON representation.
     *
     * @param dto The IdentifiedEntityDTO.
     * @return The JSON representation of the dto.
     */
    public static @Nonnull String toJsonAnonymized(final @Nonnull IdentifiedEntityDTO dto) {
        EntityDTO.Builder dtoBlder = EntityDTO.newBuilder();
        EntityDTO entity = dtoBlder.setId(String.valueOf(SUFFIX.getAndIncrement()))
                                   .setEntityType(dto.entity.getEntityType())
                                   .setDisplayName(String.valueOf(SUFFIX.getAndIncrement()))
                                   .build();
        return GSON.toJson(new IdentifiedEntityDTO(dto.oid, entity),
                           IdentifiedEntityDTO.class);
    }
}