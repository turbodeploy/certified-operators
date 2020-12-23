package com.vmturbo.mediation.udt.inventory;

import javax.annotation.Nonnull;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Class represents a child of UDT entity.
 */
public class UdtChildEntity {

    private String udtId = null;
    private final long oid;
    private final EntityType entityType;

    /**
     * Constructor.
     *
     * @param oid        - OID of topology entity.
     * @param entityType - type of entity.
     */
    public UdtChildEntity(long oid, EntityType entityType) {
        this.oid = oid;
        this.entityType = entityType;
    }

    public long getOid() {
        return oid;
    }

    public void setUdtId(@Nonnull String udtId) {
        this.udtId = udtId;
    }

    /**
     * Provides an ID for EntityDTO.
     * If this entity has defined UDT ID, it is used as ID of an EntityDTO.
     *
     * @return EntityDTO identification.
     */
    @Nonnull
    public String getDtoId() {
        return udtId != null ? udtId :  String.valueOf(oid);
    }

    public EntityType getEntityType() {
        return entityType;
    }
}
