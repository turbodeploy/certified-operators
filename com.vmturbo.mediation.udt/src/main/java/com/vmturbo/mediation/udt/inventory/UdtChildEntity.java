package com.vmturbo.mediation.udt.inventory;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Class represents a child of UDT entity.
 */
public class UdtChildEntity {

    private long udtId = 0L;
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

    public void setUdtId(long udtId) {
        this.udtId = udtId;
    }

    /**
     * Provides an ID for EntityDTO.
     * If this entity has defined UDT ID, it is used as ID of an EntityDTO.
     *
     * @return EntityDTO identification.
     */
    public long getDtoId() {
        return udtId > 0 ? udtId : oid;
    }

    public EntityType getEntityType() {
        return entityType;
    }
}
