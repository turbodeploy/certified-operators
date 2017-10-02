package com.vmturbo.repository.graph.result;

import com.google.common.base.MoreObjects;

import java.util.Objects;

/**
 * The entity object used in a scoped search.
 */
public class ScopedEntity {
    private String entityType;
    private String displayName;
    private long oid;
    private String state;

    public ScopedEntity(String entityType, String displayName, long oid) {
        this.entityType = entityType;
        this.displayName = displayName;
        this.oid = oid;
    }

    public ScopedEntity() { }

    public String getEntityType() {
        return entityType;
    }

    public void setEntityType(String entityType) {
        this.entityType = entityType;
    }

    public String getDisplayName() {
        return displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public long getOid() {
        return oid;
    }

    public void setOid(long oid) {
        this.oid = oid;
    }


    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("entityType", entityType)
                .add("displayName", displayName)
                .add("oid", oid)
                .add("state", state)
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ScopedEntity)) return false;
        ScopedEntity that = (ScopedEntity) o;
        return oid == that.oid &&
                Objects.equals(entityType, that.entityType) &&
                Objects.equals(displayName, that.displayName) &&
                Objects.equals(state, that.state);
    }

    @Override
    public int hashCode() {
        return Objects.hash(entityType, displayName, oid, state);
    }
}
