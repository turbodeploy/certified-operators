package com.vmturbo.repository.graph.result;

import com.google.common.base.MoreObjects;

import java.util.List;
import java.util.Objects;

/**
 * The entity object used in a scoped search.
 */
public class ScopedEntity {
    private String entityType;
    private String displayName;
    private String oid;
    private String state;
    // todo: if we use Long here, target "Long" ids will be returned as short id (not sure if it
    // was converted from long to integer or something else is wrong) This doesn't happen for
    // targetIds in "ServiceEntityRepoDTO", but it happens here. The oid is returned fine, but
    // targetIds are returned wrong. If I tried the query through ArangoDB UI, it is returning
    // correct ids for both. Probably something is wrong with jackson or vpack deserializer.
    private List<String> targetIds;

    public ScopedEntity(String entityType, String displayName, String oid) {
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

    public String getOid() {
        return oid;
    }

    public void setOid(String oid) {
        this.oid = oid;
    }


    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public List<String> getTargetIds() {
        return targetIds;
    }

    public void setTargetIds(List<String> targetIds) {
        this.targetIds = targetIds;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("entityType", entityType)
                .add("displayName", displayName)
                .add("oid", oid)
                .add("state", state)
                .add("targetIds", targetIds)
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ScopedEntity)) return false;
        ScopedEntity that = (ScopedEntity) o;
        return Objects.equals(oid, that.oid) &&
                Objects.equals(entityType, that.entityType) &&
                Objects.equals(displayName, that.displayName) &&
                Objects.equals(state, that.state) &&
                Objects.equals(targetIds, that.targetIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(entityType, displayName, oid, state, targetIds);
    }
}
