package com.vmturbo.repository.graph.result;

import com.google.common.base.MoreObjects;

import java.util.List;
import java.util.Objects;

/**
 * A product of <code>entityType</code> and <code>oids</code>.
 */
public class TypeAndOids {
    private String entityType;
    private List<Long> oids;

    public TypeAndOids(String entityType, List<Long> oids) {
        this.entityType = entityType;
        this.oids = oids;
    }

    public TypeAndOids() { }

    public String getEntityType() {
        return entityType;
    }

    public void setEntityType(String entityType) {
        this.entityType = entityType;
    }

    public List<Long> getOids() {
        return oids;
    }

    public void setOids(List<Long> oids) {
        this.oids = oids;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("entityType", entityType)
                .add("oids", oids)
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TypeAndOids)) return false;
        TypeAndOids that = (TypeAndOids) o;
        return Objects.equals(entityType, that.entityType) &&
                Objects.equals(oids, that.oids);
    }

    @Override
    public int hashCode() {
        return Objects.hash(entityType, oids);
    }
}
