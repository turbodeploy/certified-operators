package com.vmturbo.mediation.udt.inventory;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Class represents a UDT entity.
 */
public class UdtEntity {

    private final String id;
    private final String name;
    private final EntityType entityType;
    private final Set<UdtChildEntity> children;

    /**
     * Constructor.
     *
     * @param entityType - type of entity.
     * @param id         - ID of topology data definition.
     * @param name       - name of topology data definition..
     * @param children     - a set of children.
     */
    public UdtEntity(EntityType entityType, String id, String name, Set<UdtChildEntity> children) {
        this.entityType = entityType;
        this.id = id;
        this.name = name;
        this.children = Collections.unmodifiableSet(children);
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public Set<UdtChildEntity> getChildren() {
        return children;
    }

    public EntityType getEntityType() {
        return entityType;
    }

    @Override
    public String toString() {
        String childrenStr = children.stream().map(ch -> String.valueOf(ch.getDtoId()))
                .collect(Collectors.joining(","));
        return String.format("[id=%s name=%s type=%s children=%s]", id, name,
                entityType.name(), childrenStr);
    }
}
