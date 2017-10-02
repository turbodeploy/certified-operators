package com.vmturbo.components.test.utilities.topology.conversion;

import javax.xml.bind.annotation.XmlAttribute;

/**
 * The JAXB model for parsing a classic topology RefGroup object.
 *
 * A RefGroup is similar to a group except that its members are found by using an EMF-relationship
 * based traversal. ie there might be a refGroup that consists of every PM that a certain Datacenter
 * hosts through the "hosts" relationship.
 */
public class RefGroupElement extends GroupElement {
    /**
     * The RefMetaGroup that contains the definition for each sub-RefGroup.
     * RefMetaGroups are not currently parsed.
     */
    @XmlAttribute(name="generatedBy")
    private String generatedBy;

    /**
     * The entity whose EMF-relationships are traversed to determine the members of the RefGroup.
     */
    @XmlAttribute(name="refEntityUuid")
    private String refEntityUuid;

    public String getGeneratedBy() {
        return generatedBy;
    }

    public void setGeneratedBy(String generatedBy) {
        this.generatedBy = generatedBy;
    }

    public String getRefEntityUuid() {
        return refEntityUuid;
    }

    public void setRefEntityUuid(String refEntityUuid) {
        this.refEntityUuid = refEntityUuid;
    }

    /**
     * RefGroups are by definition dynamic.
     *
     * @return true
     */
    @Override
    public boolean isDynamic() {
        return true;
    }
}
