package com.vmturbo.components.test.utilities.topology.conversion;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;

/**
 * The JAXB model for parsing a classic topology Group object.
 */
@XmlType(name = "",
    propOrder = {
        "matchingCriteria"
    }
)
@XmlAccessorType(XmlAccessType.FIELD)
public class GroupElement {
    @XmlElement(name="matchingCriteria")
    private List<MatchingCriteriaElement> matchingCriteria = new ArrayList<>();

    @XmlAttribute(name="uuid")
    private String uuid;

    @XmlAttribute(name="name")
    private String name;

    @XmlAttribute(name="displayName")
    private String displayName;

    @XmlAttribute(name="SETypeName")
    private String entityType;

    @XmlAttribute(name="managedBy")
    private String groupManagerUuid;

    public boolean isDynamic() {
        return matchingCriteria.size() > 0;
    }

    public List<MatchingCriteriaElement> getMatchingCriteria() {
        return matchingCriteria;
    }

    public void setMatchingCriteria(List<MatchingCriteriaElement> matchingCriteria) {
        this.matchingCriteria = matchingCriteria;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDisplayName() {
        return displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public String getEntityType() {
        return entityType;
    }

    public void setEntityType(String entityType) {
        this.entityType = entityType;
    }

    public String getGroupManagerUuid() {
        return groupManagerUuid;
    }

    public void setGroupManagerUuid(String groupManagerUuid) {
        this.groupManagerUuid = groupManagerUuid;
    }

    @XmlAccessorType(XmlAccessType.FIELD)
    static class MatchingCriteriaElement {
        @XmlAttribute(name="uuid")
        private String uuid;

        @XmlAttribute(name="name")
        private String name;

        public String getUuid() {
            return uuid;
        }

        public void setUuid(String uuid) {
            this.uuid = uuid;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }
}
