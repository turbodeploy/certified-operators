package com.vmturbo.components.test.utilities.topology.conversion;

import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;

/**
 * The JAXB model for parsing a classic topology GroupManager object.
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class GroupManagerElement {
    @XmlAttribute(name="uuid")
    private String uuid;

    @XmlAttribute(name="name")
    private String name;

    /**
     * The UUID of the market instance that this group manager is associated with.
     */
    @XmlAttribute(name="market")
    private String marketUuid;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getMarketUuid() {
        return marketUuid;
    }

    public void setMarketUuid(String marketUuid) {
        this.marketUuid = marketUuid;
    }
}
