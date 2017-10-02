package com.vmturbo.components.test.utilities.topology.conversion;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlType;

/**
 * The JAXB model for parsing a classic topology CommodityBought object.
 *
 * A CommodityBought is a commodity that is being bought by a service entity
 * from a specific provider.
 */
@XmlType(name = "")
@XmlAccessorType(XmlAccessType.FIELD)
public class CommodityBoughtElement extends CommodityElement {

    /**
     * The consumes attribute indicates which CommoditySold this CommodityBought is consuming from.
     * Its value is the UUID of the matching CommoditySold. XL usually wishes to map this consumes
     * relationship back to the UUID of the provider ServiceEntity of this CommoditySold.
     */
    @XmlAttribute(name = "Consumes")
    private String Consumes;

    public String getConsumes() {
        return Consumes;
    }

    public void setConsumes(String consumes) {
        Consumes = consumes;
    }
}
