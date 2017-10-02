package com.vmturbo.components.test.utilities.topology.conversion;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * The JAXB model for parsing a classic topology file.
 * This element s the root of the file and contains several markets.
 */
@XmlRootElement(name = "XMI", namespace = "http://www.omg.org/XMI")
@XmlAccessorType(XmlAccessType.FIELD)
public class XmiElement {

    @XmlElement(name="Market", namespace = "http:///com/vmturbo/platform/VMTRoot.ecore/1.0.0/ManagedEntities/Analysis")
    private List<MarketElement> markets = new ArrayList<>();

    @XmlElement(name="GroupManager", namespace = "http:///com/vmturbo/platform/VMTRoot.ecore/1.0.0/ManagedEntities/Abstraction")
    private List<GroupManagerElement> groupManagers = new ArrayList<>();

    @XmlElement(name="Group", namespace = "http:///com/vmturbo/platform/VMTRoot.ecore/1.0.0/ManagedEntities/Abstraction")
    private List<GroupElement> basicGroups = new ArrayList<>();

    @XmlElement(name="RefGroup", namespace = "http:///com/vmturbo/platform/VMTRoot.ecore/1.0.0/ManagedEntities/Abstraction")
    private List<RefGroupElement> refGroups = new ArrayList<>();

    public List<MarketElement> getMarkets() {
        return markets;
    }

    public void setMarkets(List<MarketElement> markets) {
        this.markets = markets;
    }

    public List<GroupManagerElement> getGroupManagers() {
        return groupManagers;
    }

    public void setGroupManagers(List<GroupManagerElement> groupManagers) {
        this.groupManagers = groupManagers;
    }

    public List<GroupElement> getBasicGroups() {
        return basicGroups;
    }

    public void setBasicGroups(List<GroupElement> groups) {
        this.basicGroups = groups;
    }

    public List<RefGroupElement> getRefGroups() {
        return refGroups;
    }

    public void setRefGroups(List<RefGroupElement> refGroups) {
        this.refGroups = refGroups;
    }

    public Stream<GroupElement> getAllGroups() {
        return Stream.concat(basicGroups.stream(), refGroups.stream());
    }

    /**
     * Get the MainMarket element of the topology. The MainMarket is the equivalent of the XL
     * Live/RealTime market.
     *
     * A valid topology file will always contain a MainMarket.
     *
     * @return The MainMarket.
     */
    public MarketElement getMainMarket() {
        return markets.stream()
            .filter(MarketElement::isMainMarket)
            .findFirst()
            .get();
    }

    /**
     * Get the GroupManager associated with the main market.
     *
     * A valid topology file will always contain a MainMarket and associated GroupManager.
     *
     * @return The GroupManager for the main market.
     */
    public GroupManagerElement getMainMarketGroupManager() {
        final String mainMarketUuid = getMainMarket().getUuid();

        return groupManagers.stream()
            .filter(groupManager -> groupManager.getMarketUuid().equals(mainMarketUuid))
            .findFirst()
            .get();
    }

    /**
     * Get a {@link Stream} of the groups associated with the main market.
     *
     * @return The GroupManager for the main market.
     */
    public Stream<GroupElement> getMainMarketGroups() {
        final String groupManagerUuid = getMainMarketGroupManager().getUuid();

        return getAllGroups()
            .filter(group ->
                group.getGroupManagerUuid() != null && group.getGroupManagerUuid().equals(groupManagerUuid));
    }
}
