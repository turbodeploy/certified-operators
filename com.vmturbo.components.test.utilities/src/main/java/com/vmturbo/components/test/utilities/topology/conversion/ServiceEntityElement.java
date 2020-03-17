package com.vmturbo.components.test.utilities.topology.conversion;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.components.common.ClassicEnumMapper;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CommodityBought;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityOrigin;

/**
 * The JAXB model for parsing a classic topology ServiceEntity object.
 *
 * Note that this class currently makes no attempt to include entity-specific properties such
 * as numCpus, IP Address, LUN ID, etc.
 */
@XmlType(name = "",
    propOrder = {
        "Commodities",
        "CommoditiesBought"
    })
@XmlAccessorType(XmlAccessType.FIELD)
public class ServiceEntityElement implements IntoSdkProto<EntityDTO.Builder> {

    private static final Logger logger = LogManager.getLogger();

    @XmlElement(name="Commodities")
    private List<CommodityElement> Commodities = new ArrayList<>();

    @XmlElement(name="CommoditiesBought")
    private List<CommodityBoughtElement> CommoditiesBought = new ArrayList<>();

    @XmlAttribute
    private String uuid;
    @XmlAttribute
    private String name;
    @XmlAttribute
    private String displayName;
    @XmlAttribute
    private String state;
    @XmlAttribute
    private String rOI;
    @XmlAttribute
    private String expenses;
    @XmlAttribute
    private String revenues;
    @XmlAttribute
    private String budget;
    @XmlAttribute
    private String priceIndex;
    @XmlAttribute
    private Boolean monitored;
    @XmlAttribute
    private String suspendable;
    @XmlAttribute
    private String cloneable;

    @XmlAttribute(name = "type", namespace = "http://www.w3.org/2001/XMLSchema-instance")
    protected String namespaceEntityType;

    public String getRevenues() {
        return revenues;
    }

    public void setRevenues(String revenues) {
        this.revenues = revenues;
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

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getrOI() {
        return rOI;
    }

    public void setrOI(String rOI) {
        this.rOI = rOI;
    }

    public String getExpenses() {
        return expenses;
    }

    public void setExpenses(String expenses) {
        this.expenses = expenses;
    }

    public String getBudget() {
        return budget;
    }

    public void setBudget(String budget) {
        this.budget = budget;
    }

    public String getPriceIndex() {
        return priceIndex;
    }

    public void setPriceIndex(String priceIndex) {
        this.priceIndex = priceIndex;
    }

    public Boolean getMonitored() {
        return monitored;
    }

    public void setMonitored(Boolean monitored) {
        this.monitored = monitored;
    }

    public String getSuspendable() {
        return suspendable;
    }

    public void setSuspendable(String suspendable) {
        this.suspendable = suspendable;
    }

    public String getCloneable() {
        return cloneable;
    }

    public void setCloneable(String cloneable) {
        this.cloneable = cloneable;
    }

    @Override
    public String toString() {
//        return ReflectionToStringBuilder.toString(this);
        return getEntityType() + ": " + Commodities.size();
    }

    /**
     * EntityTypes are imported in their EMF XML namespace but we do not refer to them in that
     * namespace. Strip off the namespace to return the raw commodity type.
     *
     * Example: "Abstraction:PhysicalMachine" -> "PhysicalMachine"
     *
     * @return The EntityType with EMF namespace stripped off.
     */
    public String getEntityType() {
        return namespaceEntityType.contains(":") ?
            namespaceEntityType.split(":")[1] :
            namespaceEntityType;
    }

    /**
     * Some managers are erroneously saved as service entities. Don't include managers in the
     * import because those are not actually part of the customer topology.
     *
     * @return Whether this service entity is actually a service entity (true) or is a mis-classified
     *         EMF manager object (false).
     */
    public boolean isServiceEntity() {
        return !getEntityType().contains("Manager");
    }

    public String getNamespaceEntityType() {
        return namespaceEntityType;
    }

    public void setNamespaceEntityType(String namespaceEntityType) {
        this.namespaceEntityType = namespaceEntityType;
    }

    public List<CommodityElement> getCommodities() {
        return Commodities;
    }

    public void setCommodities(List<CommodityElement> commodities) {
        Commodities = commodities;
    }

    public List<CommodityBoughtElement> getCommoditiesBought() {
        return CommoditiesBought;
    }

    public void setCommoditiesBought(List<CommodityBoughtElement> commoditiesBought) {
        CommoditiesBought = commoditiesBought;
    }

    /**
     * Convert this {@link ServiceEntityElement} into an SDK protobuf builder.
     * Note that the returned object does not yet have its commodities bought properly
     * set up because a commoditySoldIdToProviderIdMap is required in order to do that.
     *
     * @return An {@link EntityDTO.Builder} without its commodities bought set up.
     */
    @Nonnull
    @Override
    public EntityDTO.Builder toSdkProto() {
        final EntityDTO.Builder builder = EntityDTO.newBuilder()
            .setEntityType(ApiEntityType.fromString(getEntityType()).sdkType())
            .setId(getUuid())
            .setDisplayName(getDisplayName())
            .setOrigin(EntityOrigin.DISCOVERED);

        conditionallySet(builder::setMonitored, getMonitored());
        conditionallySet(builder::setPowerState, ClassicEnumMapper.powerState(getState()));
        setupCommoditiesSold(builder);

        return builder;
    }

    /**
     * Set up the {@link EntityDTO.Builder} object generated via {@link #toSdkProto()} call with
     * commodities bought.
     *
     * @param builder The builder to setup.
     * @param commoditySoldIdToProviderIdMap A map of commodities sold by their UUID to the UUId of
     *                                       the provider selling that commodity sold.
     */
    void setupCommoditiesBought(@Nonnull final EntityDTO.Builder builder,
                                @Nonnull final Map<String, String> commoditySoldIdToProviderIdMap) {
        logCommoditiesWithoutProviders();
        final Map<String, List<CommodityBoughtElement>> commoditiesBoughtMap =
            commoditiesBoughtMap(commoditySoldIdToProviderIdMap);

        commoditiesBoughtMap.forEach((providerId, commodityBoughtList) -> builder.addCommoditiesBought(
            CommodityBought.newBuilder()
                .setProviderId(providerId)
                .addAllBought(commodityBoughtList.stream()
                    .map(CommodityElement::toSdkProto)
                    .collect(Collectors.toList()))
        ));
    }

    /**
     * Set up the commodities sold on a {@link EntityDTO.Builder}.
     *
     * @param builder The builder whose commodities sold should be set up.
     */
    private void setupCommoditiesSold(@Nonnull final EntityDTO.Builder builder) {
        Commodities.stream()
            .map(CommodityElement::toSdkProto)
            .forEach(builder::addCommoditiesSold);
    }

    /**
     * Issue a warning message describing commodities without providers that are not being properly set up.
     * This will probably lead to errors down the line when the topology is imported into XL.
     *
     * Note there is probably a way for these commodities to be handled but that is left as an exercise for later.
     */
    private void logCommoditiesWithoutProviders() {
        final List<String> commoditiesWithoutProviders = CommoditiesBought.stream()
            .filter(commodity -> commodity.getConsumes() == null)
            .map(commodity -> commodity.getCommodityType() + "::" + commodity.getUuid())
            .collect(Collectors.toList());

        if (!commoditiesWithoutProviders.isEmpty()) {
            final String commoditiesMessage = commoditiesWithoutProviders.stream()
                .collect(Collectors.joining(","));
            logger.warn("Entity {}::{} has commodities {} with no provider. Skipping these commodities.",
                getEntityType(), getUuid(), commoditiesMessage);
        }
    }

    /**
     * Create a map of providers to the commodities being bought from that provider by this
     * {@link ServiceEntityElement}. This is done by looking up the provider in the map of
     * commodity sold UUID -> providerId using the consumes relationship on the {@link CommodityBoughtElement}.
     *
     * IMPORTANT NOTE: Commodities without providers are not yet supported. If a commodity bought is buying from
     * a commodity sold whose ID does not show up in the consumer map, that commodity will be logged and dropped.
     * See note above about {@link #logCommoditiesWithoutProviders()}.
     *
     * @param commoditySoldIdToProviderIdMap A map of commodities sold by their UUID to the UUId of
     *                                       the provider selling that commodity sold.
     * @return A map of providers to the commodities being bought from that provider.
     */
    private Map<String, List<CommodityBoughtElement>> commoditiesBoughtMap(
        @Nonnull final Map<String, String> commoditySoldIdToProviderIdMap) {
        final Map<String, List<CommodityBoughtElement>> commoditiesBoughtMap = new HashMap<>(CommoditiesBought.size());

        CommoditiesBought.forEach(commodity -> {
            if (commodity.getConsumes() != null) {
                Stream.of(commodity.getConsumes().split(" ")).forEach(commoditySoldId -> {
                    final String providerId = commoditySoldIdToProviderIdMap.get(commoditySoldId);
                    if (providerId == null) {
                        logger.warn("Entity {}::{} is buying commodity {}::{} that consumes from {} " +
                                "which is not in the map of providers.",
                            getEntityType(), getUuid(), commodity.getCommodityType(),
                            commodity.getUuid(), commoditySoldId);
                    } else {
                        final List<CommodityBoughtElement> bought = commoditiesBoughtMap
                            .computeIfAbsent(providerId, unused -> new ArrayList<>());
                        bought.add(commodity);
                    }
                });
            }
        });

        return commoditiesBoughtMap;
    }
}
