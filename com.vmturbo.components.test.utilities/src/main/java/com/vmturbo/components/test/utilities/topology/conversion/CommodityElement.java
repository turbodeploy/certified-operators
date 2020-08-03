package com.vmturbo.components.test.utilities.topology.conversion;

import java.util.function.Consumer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlType;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

import com.vmturbo.common.protobuf.topology.UICommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;

/**
 * The JAXB model for parsing a classic topology Commodity object.
 *
 * Commodities are bought in sold by service entities in a market.
 */
@XmlType(name = "")
@XmlAccessorType(XmlAccessType.FIELD)
public class CommodityElement implements IntoSdkProto<CommodityDTO> {

    @XmlAttribute
    private String uuid;
    @XmlAttribute
    private String name;
    @XmlAttribute
    private Double used;
    @XmlAttribute
    private Double utilization;
    @XmlAttribute
    private Double capacity;
    @XmlAttribute
    private Double price;
    @XmlAttribute
    private Boolean active;
    @XmlAttribute
    private Boolean resizable;
    @XmlAttribute
    private Boolean thin;
    @XmlAttribute
    private Double limit;
    @XmlAttribute
    private Double peak;
    @XmlAttribute
    private Double reservation;
    @XmlAttribute
    private String key;
    @XmlAttribute
    private Double resizeFactor;
    @XmlAttribute
    private Double utilThreshold;
    @XmlAttribute
    private Integer numConsumedBy;

    @XmlAttribute(name = "type", namespace = "http://www.w3.org/2001/XMLSchema-instance")
    protected String namespaceCommodityType;

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

    public Double getUsed() {
        return used;
    }

    public void setUsed(Double used) {
        this.used = used;
    }

    public Double getUtilization() {
        return utilization;
    }

    public void setUtilization(Double utilization) {
        this.utilization = utilization;
    }

    public Double getCapacity() {
        return capacity;
    }

    public void setCapacity(Double capacity) {
        this.capacity = capacity;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    public Boolean getActive() {
        return active;
    }

    public void setActive(Boolean active) {
        this.active = active;
    }

    public Boolean getResizable() {
        return resizable;
    }

    public void setResizable(Boolean resizable) {
        this.resizable = resizable;
    }

    public Double getLimit() {
        return limit;
    }

    public void setLimit(Double limit) {
        this.limit = limit;
    }

    public Double getPeak() {
        return peak;
    }

    public void setPeak(Double peak) {
        this.peak = peak;
    }

    public Double getReservation() {
        return reservation;
    }

    public void setReservation(Double reservation) {
        this.reservation = reservation;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this);
    }

    /**
     * CommodityTypes are imported in their EMF XML namespace but we do not refer to them in that
     * namespace. Strip off the namespace to return the raw commodity type.
     *
     * Example: "Abstraction:CPU" -> "CPU"
     *
     * @return The commodity type with EMF namespace stripped off.
     */
    public String getCommodityType() {
        return namespaceCommodityType.contains(":") ?
            namespaceCommodityType.split(":")[1] :
            namespaceCommodityType;
    }

    public Double getResizeFactor() {
        return resizeFactor;
    }

    public void setResizeFactor(Double resizeFactor) {
        this.resizeFactor = resizeFactor;
    }

    public Integer getNumConsumedBy() {
        return numConsumedBy;
    }

    public void setNumConsumedBy(Integer numConsumedBy) {
        this.numConsumedBy = numConsumedBy;
    }

    public Boolean getThin() {
        return thin;
    }

    public void setThin(Boolean thin) {
        this.thin = thin;
    }

    public Double getUtilThreshold() {
        return utilThreshold;
    }

    public void setUtilThreshold(Double utilThreshold) {
        this.utilThreshold = utilThreshold;
    }

    @Nonnull
    @Override
    public CommodityDTO toSdkProto() {
        final CommodityDTO.Builder builder = CommodityDTO.newBuilder()
            .setDisplayName(getName())
            .setCommodityType(UICommodityType.fromString(getCommodityType()).sdkType());

        conditionallySet(builder::setActive, getActive());
        setPositive(builder::setCapacity, getCapacity());
        conditionallySet(builder::setKey, getKey());
        conditionallySetNonNegative(builder::setLimit, getLimit());
        conditionallySet(builder::setPeak, getPeak());
        conditionallySet(builder::setReservation, getReservation());
        conditionallySet(builder::setResizable, getResizable());
        conditionallySet(builder::setThin, getThin());
        conditionallySet(builder::setUsed, getUsed());
        conditionallySet(builder::setUtilizationThresholdPct, getUtilThreshold());

        return builder.build();
    }

    /**
     * Call the setter with the value if the value is non-null and non-negative.
     * If value is null or negative, set to a very small positive number that the TopologyProcessor
     * will consider to be legal.
     *
     * Useful on fields such as capacity which the TP expects to be present.
     *
     * @param setter The setter to call.
     * @param value The value to pass to the setter.
     * @return True if the setter was called, false was otherwise.
     */
    private boolean setPositive(Consumer<Double> setter, @Nullable Double value) {
        if (value != null && value > 0) {
            setter.accept(value);
            return true;
        } else {
            setter.accept(Double.MIN_VALUE);
            return false;
        }
    }

    /**
     * Call the setter with the value if the value is non-null and non-negative.
     *
     * @param setter The setter to call.
     * @param value The value to pass to the setter.
     * @return True if the setter was called, false was otherwise.
     */
    private boolean conditionallySetNonNegative(Consumer<Double> setter,
                                                @Nullable Double value) {
        if (value != null && value >= 0) {
            setter.accept(value);
            return true;
        } else {
            return false;
        }
    }
}
