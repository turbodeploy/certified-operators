package com.vmturbo.licensing.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

import com.vmturbo.api.dto.license.ILicense.CountedEntity;

/**
 * DTO used by jackson-xml to deserialize turbo xml v1 and v2 licenses.
 */
class LicenseXmlDTO {

    private String firstName;
    private String lastName;
    private String email;
    /**
     * The SalesForce ID uniquely identifying the customer.
     *
     * <p>It corresponds to the "id" entity in the XML license.</p>
     */
    private String customerId;

    private CountedEntity countedEntity;
    private int numSockets = 0;
    private int vmTotal = 0;
    private String expirationDate;
    private String lockCode;
    private String edition;

    private List<FeatureNode> featureNodes = new ArrayList<>();

    @JacksonXmlProperty(localName = "first-name")
    public String getFirstName() {
        return firstName;
    }

    public LicenseXmlDTO setFirstName(final String firstName) {
        this.firstName = firstName;
        return this;
    }

    @JacksonXmlProperty(localName = "last-name")
    public String getLastName() {
        return lastName;
    }

    public LicenseXmlDTO setLastName(final String lastName) {
        this.lastName = lastName;
        return this;
    }

    @JacksonXmlProperty(localName = "email")
    public String getEmail() {
        return email;
    }

    public LicenseXmlDTO setEmail(final String email) {
        this.email = email;
        return this;
    }

    @JacksonXmlProperty(localName = "id")
    public String getCustomerId() {
        return customerId;
    }

    public LicenseXmlDTO setCustomerId(final String customerId) {
        this.customerId = customerId;
        return this;
    }

    @JacksonXmlProperty(localName = "num-sockets")
    public int getNumSockets() {
        return numSockets;
    }

    @JacksonXmlProperty(localName = "vm-total")
    public int getVmTotal() {
        return vmTotal;
    }

    public LicenseXmlDTO setVmTotal(final int total) {
        this.vmTotal = total;
        this.countedEntity = CountedEntity.VM;
        return this;
    }

    public int getNumEntities() {
        return Math.max(numSockets, vmTotal);
    }

    @JacksonXmlProperty(localName = "edition")
    public String getEdition() {
        return edition;
    }

    public LicenseXmlDTO setEdition(final String edition) {
        this.edition = edition;
        return this;
    }

    public LicenseXmlDTO setNumSockets(final int numSockets) {
        this.numSockets = numSockets;
        this.countedEntity = CountedEntity.SOCKET;
        return this;
    }

    @JacksonXmlProperty(localName = "expiration-date")
    public String getExpirationDate() {
        return expirationDate;
    }

    public LicenseXmlDTO setExpirationDate(final String expirationDate) {
        this.expirationDate = expirationDate;
        return this;
    }

    @JacksonXmlProperty(localName = "lock-code")
    public String getLockCode() {
        return lockCode;
    }

    public LicenseXmlDTO setLockCode(final String lockCode) {
        this.lockCode = lockCode;
        return this;
    }

    @JacksonXmlProperty(localName = "feature")
    @JacksonXmlElementWrapper(useWrapping = false)
    public List<FeatureNode> getFeatureNodes() {
        return featureNodes;
    }

    public LicenseXmlDTO setFeatureNodes(List<FeatureNode> featureNodes) {
        this.featureNodes = featureNodes;
        return this;
    }

    public SortedSet<String> getFeatures() {
        return getFeatureNodes().stream()
            .map(FeatureNode::getFeatureName)
            .collect(Collectors.toCollection(TreeSet::new));
    }

    public LicenseXmlDTO setFeatures(List<String> features) {
        featureNodes = features.stream()
            .map(FeatureNode::new)
            .collect(Collectors.toList());
        return this;
    }

    public CountedEntity getCountedEntity() {
        return countedEntity;
    }

    /**
     * Represents a single {@code feature} element in an XML Turbonomic license.
     *
     * <p>It's used to describe how to parse the {@code feature} element to Jackson.</p>
     */
    public static class FeatureNode {
        private final String featureName;

        @JsonCreator()
        FeatureNode(@JacksonXmlProperty(isAttribute = true, localName = "FeatureName")
                                                                            String featureName) {
            this.featureName = featureName;
        }

        public String getFeatureName() {
            return featureName;
        }
    }

}
