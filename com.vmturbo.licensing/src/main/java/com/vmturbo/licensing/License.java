package com.vmturbo.licensing;

import java.io.Serializable;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import com.vmturbo.api.dto.license.ILicense;

public class License implements ILicense, Comparable<ILicense>, Serializable {

    private String uuid;
    private String licenseOwner;
    private String email;
    private String expirationDate;
    private String licenseKey;
    private String externalLicenseKey;
    private String edition;
    private String filename;
    private CountedEntity countedEntity;
    private boolean externalLicense;
    private Set<ErrorReason> errorReasons = new LinkedHashSet<>();
    private int numLicensedEntities;
    private int numInUseEntities;

    private SortedSet<String> features = new TreeSet<>();

    @Override
    public String getUuid() {
        return uuid;
    }

    public License setUuid(final String uuid) {
        this.uuid = uuid;
        return this;
    }

    @Override
    public String getLicenseOwner() {
        return licenseOwner;
    }

    public License setLicenseOwner(final String licenseOwner) {
        this.licenseOwner = licenseOwner;
        return this;
    }

    @Override
    public String getEmail() {
        return email;
    }

    public License setEmail(final String email) {
        this.email = email;
        return this;
    }

    @Override
    public String getExpirationDate() {
        return expirationDate;
    }

    public License setExpirationDate(final String expirationDate) {
        this.expirationDate = expirationDate;
        return this;
    }

    @Override
    public SortedSet<String> getFeatures() {
        return features;
    }

    public License setFeatures(final SortedSet<String> features) {
        this.features = features;
        return this;
    }

    public void addFeatures(Collection<String> features) {
        if (features != null) {
            this.features.addAll(features);
        }
    }

    @Override
    public int getNumLicensedEntities() {
        return numLicensedEntities;
    }

    public License setNumLicensedEntities(final int numLicensedEntities) {
        this.numLicensedEntities = numLicensedEntities;
        return this;
    }

    @Override
    public int getNumInUseEntities() {
        return numInUseEntities;
    }

    public License setNumInUseEntities(final int numInUseEntities) {
        this.numInUseEntities = numInUseEntities;
        return this;
    }

    @Override
    public String getLicenseKey() {
        return licenseKey;
    }

    public License setLicenseKey(final String licenseKey) {
        this.licenseKey = licenseKey;
        return this;
    }

    @Override
    public String getExternalLicenseKey() {
        return externalLicenseKey;
    }

    public License setExternalLicenseKey(final String externalLicenseKey) {
        this.externalLicenseKey = externalLicenseKey;
        return this;
    }

    @Override
    public Set<ErrorReason> getErrorReasons() {
        return errorReasons;
    }

    public License setErrorReasons(final Set<ErrorReason> errorReasons) {
        this.errorReasons = errorReasons;
        return this;
    }

    public CountedEntity getCountedEntity() {
        if (StringUtils.isNotBlank(edition)) {
            return CountedEntity.VM;
        }
        return countedEntity;
    }

    public License setCountedEntity(CountedEntity countedEntity) {
        this.countedEntity = countedEntity;
        return this;
    }

    @Override
    public boolean isExternalLicense() {
        return externalLicense;
    }

    public License setExternalLicense(boolean externalLicense) {
        this.externalLicense = externalLicense;
        return this;
    }

    public String getEdition() {
        return edition;
    }

    public License setEdition(String edition) {
        this.edition = edition;
        return this;
    }

    @Override
    public String getFilename() {
        return filename;
    }

    public License setFilename(final String filename) {
        this.filename = filename;
        return this;
    }

    @Override
    public int compareTo(ILicense other) {
        return ILicense.super.compareTo(other);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final License that = (License) o;
        return new EqualsBuilder()
                .append(externalLicenseKey, that.externalLicenseKey)
                .append(licenseKey, that.licenseKey)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(licenseKey)
                .append(externalLicenseKey)
                .toHashCode();
    }
}

