package com.vmturbo.auth.api.licensing;

/**
 * Model to describe an inputDTO for populating new license
 */
public class LicenseApiInputDTO {
    private String license = null;

    public LicenseApiInputDTO license(String license) {
        this.license = license;
        return this;
    }

    /**
     * The xml data of new license
     *
     * @return license
     **/
    public String getLicense() {
        return license;
    }

    public void setLicense(String license) {
        this.license = license;
    }
}

