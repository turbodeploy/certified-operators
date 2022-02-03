package com.vmturbo.cost.calculation;

import java.util.Objects;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.LicenseModel;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;

/**
 * The pricing context that can be use as key for storing the already computed cost.
 */

public class PricingContext {
    private final long regionId;
    private final OSType osType;
    private final LicenseModel operatingSystemLicenseModel;
    private final long computeTierId;
    private final long accountPricingDataOid;

    /**
     * The constructor.
     *
     * @param regionId the region id of the vm
     * @param osType the os type of the vm
     * @param operatingSystemLicenseModel determines if the vm has ahub or license included
     * @param computeTierId the compute tier id for which the cost is calculated
     * @param accountPricingDataOid the account pricing oid which helps in figuring out the
     *         table.
     */
    public PricingContext(long regionId, OSType osType, LicenseModel operatingSystemLicenseModel,
            long computeTierId, long accountPricingDataOid) {
        this.regionId = regionId;
        this.osType = osType;
        this.operatingSystemLicenseModel = operatingSystemLicenseModel;
        this.computeTierId = computeTierId;
        this.accountPricingDataOid = accountPricingDataOid;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof PricingContext) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PricingContext that = (PricingContext)o;
            return accountPricingDataOid == that.accountPricingDataOid && osType == that.osType
                    && operatingSystemLicenseModel == that.operatingSystemLicenseModel && computeTierId == that.computeTierId && regionId == that.regionId;
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(accountPricingDataOid, osType, operatingSystemLicenseModel,
                computeTierId, regionId);
    }
}
