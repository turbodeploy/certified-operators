/*
 * (C) Turbonomic 2020.
 */

package com.vmturbo.history.stats;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.components.common.utils.StringConstants;

/**
 * {@link PropertySubType} describes supported property subtypes. {@link Enum#ordinal()} is used to
 * store data in DB, i.e. value declaration order is important. Values returned from {@link
 * PropertySubType#getApiParameterName()} should not be changed without explicitly added migration.
 * Those values are persisted in stats tables.
 */
public enum PropertySubType {
    /**
     * Usage value in percents.
     */
    Utilization(StringConstants.UTILIZATION, false),
    /**
     * Usage value.
     */
    Used(StringConstants.USED, false),
    PriceIndex(StringConstants.PRICE_INDEX),
    /**
     * Number of VMs.
     */
    NumVms(StringConstants.NUM_VMS),
    /**
     * Number of Hosts.
     */
    NumHosts(StringConstants.NUM_HOSTS),
    /**
     * Number of Storages.
     */
    NumStorages(StringConstants.NUM_STORAGES),
    /**
     * Number of databases.
     */
    NumDbs(StringConstants.NUM_DBS),
    /**
     * Number of containers.
     */
    NumContainers(StringConstants.NUM_CONTAINERS),
    /**
     * Number of virtual datacenters.
     */
    NumVdcs(StringConstants.NUM_VDCS),
    /**
     * Number of VMs per Host.
     */
    NumVmsPerHost(StringConstants.NUM_VMS_PER_HOST),
    /**
     * Number of VMs per Storage.
     */
    NumVmsPerStorage(StringConstants.NUM_VMS_PER_STORAGE),
    NumCntPerHost(StringConstants.NUM_CNT_PER_HOST),
    NumCntPerStorage(StringConstants.NUM_CNT_PER_STORAGE),
    NumCntPerVm(StringConstants.NUM_CNT_PER_VM),
    HeadroomVms(StringConstants.HEADROOM_VMS),
    CurrentHeadroom(StringConstants.CURRENT_HEADROOM),
    DesiredVms(StringConstants.DESIREDVMS),
    Produces(StringConstants.PRODUCES),
    NumRi(StringConstants.NUM_RI),
    RiCouponCoverage(StringConstants.RI_COUPON_COVERAGE),
    RiCouponUtilization(StringConstants.RI_COUPON_UTILIZATION),
    RiDiscount(StringConstants.RI_DISCOUNT),
    /**
     * Number of CPUs available for entity.
     */
    NumCpus(StringConstants.NUM_CPUS),
    /**
     * Number of sockets available for entity.
     */
    NumSockets(StringConstants.NUM_SOCKETS),
    /**
     * Number of VCPUs available for entity.
     */
    NumVcpus(StringConstants.NUM_VCPUS);

    private final String apiParameterName;
    private final boolean metric;

    PropertySubType(@Nonnull String apiParameterName) {
        this(apiParameterName, true);
    }

    PropertySubType(@Nonnull String apiParameterName, boolean metric) {
        this.apiParameterName = apiParameterName;
        this.metric = metric;
    }

    /**
     * Finds appropriate {@link PropertySubType} value to API parameter.
     *
     * @param apiParameter which should be resolved to {@link PropertySubType}
     *                 value.
     * @return resolved {@link PropertySubType} value or {@code null}.
     */
    @Nullable
    public static PropertySubType fromApiParameter(@Nullable String apiParameter) {
        for (PropertySubType value : values()) {
            if (value.apiParameterName.equals(apiParameter)) {
                return value;
            }
        }
        return null;
    }

    /**
     * Returns API parameter name matching current value.
     *
     * @return API parameter name matching current value.
     */
    @Nonnull
    public String getApiParameterName() {
        return apiParameterName;
    }

    /**
     * Returns {@code true} in case {@link PropertySubType} value corresponds to the metric.
     *
     * @return {@code true} in case {@link PropertySubType} value corresponds to the
     *                 metric.
     */
    public boolean isMetric() {
        return metric;
    }
}
