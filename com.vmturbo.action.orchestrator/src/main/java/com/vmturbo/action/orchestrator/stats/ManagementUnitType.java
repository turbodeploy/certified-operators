package com.vmturbo.action.orchestrator.stats;


import com.vmturbo.action.orchestrator.stats.groups.MgmtUnitSubgroup;

/**
 * The type of management unit (see: {@link MgmtUnitSubgroup}), used mostly for debugging
 * and visualization purposes.
 */
public enum ManagementUnitType {
    /**
     * A default to avoid mistaking unknown/unrecognized for any particular management unit.
     */
    UNKNOWN(0),

    /**
     * The global scope.
     */
    GLOBAL(1),

    /**
     * The cluster scope. This can be a physical cluster or a storage cluster.
     */
    CLUSTER(2),

    /**
     * Business account scope. This is only relevant in the cloud.
     */
    BUSINESS_ACCOUNT(3);


    private final int numericValue;

    private final static ManagementUnitType[] types = ManagementUnitType.values();

    ManagementUnitType(final int numValue) {
        this.numericValue = numValue;
    }

    public int getNumber() {
        return numericValue;
    }

    public static ManagementUnitType forNumber(final int num) {
        for (ManagementUnitType type : types) {
            if (num == type.numericValue) {
                return type;
            }
        }
        return ManagementUnitType.UNKNOWN;
    }
}
