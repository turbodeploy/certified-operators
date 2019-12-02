package com.vmturbo.action.orchestrator.action.constraint;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.vmturbo.common.protobuf.action.ActionConstraintDTO.ActionConstraintInfo;
import com.vmturbo.common.protobuf.action.ActionConstraintDTO.ActionConstraintInfo.CoreQuotaInfo.CoreQuotaByBusinessAccount.CoreQuotaByRegion;
import com.vmturbo.common.protobuf.action.ActionConstraintDTO.ActionConstraintInfo.CoreQuotaInfo.CoreQuotaByBusinessAccount.CoreQuotaByRegion.CoreQuotaByFamily;
import com.vmturbo.components.common.utils.StringConstants;

/**
 * This class is used to store all core quota info. This is a singleton class.
 */
public class CoreQuotaStore implements ActionConstraintStore {

    private static CoreQuotaStore coreQuotaStore = new CoreQuotaStore();

    // businessAccountId -> regionId -> family -> quota
    private final Map<Long, Map<Long, Map<String, Integer>>> coreQuotaInfoMap = new HashMap<>();

    /**
     * Private to prevent instantiation.
     */
    private CoreQuotaStore() {}

    /**
     * Get the only instance of the class.
     *
     * @return the only instance of the class
     */
    static CoreQuotaStore getCoreQuotaStore() {
        return coreQuotaStore;
    }

    /**
     * {@inheritDoc}
     * synchronized is used here to ensure thread safety.
     */
    @Override
    public synchronized void updateActionConstraintInfo(ActionConstraintInfo actionConstraintInfo) {
        if (!actionConstraintInfo.hasActionConstraintType()) {
            return;
        }

        coreQuotaInfoMap.clear();
        actionConstraintInfo.getCoreQuotaInfo()
            .getCoreQuotaByBusinessAccountList()
            .forEach(coreQuotaByBusinessAccount -> coreQuotaInfoMap.put(
                coreQuotaByBusinessAccount.getBusinessAccountId(),
                coreQuotaByBusinessAccount.getCoreQuotaByRegionList().stream()
                    .collect(Collectors.toMap(CoreQuotaByRegion::getRegionId,
                        coreQuotaByRegion -> {
                            Map<String, Integer> coreQuotaByFamily = coreQuotaByRegion
                                .getCoreQuotaByFamilyList().stream()
                                .collect(Collectors.toMap(CoreQuotaByFamily::getFamily,
                                    CoreQuotaByFamily::getQuota));
                            coreQuotaByFamily.put(StringConstants.TOTAL_CORE_QUOTA,
                                coreQuotaByRegion.getTotalCoreQuota());
                            return coreQuotaByFamily;
                        }))));
    }

    /**
     * Get the core quota of a business account in a region of a family.
     * synchronized is used here to ensure thread safety. Maybe this is not needed
     * because synchronized is used on {@link CoreQuotaStore#updateActionConstraintInfo}.
     *
     * @param businessAccountId the business account id
     * @param regionId the region id
     * @param family the family name
     * @return the core quota. Return 0 if not found
     */
    public synchronized int getCoreQuota(
            final long businessAccountId, final long regionId, final String family) {
        return Optional.ofNullable(coreQuotaInfoMap.get(businessAccountId))
            .map(coreQuotaByRegionMap -> coreQuotaByRegionMap.get(regionId))
            .map(coreQuotaByFamilyMap -> coreQuotaByFamilyMap.get(family)).orElse(Integer.MAX_VALUE);
    }
}
