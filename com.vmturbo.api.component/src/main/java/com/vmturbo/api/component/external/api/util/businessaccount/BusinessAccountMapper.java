package com.vmturbo.api.component.external.api.util.businessaccount;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.mutable.MutableInt;

import com.vmturbo.api.dto.businessunit.BusinessUnitApiDTO;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.api.enums.BusinessUnitType;
import com.vmturbo.api.enums.CloudType;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.BusinessAccountInfo;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.PricingIdentifier;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;
import com.vmturbo.topology.processor.api.util.ThinTargetCache.ThinProbeInfo;
import com.vmturbo.topology.processor.api.util.ThinTargetCache.ThinTargetInfo;

/**
 * Isolates the logic for mapping {@link TopologyEntityDTO}s representing a business account
 * to the {@link BusinessUnitApiDTO} that can be returned to the API.
 */
public class BusinessAccountMapper {

    private final ThinTargetCache thinTargetCache;

    private final SupplementaryDataFactory supplementaryDataFactory;

    /**
     * Constructs business account mapper.
     *
     * @param thinTargetCache target cache
     * @param supplementaryDataFactory supplementary data factory
     */
    public BusinessAccountMapper(@Nonnull final ThinTargetCache thinTargetCache,
            @Nonnull final SupplementaryDataFactory supplementaryDataFactory) {
        this.thinTargetCache = thinTargetCache;
        this.supplementaryDataFactory = supplementaryDataFactory;
    }

    /**
     * Convert a list of {@link TopologyEntityDTO}s to the appropriate {@link BusinessUnitApiDTO}s.
     *
     * @param entities The {@link TopologyEntityDTO} representations of the accounts.
     * @param allAccounts A hint to say whether these accounts represent ALL accounts. This
     *                    allows us to optimize queries for related data, if necessary.
     * @return The {@link BusinessUnitApiDTO} representations of the input accounts.
     */
    @Nonnull
    public List<BusinessUnitApiDTO> convert(@Nonnull final List<TopologyEntityDTO> entities,
            final boolean allAccounts) {
        if (entities.isEmpty()) {
            return Collections.emptyList();
        }

        final Set<Long> accountIds =
                entities.stream().map(TopologyEntityDTO::getOid).collect(Collectors.toSet());
        final SupplementaryData supplementaryData =
                supplementaryDataFactory.newSupplementaryData(accountIds, allAccounts);

        return entities.stream()
                .map(entity -> buildDiscoveredBusinessUnitApiDTO(entity, supplementaryData))
                .collect(Collectors.toList());
    }

    /**
     * Build discovered business unit API DTO.
     *
     * @param businessAccount topology entity DTOP for the business account
     * @param supplementaryData Supplementary data required to produce the final API DTO.
     * @return The API DTO that can be returned to the UI.
     */
    private BusinessUnitApiDTO buildDiscoveredBusinessUnitApiDTO(@Nonnull final TopologyEntityDTO businessAccount,
            @Nonnull final SupplementaryData supplementaryData) {
        final BusinessUnitApiDTO businessUnitApiDTO = new BusinessUnitApiDTO();
        final long businessAccountOid = businessAccount.getOid();
        businessUnitApiDTO.setBusinessUnitType(BusinessUnitType.DISCOVERED);
        businessUnitApiDTO.setUuid(Long.toString(businessAccountOid));
        businessUnitApiDTO.setEnvironmentType(EnvironmentType.CLOUD);
        businessUnitApiDTO.setClassName(UIEntityType.BUSINESS_ACCOUNT.apiStr());
        businessUnitApiDTO.setBudget(new StatApiDTO());

        supplementaryData.getCostPrice(businessAccountOid)
                .ifPresent(businessUnitApiDTO::setCostPrice);
        businessUnitApiDTO.setResourceGroupsCount(
                supplementaryData.getResourceGroupCount(businessAccountOid));
        // discovered account doesn't have discount (yet)
        businessUnitApiDTO.setDiscount(0.0f);

        businessUnitApiDTO.setMemberType(StringConstants.WORKLOAD);

        final MutableInt workloadMemberCount = new MutableInt(0);
        final Set<String> childAccountIds = new HashSet<>();
        businessAccount.getConnectedEntityListList().forEach(connectedEntity -> {
            UIEntityType type = UIEntityType.fromType(connectedEntity.getConnectedEntityType());
            if (UIEntityType.WORKLOAD_ENTITY_TYPES.contains(type)) {
                workloadMemberCount.increment();
            }
            if (type == UIEntityType.BUSINESS_ACCOUNT) {
                childAccountIds.add(Long.toString(connectedEntity.getConnectedEntityId()));
            }
        });

        businessUnitApiDTO.setMembersCount(workloadMemberCount.intValue());
        businessUnitApiDTO.setChildrenBusinessUnits(childAccountIds);

        businessUnitApiDTO.setDisplayName(businessAccount.getDisplayName());
        if (businessAccount.getTypeSpecificInfo().hasBusinessAccount()) {
            final BusinessAccountInfo bizInfo = businessAccount.getTypeSpecificInfo().getBusinessAccount();
            if (bizInfo.hasAccountId()) {
                businessUnitApiDTO.setAccountId(bizInfo.getAccountId());
            }
            if (bizInfo.hasAssociatedTargetId()) {
                businessUnitApiDTO.setAssociatedTargetId(
                        bizInfo.getAssociatedTargetId());
            }
            businessUnitApiDTO.setPricingIdentifiers(bizInfo.getPricingIdentifiersList()
                    .stream()
                    .collect(Collectors.toMap(pricingId -> pricingId.getIdentifierName().name(),
                            PricingIdentifier::getIdentifierValue)));
        }

        businessUnitApiDTO.setMaster(childAccountIds.size() > 0);

        final List<ThinTargetInfo> discoveringTargets = businessAccount
                .getOrigin()
                .getDiscoveryOrigin()
                .getDiscoveredTargetDataMap().keySet()
                .stream()
                .map(thinTargetCache::getTargetInfo)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());

        final CloudType cloudType = discoveringTargets.stream()
                .findFirst()
                .map(ThinTargetInfo::probeInfo)
                .map(ThinProbeInfo::type)
                .map(probeType -> com.vmturbo.common.protobuf.search.CloudType.fromProbeType(
                        probeType).orElse(null))
                .map(cloud -> CloudType.fromSimilarEnum(cloud).orElse(null))
                .orElse(CloudType.UNKNOWN);

        businessUnitApiDTO.setCloudType(cloudType);

        final List<TargetApiDTO> targetApiDTOS = discoveringTargets.stream()
                .map(thinTargetInfo -> {
                    final TargetApiDTO apiDTO = new TargetApiDTO();
                    apiDTO.setType(thinTargetInfo.probeInfo().type());
                    apiDTO.setUuid(Long.toString(thinTargetInfo.oid()));
                    apiDTO.setDisplayName(thinTargetInfo.displayName());
                    apiDTO.setCategory(thinTargetInfo.probeInfo().category());
                    return apiDTO;
                })
                .collect(Collectors.toList());
        businessUnitApiDTO.setTargets(targetApiDTOS);
        return businessUnitApiDTO;
    }
}

