package com.vmturbo.api.component.external.api.mapper;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.reservedinstance.ReservedInstanceApiDTO;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatValueApiDTO;
import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.api.dto.template.TemplateApiDTO;
import com.vmturbo.api.enums.AzureRIScopeType;
import com.vmturbo.api.enums.CloudType;
import com.vmturbo.api.enums.PaymentOption;
import com.vmturbo.api.enums.Platform;
import com.vmturbo.api.enums.ReservedInstanceType;
import com.vmturbo.api.enums.Tenancy;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.AccountReferenceType;
import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceScopeInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.platform.sdk.common.CloudCostDTO;
import com.vmturbo.platform.sdk.common.CommonCost;

import org.springframework.util.CollectionUtils;

/**
 * Conversion class for reserved instances.
 */
public class ReservedInstanceMapper {

    private static final Logger logger = LogManager.getLogger();

    private static final String RESERVED_INSTANCE = "ReservedInstance";

    private static final String YEAR = "Year";

    private static final long NUM_OF_MILLISECONDS_OF_YEAR = 365L * 86400L * 1000L;

    private final CloudTypeMapper cloudTypeMapper;

    /**
     * Constructor for {@code ReservedInstanceMapper}.
     *
     * @param cloudTypeMapper {@link CloudTypeMapper} instance.
     */
    public ReservedInstanceMapper(final CloudTypeMapper cloudTypeMapper) {
        this.cloudTypeMapper = cloudTypeMapper;
    }

    /**
     * Convert {@link ReservedInstanceBought} and {@link ReservedInstanceSpec} to {@link ReservedInstanceApiDTO}.
     *
     * @param reservedInstanceBought a {@link ReservedInstanceBought}.
     * @param reservedInstanceSpec a {@link ReservedInstanceSpec}.
     * @param serviceEntityApiDTOMap a map which key is entity id, value is {@link ServiceEntityApiDTO}.
     *                               which contains full entity information if region, account,
     *                               availability zones entity.
     * @param coveredEntitiesCount count of workload entities covered by the reserved instance.
     * @param coveredUndiscoveredAccountsCount count of undiscovered accounts covered
     *                               by the reserved instance.
     * @param relatedBusinessAccountsList list of Topology Entity DTO's for related business accounts of the RI
     * @return a {@link ReservedInstanceApiDTO}.
     * @throws NotFoundMatchPaymentOptionException when no matching payment option can be found.
     * @throws NotFoundMatchTenancyException when no matching tenancy can be found.
     * @throws NotFoundMatchOfferingClassException when no matching offering class can be found.
     */
    public ReservedInstanceApiDTO mapToReservedInstanceApiDTO(
            @Nonnull final ReservedInstanceBought reservedInstanceBought,
            @Nonnull final ReservedInstanceSpec reservedInstanceSpec,
            @Nonnull final Map<Long, ServiceEntityApiDTO> serviceEntityApiDTOMap,
            @Nullable final Integer coveredEntitiesCount,
            @Nullable final Integer coveredUndiscoveredAccountsCount,
            @Nullable final List<TopologyDTO.TopologyEntityDTO> relatedBusinessAccountsList)
                throws NotFoundMatchPaymentOptionException, NotFoundMatchTenancyException,
                NotFoundMatchOfferingClassException, NotFoundCloudTypeException {
        // TODO: set RI cost data which depends on discount information.

        final ReservedInstanceApiDTO reservedInstanceApiDTO = new ReservedInstanceApiDTO();
        final ReservedInstanceBoughtInfo reservedInstanceBoughtInfo =
                reservedInstanceBought.getReservedInstanceBoughtInfo();
        final ReservedInstanceSpecInfo riSpecInfo = reservedInstanceSpec.getReservedInstanceSpecInfo();

        reservedInstanceApiDTO.setLocation(createLocationBaseApi(reservedInstanceBoughtInfo, reservedInstanceSpec,
                serviceEntityApiDTOMap));
        final BaseApiDTO templateBaseDTO = createTemplateBaseApi(reservedInstanceSpec
                .getReservedInstanceSpecInfo().getTierId(), serviceEntityApiDTOMap);
        reservedInstanceApiDTO.setTemplate(templateBaseDTO);
        if (riSpecInfo.hasSizeFlexible()) {
            reservedInstanceApiDTO.setSizeFlexible(riSpecInfo.getSizeFlexible());
        }
        reservedInstanceApiDTO.setDisplayName(reservedInstanceBoughtInfo.getDisplayName());
        reservedInstanceApiDTO.setUuid(String.valueOf(reservedInstanceBought.getId()));
        reservedInstanceApiDTO.setClassName(RESERVED_INSTANCE);

        if (reservedInstanceBoughtInfo.hasBusinessAccountId()) {
            final long accountId = reservedInstanceBoughtInfo.getBusinessAccountId();
            reservedInstanceApiDTO.setAccountId(String.valueOf(accountId));
            final ServiceEntityApiDTO businessAccount = serviceEntityApiDTOMap.get(accountId);
            if (businessAccount != null) {
                reservedInstanceApiDTO.setAccountDisplayName(businessAccount.getDisplayName());
            }
            // set associated target ID if available
            if (!CollectionUtils.isEmpty(relatedBusinessAccountsList)) {
                // RI will always have one associated business account/subscription
                TopologyDTO.TopologyEntityDTO baTopologyEntityDTO =
                        relatedBusinessAccountsList.stream().filter(account -> account.getOid() == accountId)
                                .findFirst().orElse(null);
                if (baTopologyEntityDTO != null
                        && baTopologyEntityDTO.getTypeSpecificInfo().getBusinessAccount().hasAssociatedTargetId()) {
                    reservedInstanceApiDTO
                            .setTargetId(Long.toString(baTopologyEntityDTO.getTypeSpecificInfo()
                                    .getBusinessAccount().getAssociatedTargetId()));
                }
            }
        }

        reservedInstanceApiDTO.setPayment(convertPaymentToApiDTO(
                riSpecInfo.getType().getPaymentOption()));
        reservedInstanceApiDTO.setPlatform(Platform.findFromOsName(
                riSpecInfo.getOs().name()));
        reservedInstanceApiDTO.setTenancy(Tenancy.getByName(
                riSpecInfo.getTenancy().name())
                .orElseThrow(NotFoundMatchTenancyException::new));
        reservedInstanceApiDTO.setType(convertReservedInstanceTypeToApiDTO(
                riSpecInfo.getType().getOfferingClass()));
        reservedInstanceApiDTO.setInstanceCount(reservedInstanceBoughtInfo.getNumBought());
        // TODO: Apply discount to RI cost

        // Set the recurring hourly cost for an RI.
        reservedInstanceApiDTO.setActualHourlyCost(reservedInstanceBoughtInfo
                .getReservedInstanceBoughtCost()
                .getRecurringCostPerHour()
                .getAmount());

        // Set the upfront hourly cost for an RI.
        reservedInstanceApiDTO.setUpFrontCost(reservedInstanceBoughtInfo
                .getReservedInstanceBoughtCost()
                .getFixedCost()
                .getAmount());

        // Set the effective hourly cost for an RI.(Recurring + UpFront)
        reservedInstanceApiDTO.setEffectiveHourlyCost(reservedInstanceBoughtInfo
                .getReservedInstanceDerivedCost()
                .getAmortizedCostPerHour()
                .getAmount());

        reservedInstanceApiDTO.setCostPrice(createStatApiDTO(StringConstants.DOLLARS_PER_HOUR,
                Optional.empty(), (float)reservedInstanceBoughtInfo
                        .getReservedInstanceDerivedCost()
                        .getAmortizedCostPerHour()
                        .getAmount()));

        reservedInstanceApiDTO.setOnDemandPrice(createStatApiDTO(StringConstants.DOLLARS_PER_HOUR,
                Optional.empty(), (float)reservedInstanceBoughtInfo
                .getReservedInstanceDerivedCost()
                .getOnDemandRatePerHour()
                .getAmount()));

        reservedInstanceApiDTO.setCoupons(createStatApiDTO(StringConstants.RI_COUPON_UNITS,
                Optional.of((float)reservedInstanceBought.getReservedInstanceBoughtInfo()
                        .getReservedInstanceBoughtCoupons().getNumberOfCoupons()),
                (float)reservedInstanceBought.getReservedInstanceBoughtInfo()
                        .getReservedInstanceBoughtCoupons().getNumberOfCouponsUsed()));
        reservedInstanceApiDTO.setTerm(createStatApiDTO(YEAR, Optional.empty(),
                riSpecInfo.getType().getTermYears()));
        //if endTime is available use that instead of startTime + termYears.
        final long endTime =
                reservedInstanceBought.getReservedInstanceBoughtInfo().hasEndTime() ?
                        reservedInstanceBought.getReservedInstanceBoughtInfo().getEndTime() :
                        (reservedInstanceBought.getReservedInstanceBoughtInfo().getStartTime() +
                                riSpecInfo.getType().getTermYears()
                                        * NUM_OF_MILLISECONDS_OF_YEAR);
        reservedInstanceApiDTO.setExpDateEpochTime(endTime);
        reservedInstanceApiDTO.setExpDate(DateTimeUtil.toString(endTime));
        reservedInstanceApiDTO.setCloudType(retrieveCloudType(reservedInstanceSpec,
                serviceEntityApiDTOMap));

        // The following properties are used for Azure
        reservedInstanceApiDTO.setTrueID(reservedInstanceBoughtInfo.getProbeReservedInstanceId());
        reservedInstanceApiDTO.setOrderID(reservedInstanceBoughtInfo.getReservationOrderId());

        if (reservedInstanceBoughtInfo.hasReservedInstanceScopeInfo()) {
            final ReservedInstanceScopeInfo reservedInstanceScopeInfo =
                    reservedInstanceBoughtInfo.getReservedInstanceScopeInfo();
            final List<Long> applicableBusinessAccountIdList =
                    reservedInstanceScopeInfo.getApplicableBusinessAccountIdList();
            if (!applicableBusinessAccountIdList.isEmpty()) {
                reservedInstanceApiDTO.setScopeType(AzureRIScopeType.SINGLE);
                reservedInstanceApiDTO.setAppliedScopes(
                    reservedInstanceBoughtInfo.getReservedInstanceScopeInfo()
                        .getApplicableBusinessAccountIdList()
                        .stream()
                        .map(oid -> {
                            final ServiceEntityApiDTO account = serviceEntityApiDTOMap.get(oid);
                            if (account == null) {
                                logger.error("Cannot find account specified in applied scopes: {}",
                                        oid);
                            }
                            return account;
                        })
                        .filter(Objects::nonNull)
                        .map(BaseApiDTO::getDisplayName)
                        .collect(Collectors.toList()));
            } else if (reservedInstanceScopeInfo.hasShared()) {
                reservedInstanceApiDTO.setScopeType(reservedInstanceScopeInfo.getShared()
                        ? AzureRIScopeType.SHARED : AzureRIScopeType.SINGLE);
            } else {
                reservedInstanceApiDTO.setScopeType(AzureRIScopeType.UNKNOWN);
            }
        }

        reservedInstanceApiDTO.setCoveredEntityCount(coveredEntitiesCount);
        reservedInstanceApiDTO.setUndiscoveredAccountsCoveredCount(
                coveredUndiscoveredAccountsCount);
        // Set the toBuy field based on the value in the ReservedInstanceBoughtInfo
        reservedInstanceApiDTO.setToBuy(reservedInstanceBoughtInfo.getToBuy());

        return reservedInstanceApiDTO;
    }

    /**
     * Convert {@link CommonCost.PaymentOption} to {@link PaymentOption}.
     *
     * @param paymentOption a {@link CommonCost.PaymentOption}.
     * @return a {@link PaymentOption}.
     * @throws NotFoundMatchPaymentOptionException if can not find matched payment option.
     */
    private PaymentOption convertPaymentToApiDTO(
            @Nonnull final CommonCost.PaymentOption paymentOption)
        throws NotFoundMatchPaymentOptionException {
        switch (paymentOption) {
            case ALL_UPFRONT:
                return PaymentOption.ALL_UPFRONT;
            case PARTIAL_UPFRONT:
                return PaymentOption.PARTIAL_UPFRONT;
            case NO_UPFRONT:
                return PaymentOption.NO_UPFRONT;
            default:
                logger.error("Can not find matched payment option: " + paymentOption);
                throw new NotFoundMatchPaymentOptionException();
        }
    }

    /**
     * Convert a {@link CloudCostDTO.ReservedInstanceType.OfferingClass} to {@link ReservedInstanceType}.
     *
     * @param offeringClass a {@link CloudCostDTO.ReservedInstanceType.OfferingClass}.
     * @return a {@link ReservedInstanceType}.
     * @throws NotFoundMatchOfferingClassException if can not find matched offering class type.
     */
    private ReservedInstanceType convertReservedInstanceTypeToApiDTO(
            @Nonnull final CloudCostDTO.ReservedInstanceType.OfferingClass offeringClass)
        throws NotFoundMatchOfferingClassException {
        switch (offeringClass) {
            case STANDARD:
                return ReservedInstanceType.STANDARD;
            case CONVERTIBLE:
                return ReservedInstanceType.CONVERTIBLE;
            default:
                logger.error("Can not find matched offering class type: " + offeringClass);
                throw new NotFoundMatchOfferingClassException();
        }
    }

    /**
     * Create {@link StatApiDTO} based on input units, capacity and value.
     *
     * @param units Units of stats. E.G. $/h
     * @param capacity the total capacity of this stats
     * @param value the value of stats.
     * @return a {@link StatApiDTO}.
     */
    private StatApiDTO createStatApiDTO(@Nonnull final String units,
                                        @Nonnull final Optional<Float> capacity,
                                        final float value) {
        StatValueApiDTO statsValueDto = new StatValueApiDTO();
        statsValueDto.setMin(value);
        statsValueDto.setMax(value);
        statsValueDto.setAvg(value);
        statsValueDto.setTotal(value);
        StatApiDTO statsDto = new StatApiDTO();
        statsDto.setValues(statsValueDto);
        statsDto.setValue(value);
        StatValueApiDTO capacityDto = new StatValueApiDTO();
        if (capacity.isPresent()) {
            capacityDto.setMin(capacity.get());
            capacityDto.setMax(capacity.get());
            capacityDto.setAvg(capacity.get());
        }
        statsDto.setCapacity(capacityDto);
        statsDto.setUnits(units);
        return statsDto;
    }

    /**
     * Create the local {@link BaseApiDTO} for the reserved instance bought.
     *
     * @param reservedInstanceBoughtInfo {@link ReservedInstanceBoughtInfo}.
     * @param reservedInstanceSpec {@link ReservedInstanceSpec}.
     * @param serviceEntityApiDTOMap a map which key is entity id, value is {@link ServiceEntityApiDTO}.
     *                               which contains full entity information if region, account,
     *                               availability zones entity.
     * @return a {@link BaseApiDTO}.
     */
    private BaseApiDTO createLocationBaseApi(@Nonnull final ReservedInstanceBoughtInfo reservedInstanceBoughtInfo,
                                             @Nonnull final ReservedInstanceSpec reservedInstanceSpec,
                                             @Nonnull final Map<Long, ServiceEntityApiDTO> serviceEntityApiDTOMap) {
        final BaseApiDTO location = new BaseApiDTO();
        final long locationEntityId = reservedInstanceBoughtInfo.hasAvailabilityZoneId()
                ? reservedInstanceBoughtInfo.getAvailabilityZoneId()
                : reservedInstanceSpec.getReservedInstanceSpecInfo().getRegionId();
        if (serviceEntityApiDTOMap.containsKey(locationEntityId)) {
            final ServiceEntityApiDTO locationEntity = serviceEntityApiDTOMap.get(locationEntityId);
            location.setUuid(locationEntity.getUuid());
            location.setDisplayName(locationEntity.getDisplayName());
        }
        return  location;
    }

    /**
     * Create a {@link TemplateApiDTO} for the reserved instance bought.
     *
     * @param templateServiceEntityId the id of template entity.
     * @param serviceEntityApiDTOMap a map which key is entity id, value is {@link ServiceEntityApiDTO}.
     *                               which contains full entity information if region, account,
     *                               availability zones entity.
     * @return {@link BaseApiDTO}.
     */
    private BaseApiDTO createTemplateBaseApi(final long templateServiceEntityId,
                                             @Nonnull final Map<Long, ServiceEntityApiDTO> serviceEntityApiDTOMap) {
        final TemplateApiDTO template = new TemplateApiDTO();
        if (serviceEntityApiDTOMap.containsKey(templateServiceEntityId)) {
            final ServiceEntityApiDTO templateServiceEntityDTO = serviceEntityApiDTOMap.get(templateServiceEntityId);
            template.setUuid(templateServiceEntityDTO.getUuid());
            template.setDisplayName(templateServiceEntityDTO.getDisplayName());
        }
        return template;
    }

    /**
     * Retrieve reserved instance Cloud type from the RI spec Region.
     *
     * @param reservedInstanceSpec Reserved instance specification object.
     * @param serviceEntityApiDTOMap Map which key is entity id, value is {@link ServiceEntityApiDTO}.
     * @return {@link CloudType} instance for the given reserved instance.
     */
    @Nonnull
    private CloudType retrieveCloudType(
            @Nonnull final Cost.ReservedInstanceSpec reservedInstanceSpec,
            @Nonnull final Map<Long, ServiceEntityApiDTO> serviceEntityApiDTOMap)
            throws NotFoundCloudTypeException {
        Cost.ReservedInstanceSpecInfo reservedInstanceSpecInfo = reservedInstanceSpec.getReservedInstanceSpecInfo();
        if (!reservedInstanceSpecInfo.hasRegionId()) {
            throw new NotFoundCloudTypeException("No Region in RI Specification: " +
                    reservedInstanceSpec.getId());
        }
        final Long regionId = reservedInstanceSpecInfo.getRegionId();
        final ServiceEntityApiDTO region = serviceEntityApiDTOMap.get(regionId);
        if (region == null) {
            throw new NotFoundCloudTypeException("Cannot find Region with ID " +
                    regionId);
        }
        final TargetApiDTO targetApiDTO = region.getDiscoveredBy();
        if (targetApiDTO == null) {
            throw new NotFoundCloudTypeException("Missing target in Region " +
                    regionId);
        }
        final String targetType = targetApiDTO.getType();
        if (targetType == null) {
            throw new NotFoundCloudTypeException("Missing target type for target: " + targetApiDTO);
        }
        return cloudTypeMapper.fromTargetType(targetType)
            .orElseThrow(() -> new NotFoundCloudTypeException(
                "Cannot identify Cloud for target type: " + targetType));
    }

    /**
     * This exception is thrown when no matching payment option can be found.
     */
    public static class NotFoundMatchPaymentOptionException extends Exception {
        NotFoundMatchPaymentOptionException() {
            super("Not found matched payment option!");
        }
    }

    /**
     * This exception is thrown when no matching tenancy can be found.
     */
    public static class NotFoundMatchTenancyException extends Exception {
        NotFoundMatchTenancyException() {
            super("Not found matched tenancy option!");
        }
    }

    /**
     * This exception is thrown when no matching offering class can be found.
     */
    public static class NotFoundMatchOfferingClassException extends Exception {
        NotFoundMatchOfferingClassException() {
            super("Not found matched offering class option!");
        }
    }

    /**
     * This exception is thrown when no matching Cloud type can be found.
     */
    public static class NotFoundCloudTypeException extends Exception {
        NotFoundCloudTypeException(String message) {
            super(message);
        }
    }

    /**
     * Map an API account filter type string to an equivalent XL account filter type.
     *
     * @param accountFilterType The string representing the account filter type in UI.
     * @return An optional containing a {@link AccountReferenceType}, or an empty optional if
     *         no equivalent filter type exists in XL.
     */
    @Nonnull
    public static AccountReferenceType mapApiAccountFilterTypeToXl(
            @Nonnull final String accountFilterType) {
        switch (accountFilterType) {
            case "USED_BY":
                return AccountReferenceType.USED_BY;
            case "USED_AND_PURCHASED_BY":
                return AccountReferenceType.USED_AND_PURCHASED_BY;
            case "PURCHASED_BY":
                return AccountReferenceType.PURCHASED_BY;
            default:
                return AccountReferenceType.PURCHASED_BY;
        }
    }
}
