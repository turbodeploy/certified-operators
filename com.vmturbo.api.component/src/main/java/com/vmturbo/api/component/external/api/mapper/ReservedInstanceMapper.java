package com.vmturbo.api.component.external.api.mapper;

import static com.vmturbo.api.component.external.api.mapper.UuidMapper.UI_REAL_TIME_MARKET_STR;
import static com.vmturbo.api.component.external.api.util.ApiUtils.isGlobalScope;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Lists;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.reservedinstance.ReservedInstanceApiDTO;
import com.vmturbo.api.dto.statistic.EntityStatsApiDTO;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.dto.statistic.StatValueApiDTO;
import com.vmturbo.api.dto.template.TemplateApiDTO;
import com.vmturbo.api.enums.PaymentOption;
import com.vmturbo.api.enums.Platform;
import com.vmturbo.api.enums.ReservedInstanceType;
import com.vmturbo.api.enums.Tenancy;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceBoughtCost;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceStatsRecord;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.platform.sdk.common.CloudCostDTO;

public class ReservedInstanceMapper {

    private final Logger logger = LogManager.getLogger();

    private final RepositoryApi repositoryApi;

    private static final String RESERVED_INSTANCE = "ReservedInstance";

    private static final String YEAR = "Year";

    private static final long NUM_OF_MILLISECONDS_OF_YEAR = 365L * 86400L * 1000L;

    private static final int NUM_OF_HOURS_OF_YEAR = 365 * 24;

    public ReservedInstanceMapper(@Nonnull final RepositoryApi repositoryApi) {
        this.repositoryApi = Objects.requireNonNull(repositoryApi);
    }

    /**
     * Convert {@link ReservedInstanceBought} and {@link ReservedInstanceSpec} to {@link ReservedInstanceApiDTO}.
     *
     * @param reservedInstanceBought a {@link ReservedInstanceBought}.
     * @param reservedInstanceSpec a {@link ReservedInstanceSpec}.
     * @param serviceEntityApiDTOMap a map which key is entity id, value is {@link ServiceEntityApiDTO}.
     *                               which contains full entity information if region, account,
     *                               availability zones entity.
     * @return a {@link ReservedInstanceApiDTO}.
     * @throws NotFoundMatchPaymentOptionException
     * @throws NotFoundMatchTenancyException
     * @throws NotFoundMatchOfferingClassException
     */
    public ReservedInstanceApiDTO mapToReservedInstanceApiDTO(
            @Nonnull final ReservedInstanceBought reservedInstanceBought,
            @Nonnull final ReservedInstanceSpec reservedInstanceSpec,
            @Nonnull final Map<Long, ServiceEntityApiDTO> serviceEntityApiDTOMap)
                throws NotFoundMatchPaymentOptionException, NotFoundMatchTenancyException,
                NotFoundMatchOfferingClassException {
        // TODO: set RI cost data which depends on discount information.
        ReservedInstanceApiDTO reservedInstanceApiDTO = new ReservedInstanceApiDTO();
        final ReservedInstanceBoughtInfo reservedInstanceBoughtInfo =
                reservedInstanceBought.getReservedInstanceBoughtInfo();
        // TODO: Also set master account of RI.
        reservedInstanceApiDTO.setLocation(createLocationBaseApi(reservedInstanceBoughtInfo, reservedInstanceSpec,
                serviceEntityApiDTOMap));
        final BaseApiDTO templateBaseDTO = createTemplateBaseApi(reservedInstanceSpec
                .getReservedInstanceSpecInfo().getTierId(), serviceEntityApiDTOMap);
        reservedInstanceApiDTO.setTemplate(templateBaseDTO);
        reservedInstanceApiDTO.setDisplayName(templateBaseDTO.getDisplayName());

        reservedInstanceApiDTO.setUuid(String.valueOf(reservedInstanceBought.getId()));
        reservedInstanceApiDTO.setClassName(RESERVED_INSTANCE);
        reservedInstanceApiDTO.setAccountId(String.valueOf(reservedInstanceBoughtInfo.getBusinessAccountId()));
        reservedInstanceApiDTO.setPayment(convertPaymentToApiDTO(
                reservedInstanceSpec.getReservedInstanceSpecInfo().getType().getPaymentOption()));
        reservedInstanceApiDTO.setPlatform(Platform.findFromOsName(
                reservedInstanceSpec.getReservedInstanceSpecInfo().getOs().name()));
        reservedInstanceApiDTO.setTenancy(Tenancy.getByName(
                reservedInstanceSpec.getReservedInstanceSpecInfo().getTenancy().name())
                .orElseThrow(() -> new NotFoundMatchTenancyException()));
        reservedInstanceApiDTO.setType(convertReservedInstanceTypeToApiDTO(
                reservedInstanceSpec.getReservedInstanceSpecInfo().getType().getOfferingClass()));
        reservedInstanceApiDTO.setInstanceCount(reservedInstanceBoughtInfo.getNumBought());
        // TODO: Apply discount to RI cost
        reservedInstanceApiDTO.setActualHourlyCost(reservedInstanceBoughtInfo
                .getReservedInstanceBoughtCost()
                .getRecurringCostPerHour()
                .getAmount());
        reservedInstanceApiDTO.setCostPrice(createStatApiDTO(StringConstants.DOLLARS_PER_HOUR,
                Optional.empty(), (float) getTotalHourlyCost(reservedInstanceBoughtInfo,
                        reservedInstanceSpec)));
        reservedInstanceApiDTO.setUpFrontCost(reservedInstanceBoughtInfo
                .getReservedInstanceBoughtCost()
                .getFixedCost()
                .getAmount());
        reservedInstanceApiDTO.setEffectiveHourlyCost(getTotalHourlyCost(reservedInstanceBoughtInfo,
                reservedInstanceSpec));
        // TODO: need to get on demand price of reserved instance from price table.
        reservedInstanceApiDTO.setOnDemandPrice(createStatApiDTO(StringConstants.DOLLARS_PER_HOUR, Optional.empty(), 0));
        reservedInstanceApiDTO.setCoupons(createStatApiDTO(StringConstants.RI_COUPON_UNITS,
                Optional.of((float) reservedInstanceBought.getReservedInstanceBoughtInfo()
                        .getReservedInstanceBoughtCoupons().getNumberOfCoupons()),
                (float) reservedInstanceBought.getReservedInstanceBoughtInfo()
                        .getReservedInstanceBoughtCoupons().getNumberOfCouponsUsed()));
        reservedInstanceApiDTO.setTerm(createStatApiDTO(YEAR, Optional.empty(),
                reservedInstanceSpec
                        .getReservedInstanceSpecInfo().getType().getTermYears()));
        final long endTime = reservedInstanceBought.getReservedInstanceBoughtInfo().getStartTime() +
                reservedInstanceSpec.getReservedInstanceSpecInfo().getType().getTermYears()
                        * NUM_OF_MILLISECONDS_OF_YEAR;
        reservedInstanceApiDTO.setExpDate(DateTimeUtil.toString(endTime));
        return reservedInstanceApiDTO;
    }

    /**
     * Convert reserved instance count map to a {@link EntityStatsApiDTO}.
     *
     * @param reservedInstanceCountMap a map which key is computer tier id, value is the count of
     *                                 reserved instance bought.
     * @param serviceEntityApiDTOMap a map which key is computer tier id, value is the full entity
     *                               information of the computer tier.
     * @return a {@link EntityStatsApiDTO}.
     */
    public EntityStatsApiDTO riCountMapToEntityStatsApiDTO(
            @Nonnull final  Map<Long, Long> reservedInstanceCountMap,
            @Nonnull final Map<Long, ServiceEntityApiDTO> serviceEntityApiDTOMap) {
        final EntityStatsApiDTO result = new EntityStatsApiDTO();
        final List<StatApiDTO> statApiDTOS =
                convertRiCountMapToStatApiDTO(reservedInstanceCountMap, serviceEntityApiDTOMap);
        final StatSnapshotApiDTO statSnapshotApiDTO = new StatSnapshotApiDTO();
        statSnapshotApiDTO.setStatistics(statApiDTOS);
        result.setStats(Lists.newArrayList(statSnapshotApiDTO));
        return result;
    }

    /**
     * convert a list of {@link ReservedInstanceStatsRecord} to {@link EntityStatsApiDTO}.
     *
     * @param records a list of {@link ReservedInstanceStatsRecord}
     * @param scope the scope of this request.
     * @param groupOptional a optional of {@link Group}.
     * @return a {@link EntityStatsApiDTO}.
     * @throws UnknownObjectException
     */
    public EntityStatsApiDTO convertRIUtilizationStatsRecordsToEntityStatsApiDTO(
            @Nonnull final List<ReservedInstanceStatsRecord> records,
            @Nonnull final String scope,
            @Nonnull final Optional<Group> groupOptional) throws UnknownObjectException {
        final EntityStatsApiDTO entityStatsApiDTO = new EntityStatsApiDTO();
        final List<StatSnapshotApiDTO> statSnapshotApiDTOS =
                convertRIStatsRecordsToStatSnapshotApiDTO(records, false);
        entityStatsApiDTO.setStats(statSnapshotApiDTOS);
        final String displayName = getScopeDisplayName(scope, groupOptional);
        entityStatsApiDTO.setDisplayName(displayName);
        return entityStatsApiDTO;
    }

    /**
     * Convert a list of {@link ReservedInstanceStatsRecord} to a list of {@link StatSnapshotApiDTO}.
     *
     * @param records a list of {@link ReservedInstanceStatsRecord}.
     * @param isRICoverage a boolean which true means it's a reserved instance coverage stats request,
     *                     false means it's a reserved instance utilization stats request.
     * @return a list {@link ReservedInstanceStatsRecord}.
     */
    public List<StatSnapshotApiDTO> convertRIStatsRecordsToStatSnapshotApiDTO(
            @Nonnull final List<ReservedInstanceStatsRecord> records,
            final boolean isRICoverage) {
        return records.stream()
                .map(record -> {
                    final StatSnapshotApiDTO snapshotApiDTO = new StatSnapshotApiDTO();
                    snapshotApiDTO.setDate(DateTimeUtil.toString(record.getSnapshotDate()));
                    final StatApiDTO statApiDTO = createRIUtilizationStatApiDTO(record, isRICoverage);
                    snapshotApiDTO.setStatistics(Lists.newArrayList(statApiDTO));
                    return snapshotApiDTO;
                })
                .collect(Collectors.toList());
    }

    /**
     * Get the scope display name based on input scope string and a optional of {@link Group}.
     * if it is Market scope, then return Market, if it is a Group, return group name, otherwise
     * it is a service entity, return the display name of the entity.
     *
     * @param scope the scope string.
     * @param groupOptional a optional of {@link Group}.
     * @return the scope name.
     * @throws UnknownObjectException if scope is a unknown entity.
     */
    private String getScopeDisplayName(@Nonnull final String scope,
                                       @Nonnull final Optional<Group> groupOptional)
            throws UnknownObjectException {
        if (isGlobalScope(scope, groupOptional)) {
            return UI_REAL_TIME_MARKET_STR;
        } else if (groupOptional.isPresent()) {
            return GroupProtoUtil.getGroupDisplayName(groupOptional.get());
        } else {
            final ServiceEntityApiDTO scopeEntity = repositoryApi.getServiceEntityForUuid(Long.valueOf(scope));
            return scopeEntity.getDisplayName();
        }
    }

    /**
     * Convert {@link CloudCostDTO.ReservedInstanceType.PaymentOption} to {@link PaymentOption}.
     *
     * @param paymentOption a {@link CloudCostDTO.ReservedInstanceType.PaymentOption}.
     * @return a {@link PaymentOption}.
     * @throws NotFoundMatchPaymentOptionException if can not find matched payment option.
     */
    private PaymentOption convertPaymentToApiDTO(
            @Nonnull final CloudCostDTO.ReservedInstanceType.PaymentOption paymentOption)
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
        StatApiDTO statsDto = new StatApiDTO();
        StatValueApiDTO statsValueDto = new StatValueApiDTO();
        statsValueDto.setMin(value);
        statsValueDto.setMax(value);
        statsValueDto.setAvg(value);
        statsValueDto.setTotal(value);
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
     * Create a {@link StatApiDTO} from input {@link ReservedInstanceStatsRecord}.
     *
     * @param record a {@link ReservedInstanceStatsRecord}.
     * @param isRICoverage a boolean which true means it's a reserved instance coverage stats request,
     *                     false means it's a reserved instance utilization stats request.
     * @return a {@link StatApiDTO}.
     */
    private StatApiDTO createRIUtilizationStatApiDTO(@Nonnull final ReservedInstanceStatsRecord record,
                                                     final boolean isRICoverage) {
        final String name = isRICoverage ? StringConstants.RI_COUPON_COVERAGE : StringConstants.RI_COUPON_UTILIZATION;
        StatApiDTO statsDto = new StatApiDTO();
        StatValueApiDTO statsValueDto = new StatValueApiDTO();
        statsValueDto.setAvg(record.getValues().getAvg());
        statsValueDto.setMax(record.getValues().getMax());
        statsValueDto.setMin(record.getValues().getMin());
        statsValueDto.setTotal(record.getValues().getTotal());
        statsDto.setValues(statsValueDto);
        StatValueApiDTO capacityDto = new StatValueApiDTO();
        capacityDto.setAvg(record.getCapacity().getAvg());
        capacityDto.setMax(record.getCapacity().getMax());
        capacityDto.setMin(record.getCapacity().getMin());
        capacityDto.setTotal(record.getCapacity().getTotal());
        statsDto.setCapacity(capacityDto);
        statsDto.setUnits(StringConstants.RI_COUPON_UNITS);
        statsDto.setName(name);
        statsDto.setValue(record.getValues().getAvg());
        return statsDto;
    }

    /**
     * Get the total hourly cost for the reserved instance bought.
     *
     * @param reservedInstanceBoughtInfo a {@link ReservedInstanceBoughtInfo}.
     * @param reservedInstanceSpec a {@link ReservedInstanceSpec}.
     * @return the total hourly cost.
     */
    private double getTotalHourlyCost(@Nonnull final ReservedInstanceBoughtInfo reservedInstanceBoughtInfo,
                                      @Nonnull final ReservedInstanceSpec reservedInstanceSpec) {
        final long hours = reservedInstanceSpec.getReservedInstanceSpecInfo()
                .getType().getTermYears() * NUM_OF_HOURS_OF_YEAR;
        final ReservedInstanceBoughtCost riBoughtCost = reservedInstanceBoughtInfo.getReservedInstanceBoughtCost();
        return (riBoughtCost.getFixedCost().getAmount() / hours) + riBoughtCost.getUsageCostPerHour().getAmount() +
                riBoughtCost.getRecurringCostPerHour().getAmount();
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
     * Convert reserved instance count map to a list of {@link StatApiDTO}.
     *
     * @param reservedInstanceCountMap a map which key is computer tier id, value is the count of
     *                                 reserved instance bought.
     * @param serviceEntityApiDTOMap a map which key is computer tier id, value is the full entity
     *                               information of the computer tier.
     * @return a list of {@link StatApiDTO}.
     */
    private List<StatApiDTO> convertRiCountMapToStatApiDTO(
            @Nonnull final  Map<Long, Long> reservedInstanceCountMap,
            @Nonnull final Map<Long, ServiceEntityApiDTO> serviceEntityApiDTOMap) {
        return reservedInstanceCountMap.keySet().stream()
                .map(riType ->
                        riCountToStatApiDTO(reservedInstanceCountMap.get(riType),
                                serviceEntityApiDTOMap.get(riType)))
                .collect(Collectors.toList());
    }

    /**
     * Convert a {@link ServiceEntityApiDTO} a {@link StatApiDTO}.
     *
     * @param value the count of reserved instance bought..
     * @param serviceEntityApiDTO a {@link ServiceEntityApiDTO}.
     * @return a {@link StatApiDTO}.
     */
    private StatApiDTO riCountToStatApiDTO(
            final Long value, @Nonnull final ServiceEntityApiDTO serviceEntityApiDTO) {
        final StatApiDTO statApiDTO = new StatApiDTO();
        StatValueApiDTO statsValueDto = new StatValueApiDTO();
        statsValueDto.setMin(value.floatValue());
        statsValueDto.setMax(value.floatValue());
        statsValueDto.setAvg(value.floatValue());
        statsValueDto.setTotal(value.floatValue());
        statApiDTO.setValue(value.floatValue());
        statApiDTO.setName(StringConstants.NUM_RI);
        statApiDTO.addFilter(StringConstants.TEMPLATE, serviceEntityApiDTO.getDisplayName());
        statApiDTO.setValues(statsValueDto);
        return statApiDTO;
    }

    public static class NotFoundMatchPaymentOptionException extends Exception {
        public NotFoundMatchPaymentOptionException() {
            super("Not found matched payment option!");
        }
    }

    public static class NotFoundMatchTenancyException extends Exception {
        public NotFoundMatchTenancyException() {
            super("Not found matched tenancy option!");
        }
    }

    public static class NotFoundMatchOfferingClassException extends Exception {
        public NotFoundMatchOfferingClassException() {
            super("Not found matched offering class option!");
        }
    }
}
