package com.vmturbo.api.component.external.api.mapper;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.ReservationNotificationDTO;
import com.vmturbo.api.ReservationNotificationDTO.ReservationNotification;
import com.vmturbo.api.ReservationNotificationDTO.ReservationStatusNotification;
import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.util.TemplatesUtils;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.reservation.DemandEntityInfoDTO;
import com.vmturbo.api.dto.reservation.DemandReservationApiDTO;
import com.vmturbo.api.dto.reservation.DemandReservationApiInputDTO;
import com.vmturbo.api.dto.reservation.DemandReservationParametersDTO;
import com.vmturbo.api.dto.reservation.PlacementInfoDTO;
import com.vmturbo.api.dto.reservation.PlacementParametersDTO;
import com.vmturbo.api.dto.reservation.ReservationConstraintApiDTO;
import com.vmturbo.api.dto.reservation.ReservationFailureInfoDTO;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatValueApiDTO;
import com.vmturbo.api.dto.template.ResourceApiDTO;
import com.vmturbo.api.exceptions.ConversionException;
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupID;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.group.PolicyDTO.Policy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyResponse;
import com.vmturbo.common.protobuf.group.PolicyDTO.SinglePolicyRequest;
import com.vmturbo.common.protobuf.group.PolicyServiceGrpc.PolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ConstraintInfoCollection;
import com.vmturbo.common.protobuf.plan.ReservationDTO.Reservation;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationChange;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationChanges;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationStatus;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ReservationConstraintInfo;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.TopologyAddition;
import com.vmturbo.common.protobuf.plan.TemplateDTO.GetTemplateRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.plan.TemplateServiceGrpc.TemplateServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityStats;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.UnplacementReason;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.UnplacementReason.FailedResources;
import com.vmturbo.components.common.ClassicEnumMapper.CommodityTypeUnits;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * A mapper class for convert Reservation related objects between Api DTO with XL DTO.
 */
public class ReservationMapper {

    private static final Map<Integer, ReservationConstraintInfo.Type> ENTITY_TYPE_TO_CONSTRAINT_TYPE =
        ImmutableMap.<Integer, ReservationConstraintInfo.Type>builder()
            .put(ApiEntityType.DATACENTER.typeNumber(), ReservationConstraintInfo.Type.DATA_CENTER)
            .put(ApiEntityType.VIRTUAL_DATACENTER.typeNumber(), ReservationConstraintInfo.Type.VIRTUAL_DATA_CENTER)
            .put(ApiEntityType.NETWORK.typeNumber(), ReservationConstraintInfo.Type.NETWORK)
            .build();

    private static final Map<Integer, Pair<String, String>> COMMODITY_TYPE_NAME_UNIT_MAP =
            ImmutableMap.<Integer, Pair<String, String>>builder()
                    .put(CommodityType.CPU_PROVISIONED_VALUE,
                            Pair.of(CommodityTypeUnits.CPU_PROVISIONED.getMixedCase(),
                                    CommodityTypeUnits.CPU_PROVISIONED.getUnits()))
                    .put(CommodityType.MEM_PROVISIONED_VALUE,
                            Pair.of(CommodityTypeUnits.MEM_PROVISIONED.getMixedCase(),
                                    CommodityTypeUnits.MEM_PROVISIONED.getUnits()))
                    .put(CommodityType.CPU_VALUE,
                            Pair.of(CommodityTypeUnits.CPU.getMixedCase(),
                            CommodityTypeUnits.CPU.getUnits()))
                    .put(CommodityType.MEM_VALUE,
                            Pair.of(CommodityTypeUnits.MEM.getMixedCase(),
                                    CommodityTypeUnits.MEM.getUnits()))
                    .put(CommodityType.IO_THROUGHPUT_VALUE,
                            Pair.of(CommodityTypeUnits.IO_THROUGHPUT.getMixedCase(),
                                    CommodityTypeUnits.IO_THROUGHPUT.getUnits()))
                    .put(CommodityType.NET_THROUGHPUT_VALUE,
                            Pair.of(CommodityTypeUnits.NET_THROUGHPUT.getMixedCase(),
                                    CommodityTypeUnits.NET_THROUGHPUT.getUnits()))
                    .put(CommodityType.STORAGE_AMOUNT_VALUE,
                            Pair.of(CommodityTypeUnits.STORAGE_AMOUNT.getMixedCase(),
                                    CommodityTypeUnits.STORAGE_AMOUNT.getUnits()))
                    .put(CommodityType.STORAGE_ACCESS_VALUE,
                            Pair.of(CommodityTypeUnits.STORAGE_ACCESS.getMixedCase(),
                                    CommodityTypeUnits.STORAGE_ACCESS.getUnits()))
                    .put(CommodityType.STORAGE_PROVISIONED_VALUE,
                            Pair.of(CommodityTypeUnits.STORAGE_PROVISIONED.getMixedCase(),
                                    CommodityTypeUnits.STORAGE_PROVISIONED.getUnits()))
                    .put(CommodityType.SEGMENTATION_VALUE,
                            Pair.of(CommodityTypeUnits.SEGMENTATION.getMixedCase(),
                                    CommodityTypeUnits.SEGMENTATION.getUnits()))
                    .build();

    private final Logger logger = LogManager.getLogger();

    private final RepositoryApi repositoryApi;

    private final GroupServiceBlockingStub groupServiceBlockingStub;

    private final TemplateServiceBlockingStub templateService;

    private final PolicyServiceBlockingStub policyService;

    ReservationMapper(@Nonnull final RepositoryApi repositoryApi,
                      @Nonnull final TemplateServiceBlockingStub templateService,
                      @Nonnull final GroupServiceBlockingStub groupServiceBlockingStub,
                      @Nonnull final PolicyServiceBlockingStub policyService) {
        this.repositoryApi = Objects.requireNonNull(repositoryApi);
        this.templateService = Objects.requireNonNull(templateService);
        this.groupServiceBlockingStub = Objects.requireNonNull(groupServiceBlockingStub);
        this.policyService = Objects.requireNonNull(policyService);
    }

    /**
     * Convert {@link DemandReservationApiInputDTO} to {@link Reservation}, it will always set status
     * to FUTURE, during the next broadcast cycle, status will be changed to RESERVED if its start
     * day comes.
     *
     * @param demandApiInputDTO {@link DemandReservationApiInputDTO}
     * @return a {@link Reservation}
     * @throws InvalidOperationException if input parameter are not correct.
     * @throws UnknownObjectException if there are any unknown objects.
     */
    public Reservation convertToReservation(
            @Nonnull final DemandReservationApiInputDTO demandApiInputDTO)
            throws InvalidOperationException, UnknownObjectException {
        final Reservation.Builder reservationBuilder = Reservation.newBuilder();
        reservationBuilder.setName(demandApiInputDTO.getDemandName());
        convertReservationDateStatus(demandApiInputDTO.getReserveDateTime(),
                demandApiInputDTO.getExpireDateTime(), reservationBuilder);
        final List<DemandReservationParametersDTO> placementParameters = demandApiInputDTO.getParameters();
        final List<ReservationTemplate> reservationTemplates = placementParameters.stream()
                .map(DemandReservationParametersDTO::getPlacementParameters)
                .map(this::convertToReservationTemplate)
                .collect(Collectors.toList());
        reservationBuilder.setReservationTemplateCollection(ReservationTemplateCollection.newBuilder()
                .addAllReservationTemplate(reservationTemplates)
                .build());
        final Set<Long> constraintIds = placementParameters.stream()
                .map(DemandReservationParametersDTO::getPlacementParameters)
                .map(PlacementParametersDTO::getConstraintIDs)
                .filter(Objects::nonNull)
                .flatMap(Set::stream)
                .map(Long::valueOf)
                .collect(Collectors.toSet());
        final List<ReservationConstraintInfo> constraintInfos = new ArrayList<>();
        for (Long constraintId : constraintIds) {
            constraintInfos.add(ReservationConstraintInfo.newBuilder()
                    .setConstraintId(constraintId).build());
        }
        reservationBuilder.setConstraintInfoCollection(ConstraintInfoCollection.newBuilder()
                .addAllReservationConstraintInfo(constraintInfos)
                .build());
        return reservationBuilder.build();
    }

    /**
     * Convert {@link Reservation} to {@link DemandReservationApiDTO}. Right now, it only pick the
     * first ReservationTemplate, because currently Reservation only support one type of template.
     *
     * @param reservation {@link Reservation}.
     * @return {@link DemandReservationApiDTO}.
     * @throws UnknownObjectException if there are any unknown objects.
     * @throws InterruptedException if thread has been interrupted
     * @throws ConversionException if errors faced during converting data to API DTOs
     */
    public DemandReservationApiDTO convertReservationToApiDTO(
            @Nonnull final Reservation reservation)
            throws UnknownObjectException, ConversionException, InterruptedException {
        final DemandReservationApiDTO reservationApiDTO = new DemandReservationApiDTO();
        reservationApiDTO.setUuid(String.valueOf(reservation.getId()));
        reservationApiDTO.setDisplayName(reservation.getName());
        reservationApiDTO.setReserveDateTime(convertProtoDateToString(reservation.getStartDate()));
        reservationApiDTO.setExpireDateTime(convertProtoDateToString(reservation.getExpirationDate()));
        reservationApiDTO.setStatus(reservation.getStatus().toString());
        List<ReservationConstraintApiDTO> constraintInfos = new ArrayList<>();
        if (reservation.hasConstraintInfoCollection() &&
                reservation.getConstraintInfoCollection()
                        .getReservationConstraintInfoList() != null) {
            for (ScenarioOuterClass.ReservationConstraintInfo reservationConstraintInfo :
                    reservation.getConstraintInfoCollection().getReservationConstraintInfoList()) {
                constraintInfos.add(
                        new ReservationConstraintApiDTO(String.valueOf(reservationConstraintInfo
                                .getConstraintId()),
                                reservationConstraintInfo.getType().name()));
            }
        }
        reservationApiDTO.setConstraintInfos(constraintInfos);
        final List<ReservationTemplate> reservationTemplates =
                reservation.getReservationTemplateCollection().getReservationTemplateList();
        // Because right now, DemandReservationApiDTO support only one type of template for each
        // reservation, it is ok to only pick the first one.
        Optional<ReservationTemplate> reservationTemplate = reservationTemplates.stream().findFirst();
        if (reservationTemplate.isPresent()) {
            convertToDemandEntityDTO(reservationTemplate.get(),
                        reservationApiDTO);
        }
        return reservationApiDTO;
    }

    /**
     * Create a {@link ReservationNotification} from a {@link ReservationChanges}.
     *
     * @param reservationChanges the the current batch of reservation changes.
     * @return a reservation notification that conforms to what the API layer will broadcast.
     */
    public static ReservationNotification notificationFromReservationChanges(@Nonnull final ReservationChanges reservationChanges) {
        final ReservationStatusNotification.Builder notificationBuilder = ReservationStatusNotification.newBuilder();
        for (ReservationChange resChange : reservationChanges.getReservationChangeList()) {
            notificationBuilder.addReservationStatus(
                ReservationNotificationDTO.ReservationStatus.newBuilder()
                    .setId(Long.toString(resChange.getId()))
                    .setStatus(resChange.getStatus().toString())
                    .build());
        }
        return ReservationNotification.newBuilder()
            .setStatusNotification(notificationBuilder.build())
            .build();
    }

    /**
     * Convert reservation's placement information to {@link DemandEntityInfoDTO}.
     *
     * @param reservationTemplate {@link ReservationTemplate} contains placement information by template.
     * @param reservationApiDTO {@link DemandReservationApiDTO}
     * @throws UnknownObjectException if there are any unknown objects.
     * @throws InterruptedException if thread has been interrupted
     * @throws ConversionException if errors faced during converting data to API DTOs
     */
    private void convertToDemandEntityDTO(@Nonnull final ReservationTemplate reservationTemplate,
                                          @Nonnull final DemandReservationApiDTO reservationApiDTO)
            throws UnknownObjectException, ConversionException, InterruptedException {
        reservationApiDTO.setCount(Math.toIntExact(reservationTemplate.getCount()));
        //TODO: need to make sure templates are always available, if templates are deleted, need to
        // mark Reservation not available or also delete related reservations.
        try {
            final Template template = reservationTemplate.hasTemplate()
                    ? reservationTemplate.getTemplate()
                    : templateService.getTemplate(GetTemplateRequest.newBuilder()
                    .setTemplateId(reservationTemplate.getTemplateId())
                    .build()).getTemplate();

            final List<PlacementInfo> placementInfos = reservationTemplate.getReservationInstanceList().stream()
                    .map(reservationInstance -> {
                        final List<ProviderInfo> providerInfos = reservationInstance.getPlacementInfoList().stream()
                                .map(a -> new ProviderInfo(
                                        a.hasProviderId()
                                                ? Optional.of(a.getProviderId()) : Optional.empty(),
                                        a.getCommodityBoughtList(),
                                        a.hasClusterId()
                                                ? Optional.of(a.getClusterId()) : Optional.empty(),
                                        a.getCommodityStatsList()))
                                .collect(Collectors.toList());
                        return new PlacementInfo(reservationInstance.getEntityId(),
                                ImmutableList.copyOf(providerInfos), reservationInstance.getUnplacedReasonList());
                    }).collect(Collectors.toList());
            final Map<Long, ServiceEntityApiDTO> serviceEntityMap = getServiceEntityMap(placementInfos);
            final List<DemandEntityInfoDTO> demandEntityInfoDTOS = new ArrayList<>();
            for (PlacementInfo placementInfo : placementInfos) {
                demandEntityInfoDTOS.add(
                        generateDemandEntityInfoDTO(placementInfo, template, serviceEntityMap));
            }
            reservationApiDTO.setDemandEntities(demandEntityInfoDTOS);
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode().equals(Code.NOT_FOUND)) {
                logger.error("Error: " + e.getMessage());
                return;
            } else {
                throw e;
            }
        }
    }

    /**
     * Validate the input start day and expire date, if there is any wrong input date, it will throw
     * InvalidOperationException. And also it always set Reservation status to INITIAL, during create
     * Reservation, we will update its status if it is start day comes.
     *
     * @param reserveDateStr start date of reservation, and date format is: yyyy-MM-ddT00:00:00Z.
     * @param expireDateStr expire date of reservation, and date format is: yyyy-MM-ddT00:00:00Z.
     * @param reservationBuilder builder of Reservation.
     * @throws InvalidOperationException if input start date and expire date are illegal.
     */
    private void convertReservationDateStatus(@Nonnull final String reserveDateStr,
                                        @Nonnull final String expireDateStr,
                                        @Nonnull final Reservation.Builder reservationBuilder)
            throws InvalidOperationException {
        if (reserveDateStr == null) {
            throw new InvalidOperationException("Reservation date is missing.");
        }
        if (expireDateStr == null) {
            throw new InvalidOperationException("Expiry date is missing.");
        }
        if (reservationBuilder == null) {
            throw new InvalidOperationException("reservationBuilder is missing.");
        }

        // Right now, UI set reservation start date to empty string when start date is current date,
        // after UI fix this issue, we can remove empty string covert here.
        final long reserveDate = reserveDateStr.isEmpty() ? Instant.now().toEpochMilli() :
                DateTimeUtil.parseIso8601TimeAndAdjustTimezone(reserveDateStr, null);
        final long expireDate = DateTimeUtil.parseIso8601TimeAndAdjustTimezone(expireDateStr, null);
        if (reserveDate > expireDate) {
            throw new InvalidOperationException("Reservation expire date should be after start date.");
        }
        reservationBuilder.setStartDate(reserveDate);
        reservationBuilder.setExpirationDate(expireDate);
        final long today = Instant.now().toEpochMilli();
        if (today > expireDate) {
            throw new InvalidOperationException("Reservation expire date should be after current date.");
        }
        reservationBuilder.setStatus(ReservationStatus.INITIAL);
    }

    private ReservationTemplate convertToReservationTemplate(
            @Nonnull final PlacementParametersDTO placementParameter) {
        return ReservationTemplate.newBuilder()
                .setCount(placementParameter.getCount())
                .setTemplateId(Long.valueOf(placementParameter.getTemplateID()))
                .build();
    }

    /**
     * For input constraint id, try to create {@link ReservationConstraintInfo}. Right now, it only
     * support Cluster, Data center, Virtual data center constraints.
     *
     * @param constraintId id of constraints.
     * @return {@link ReservationConstraintInfo}
     * @throws UnknownObjectException if there are any unknown objects.
     */
    private Optional<ReservationConstraintInfo> generateRelateConstraint(final long constraintId)
            throws UnknownObjectException {
        final ReservationConstraintInfo.Builder constraint = ReservationConstraintInfo.newBuilder()
                .setConstraintId(constraintId);
        final GroupID groupID = GroupID.newBuilder()
                .setId(constraintId)
                .build();
        // try Cluster constraint first, if not, will try data center and virtual data center constraints.
        // Because UI doesn't tell us what type this id belongs to, we need to try different api
        // call to find out constraint type.
        // TODO: After UI changes to send constraint type, we can avoid these api calls.
        final GetGroupResponse getGroupResponse = groupServiceBlockingStub.getGroup(groupID);
        if (getGroupResponse.hasGroup() && GroupProtoUtil.CLUSTER_GROUP_TYPES.contains(getGroupResponse
                        .getGroup().getDefinition().getType())) {
            return Optional.of(constraint.setType(ReservationConstraintInfo.Type.CLUSTER).build());
        }
        // TODO: (OM-30821) implement validation check for policy constraint. For example: if reservation
        // entity type is VM, but the policy consumer and provider group doesn't related with VM
        // type, it should throw exception.
        if (getPolicyConstraint(constraintId).isPresent()) {
            return Optional.of(constraint.setType(ReservationConstraintInfo.Type.POLICY).build());
        }
        final int entityType = repositoryApi.entityRequest(constraintId)
            .getMinimalEntity()
            .orElseThrow(() -> new UnknownObjectException("Unknown constraint id: " + constraintId))
            .getEntityType();

        final ReservationConstraintInfo.Type constraintType = ENTITY_TYPE_TO_CONSTRAINT_TYPE.get(entityType);
        if (constraintType == null) {
            throw new UnknownObjectException("Unknown type for constraint id: " + constraintId);
        } else {
            return Optional.of(constraint.setType(constraintType).build());
        }
    }

    /**
     * Return a policy if input constraint id is a policy Id.
     *
     * @param constraintId id of constraint.
     * @return a optional of {@link Policy}.
     */
    private Optional<Policy> getPolicyConstraint(final long constraintId) {
        try {
            final PolicyResponse response = policyService.getPolicy(SinglePolicyRequest.newBuilder()
                    .setPolicyId(constraintId)
                    .build());
            return response.hasPolicy() ? Optional.of(response.getPolicy()) : Optional.empty();
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode().equals(Code.NOT_FOUND) ||
                    e.getStatus().getCode().equals(Code.INVALID_ARGUMENT)) {
                // not find a policy.
                return Optional.empty();
            } else {
                throw e;
            }
        }
    }

    /**
     * Convert timestamp to {@link java.util.Date} string with default UTC timezone.
     *
     * @param timestamp milliseconds.
     * @return  {@link java.util.Date} string.
     */
    private String convertProtoDateToString(@Nonnull final long timestamp) {
        return DateTimeUtil.toString(timestamp);
    }

    /**
     * Send request to repository to fetch entity information by entity ids.
     * This set contains provider oid if placed or closest seller oid if not placed.
     *
     * @param placementInfos contains all initial placement results which only keep ids.
     * @return a map which key is entity id, value is {@link ServiceEntityApiDTO}.
     * @throws ConversionException if error faced converting objects to API DTOs
     * @throws InterruptedException if current thread has been interrupted
     */
    private Map<Long, ServiceEntityApiDTO> getServiceEntityMap(
            @Nonnull final List<PlacementInfo> placementInfos)
            throws ConversionException, InterruptedException {
        // This set contains provider oid if placed or closest seller oid if not placed.
        Set<Long> entitiesOid = new HashSet<>();
        for (PlacementInfo placementInfo : placementInfos) {
            for (ProviderInfo providerInfo : placementInfo.getProviderInfos()) {
                if (providerInfo.getProviderId().isPresent()) {
                    entitiesOid.add(providerInfo.getProviderId().get());
                }
            }
            for (UnplacementReason reason : placementInfo.getUnpalcementReasons()) {
                entitiesOid.add(reason.getClosestSeller());
            }
        }

        final Map<Long, ServiceEntityApiDTO> serviceEntityMap =
            repositoryApi.entitiesRequest(entitiesOid)
                .getSEMap();
        return serviceEntityMap;
    }

    private DemandReservationApiDTO generateDemandReservationApiDTO(
            @Nonnull final TopologyAddition topologyAddition,
            @Nonnull final String placementStatus) {
        DemandReservationApiDTO reservationApiDTO = new DemandReservationApiDTO();
        reservationApiDTO.setCount(topologyAddition.getAdditionCount());
        reservationApiDTO.setStatus(placementStatus);
        return reservationApiDTO;
    }

    private DemandEntityInfoDTO generateDemandEntityInfoDTO(
            @Nonnull final PlacementInfo placementInfo,
            @Nonnull final Template template,
            @Nonnull Map<Long, ServiceEntityApiDTO> serviceEntityApiDTOMap)
            throws UnknownObjectException {
        DemandEntityInfoDTO demandEntityInfoDTO = new DemandEntityInfoDTO();
        demandEntityInfoDTO.setTemplate(generateTemplateBaseApiDTO(template));
        PlacementInfoDTO placementInfoApiDTO =
                createPlacementInfoDTO(placementInfo, serviceEntityApiDTOMap);
        demandEntityInfoDTO.setPlacements(placementInfoApiDTO);
        return demandEntityInfoDTO;
    }

    private BaseApiDTO generateTemplateBaseApiDTO(@Nonnull final Template template) {
        BaseApiDTO templateApiDTO = new BaseApiDTO();
        templateApiDTO.setDisplayName(template.getTemplateInfo().getName());
        if (template.getTemplateInfo().hasEntityType()) {
            templateApiDTO.setClassName(ApiEntityType.fromType(
                    template.getTemplateInfo().getEntityType()).apiStr() + TemplatesUtils.PROFILE);
        } else {
            templateApiDTO.setClassName("TEMP-" + TemplatesUtils.PROFILE);
        }
        templateApiDTO.setUuid(String.valueOf(template.getId()));
        return templateApiDTO;
    }

    private PlacementInfoDTO createPlacementInfoDTO(
            @Nonnull final PlacementInfo placementInfo,
            @Nonnull final Map<Long, ServiceEntityApiDTO> serviceEntityApiDTOMap)
            throws UnknownObjectException {
        PlacementInfoDTO placementInfoApiDTO = new PlacementInfoDTO();
        addReservationFailureInfoDTO(placementInfo.getUnpalcementReasons(),
                placementInfoApiDTO,
                serviceEntityApiDTOMap);
        for (ProviderInfo providerInfo : placementInfo.getProviderInfos()) {
            if (providerInfo.getProviderId().isPresent()) {
                try {
                    addResourcesApiDTO(providerInfo,
                            serviceEntityApiDTOMap, placementInfoApiDTO);
                } catch (ProviderIdNotRecognizedException e) {
                    // If there are providerId not found, it means this reservation is unplaced.
                    logger.error("providerId not found", e);
                }
            }
        }
        return placementInfoApiDTO;
    }

    /**
     * Create failure info for reservations that failed.
     *
     * @param reasons                 list of unplacement reasons.
     * @param placementInfoApiDTO    {@link PlacementInfoDTO}.
     * @param serviceEntityApiDTOMap a Map which key is oid, value is {@link ServiceEntityApiDTO}.
     */
    private void addReservationFailureInfoDTO(List<UnplacementReason> reasons,
                                              PlacementInfoDTO placementInfoApiDTO,
                                              @Nonnull Map<Long, ServiceEntityApiDTO> serviceEntityApiDTOMap) {
        for (UnplacementReason reason : reasons) {
            Optional<ServiceEntityApiDTO> serviceEntityApiDTO = Optional
                    .ofNullable(serviceEntityApiDTOMap
                            .get(reason.getClosestSeller()));
            if (!serviceEntityApiDTO.isPresent()) {
                return;
            }
            final BaseApiDTO providerBaseApiDTO = new BaseApiDTO();
            providerBaseApiDTO.setClassName(serviceEntityApiDTO.get().getClassName());
            providerBaseApiDTO.setDisplayName(serviceEntityApiDTO.get().getDisplayName());
            providerBaseApiDTO.setUuid(serviceEntityApiDTO.get().getUuid());
            if (reason.getFailedResourcesList().isEmpty()) {
                logger.warn("Unplacement reason resource list is empty for service entity {}",
                        serviceEntityApiDTO.get().getDisplayName());
                break;
            }
            FailedResources failedResource = reason.getFailedResourcesList().get(0);
            Pair<String, String> resource = COMMODITY_TYPE_NAME_UNIT_MAP.get(
                    failedResource.getCommType().getType()) == null
                    ? Pair.of(CommodityType.UNKNOWN.name(), CommodityType.UNKNOWN.name())
                    : COMMODITY_TYPE_NAME_UNIT_MAP.get(failedResource.getCommType().getType());
            placementInfoApiDTO.getFailureInfos().add(new ReservationFailureInfoDTO(
                    resource.getLeft(),
                    providerBaseApiDTO,
                    failedResource.getMaxAvailable(),
                    failedResource.getRequestedAmount(),
                    resource.getRight()));
        }
    }

    /**
     * Creates related resource objects, based on the entity type. If it is Physical machine, it creates
     * compute resources, if it is Storage, it creates storage resources, if it is Network, it creates
     * network resources.
     *
     * @param providerInfo oid of provider.
     * @param serviceEntityApiDTOMap a Map which key is oid, value is {@link ServiceEntityApiDTO}.
     * @param placementInfoApiDTO {@link PlacementInfoDTO}.
     * @throws UnknownObjectException if there are entity types are not support.
     * @throws ProviderIdNotRecognizedException if there are provider id is not exist.
     */
    private void addResourcesApiDTO(final ProviderInfo providerInfo,
                                    @Nonnull Map<Long, ServiceEntityApiDTO> serviceEntityApiDTOMap,
                                    @Nonnull PlacementInfoDTO placementInfoApiDTO)
            throws UnknownObjectException, ProviderIdNotRecognizedException {
        if (!providerInfo.getProviderId().isPresent()) {
            // should not be happening. The caller already verifies this.
            return;
        }
        Optional<ServiceEntityApiDTO> serviceEntityApiDTO = Optional
                .ofNullable(serviceEntityApiDTOMap
                        .get(providerInfo.getProviderId().get()));
        // if entity id is not present, it means this reservation is unplaced.
        if (!serviceEntityApiDTO.isPresent() && providerInfo.getProviderId().isPresent()) {
            throw  new ProviderIdNotRecognizedException(providerInfo.getProviderId().get());
        }
        final int entityType = ApiEntityType.fromString(serviceEntityApiDTO.get().getClassName()).typeNumber();
        Optional<String> clusterId = providerInfo.getClusterId().isPresent()
                ? Optional.of(String.valueOf(providerInfo.getClusterId().get())) : Optional.empty();
        if (clusterId.isPresent() && !placementInfoApiDTO.getRelatedResources().stream()
                .anyMatch(a -> a.getResourceId().equals(clusterId.get()))) {
            placementInfoApiDTO.getRelatedResources()
                    .add(generateClusterResourcesApiDTO(clusterId.get(),
                            providerInfo.getCommodityStats()));
        }
        switch (entityType) {
            case EntityType.PHYSICAL_MACHINE_VALUE:
                final List<ResourceApiDTO> computeResources =
                        placementInfoApiDTO.getComputeResources() != null ?
                                placementInfoApiDTO.getComputeResources() :
                                new ArrayList<>();
                computeResources.add(generateResourcesApiDTO(serviceEntityApiDTO.get(), providerInfo.getCommoditiesBought(), clusterId));
                placementInfoApiDTO.setComputeResources(computeResources);
                break;
            case EntityType.STORAGE_VALUE:
                final List<ResourceApiDTO> storageResources =
                        placementInfoApiDTO.getStorageResources() != null ?
                                placementInfoApiDTO.getStorageResources() :
                                new ArrayList<>();
                storageResources.add(generateResourcesApiDTO(serviceEntityApiDTO.get(), providerInfo.getCommoditiesBought(), clusterId));
                placementInfoApiDTO.setStorageResources(storageResources);
                break;
            case EntityType.NETWORK_VALUE:
                final List<ResourceApiDTO> networkResources =
                        placementInfoApiDTO.getNetworkResources() != null ?
                                placementInfoApiDTO.getNetworkResources() :
                                new ArrayList<>();
                networkResources.add(generateResourcesApiDTO(serviceEntityApiDTO.get(), providerInfo.getCommoditiesBought(), clusterId));
                placementInfoApiDTO.setNetworkResources(networkResources);
                break;
            default:
                throw new UnknownObjectException("Unknown entity type: " + entityType);
        }
    }

    /**
     * Populate cluster stats in the api response of reservation.
     *
     * @param clusterId the cluster id where the reservation is placed.
     * @param commodityStats the stats of the cluster.
     * @return cluster stats wrapped as ResourceApiDTO.
     */
    private ResourceApiDTO generateClusterResourcesApiDTO(@Nonnull final String clusterId,
                                                   List<CommodityStats> commodityStats) {
        final ResourceApiDTO resourceApiDTO = new ResourceApiDTO();
        resourceApiDTO.setResourceId(clusterId);
        List<StatApiDTO> statApiDTOS = new ArrayList<>();
        for (CommodityStats commodityStat : commodityStats) {
            Pair<String, String> commodityNameUnit = COMMODITY_TYPE_NAME_UNIT_MAP
                    .get(commodityStat.getCommodityType().getType());
            if (commodityNameUnit != null) {
                final StatApiDTO statApiDTO = new StatApiDTO();
                statApiDTO.setName(commodityNameUnit.getLeft());
                statApiDTO.setUnits(commodityNameUnit.getRight());
                final StatValueApiDTO statValueUsedApiDTO = new StatValueApiDTO();
                statValueUsedApiDTO.setAvg((float)commodityStat.getTotalUsed());
                statApiDTO.setValues(statValueUsedApiDTO);
                final StatValueApiDTO statValueCapacityApiDTO = new StatValueApiDTO();
                statValueCapacityApiDTO.setAvg((float)commodityStat.getTotalCapacity());
                statApiDTO.setCapacity(statValueCapacityApiDTO);
                statApiDTOS.add(statApiDTO);
            }
        }
        resourceApiDTO.setStats(statApiDTOS);
        return resourceApiDTO;
    }

    /**
     * Populate the stats for each of the commodity bought by the VM associated with the reservation.
     * We need the stats info to distinguish between different disk buying from different storages.
     *
     * @param serviceEntityApiDTO  The provider service entity
     * @param commodityBoughtDTOList the commodities bought by the VM associated with the reservation.
     * @param clusterID the id of the cluster the provider belongs to.
     * @return ResourceApiDTO populated with providerID and the stats.
     */
    private ResourceApiDTO generateResourcesApiDTO(@Nonnull final ServiceEntityApiDTO serviceEntityApiDTO,
                                                   List<CommodityBoughtDTO> commodityBoughtDTOList,
                                                   Optional<String> clusterID) {
        final BaseApiDTO providerBaseApiDTO = new BaseApiDTO();
        providerBaseApiDTO.setClassName(serviceEntityApiDTO.getClassName());
        providerBaseApiDTO.setDisplayName(serviceEntityApiDTO.getDisplayName());
        providerBaseApiDTO.setUuid(serviceEntityApiDTO.getUuid());
        final ResourceApiDTO resourceApiDTO = new ResourceApiDTO();
        resourceApiDTO.setProvider(providerBaseApiDTO);
        List<StatApiDTO> statApiDTOS = new ArrayList<>();
        for (CommodityBoughtDTO commodityBoughtDTO : commodityBoughtDTOList) {
            Pair<String, String> commodityNameUnit = COMMODITY_TYPE_NAME_UNIT_MAP.get(commodityBoughtDTO.getCommodityType().getType());
            if (commodityNameUnit != null) {
                final StatApiDTO statApiDTO = new StatApiDTO();
                statApiDTO.setName(commodityNameUnit.getLeft());
                statApiDTO.setUnits(commodityNameUnit.getRight());
                statApiDTO.setValue((float)commodityBoughtDTO.getUsed());
                final StatValueApiDTO statValueApiDTO = new StatValueApiDTO();
                statValueApiDTO.setAvg((float)commodityBoughtDTO.getUsed());
                statApiDTO.setValues(statValueApiDTO);
                statApiDTOS.add(statApiDTO);
            }
        }
        resourceApiDTO.setStats(statApiDTOS);
        if (clusterID.isPresent()) {
            resourceApiDTO.getRelatedResources().add(clusterID.get());
        }
        return resourceApiDTO;
    }

    /**
     * A wrapper class to store initial placement results.
     */
    @Immutable
    public static class PlacementInfo {
        // oid of added template entity
        private final long entityId;
        // a list of oids which are the providers of entityId.
        private final List<ProviderInfo> providerInfos;
        // failure information when reservation fails.
        private final List<UnplacementReason> reasons;

        /**
         * Constructor.
         *
         * @param entityId         The ID of the template entity.
         * @param providerInfoList The list of provider OIDs.
         * @param reasons          The reasons for failed reservations.
         */
        public PlacementInfo(final long entityId,
                             @Nonnull final List<ProviderInfo> providerInfoList,
                             @Nonnull final List<UnplacementReason> reasons) {
            this.entityId = entityId;
            this.providerInfos = Collections.unmodifiableList(providerInfoList);
            this.reasons = reasons;
        }

        /**
         * Getter method for failureInfos.
         * @return the failureInfos.
         */
        public List<UnplacementReason> getUnpalcementReasons() {
            return reasons;
        }

        public long getEntityId() {
            return this.entityId;
        }



        public List<ProviderInfo> getProviderInfos() {
            return this.providerInfos;
        }
    }

    /**
     * A wrapper class to store the oid of provider and the commodities bought by the template from
     * the provider.
     */
    @Immutable
    public static class ProviderInfo {

        private final Optional<Long> providerId;
        private final List<CommodityBoughtDTO> commoditiesBought;
        // the id of the cluster where the entity is placed on.
        private final Optional<Long> clusterId;
        // the stats of the cluster.
        private final List<CommodityStats> commodityStats;

        /**
         * Constructor.
         *
         * @param providerId       the oid of the provider
         * @param commoditiesBought the commodities bought by the template from the provider.
         * @param clusterId the id of the cluster where the entity is placed on.
         * @param commodityStats the stats of the cluster.
         */
        public ProviderInfo(final Optional<Long> providerId,
                            @Nonnull final List<CommodityBoughtDTO> commoditiesBought,
                            Optional<Long> clusterId,
                            List<CommodityStats> commodityStats) {
            this.providerId = providerId;
            this.commoditiesBought = Collections.unmodifiableList(commoditiesBought);
            this.clusterId = clusterId;
            this.commodityStats = commodityStats;
        }

        public Optional<Long> getProviderId() {
            return providerId;
        }

        public List<CommodityBoughtDTO> getCommoditiesBought() {
            return commoditiesBought;
        }

        /**
         * getter for commodityStats.
         * @return the commodityStats.
         */
        public List<CommodityStats> getCommodityStats() {
            return commodityStats;
        }

        /**
         * getter for clusterId.
         * @return the clusterId.
         */
        public Optional<Long> getClusterId() {
            return clusterId;
        }
    }

    /**
     * A exception represents provider id is not recognized in current topology.
     */
    private static class ProviderIdNotRecognizedException extends Exception {
        ProviderIdNotRecognizedException(@Nonnull final long id) {
            super("Provider Id: " + id + " not found.");
        }
    }
}
