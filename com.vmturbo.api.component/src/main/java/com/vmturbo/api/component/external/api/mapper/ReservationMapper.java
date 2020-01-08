package com.vmturbo.api.component.external.api.mapper;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import javax.ws.rs.NotSupportedException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.util.TemplatesUtils;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.reservation.DemandEntityInfoDTO;
import com.vmturbo.api.dto.reservation.DemandReservationApiDTO;
import com.vmturbo.api.dto.reservation.DemandReservationApiInputDTO;
import com.vmturbo.api.dto.reservation.DemandReservationParametersDTO;
import com.vmturbo.api.dto.reservation.DeploymentParametersDTO;
import com.vmturbo.api.dto.reservation.PlacementInfoDTO;
import com.vmturbo.api.dto.reservation.PlacementParametersDTO;
import com.vmturbo.api.dto.template.ResourceApiDTO;
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupID;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.group.PolicyDTO.Policy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyRequest;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyResponse;
import com.vmturbo.common.protobuf.group.PolicyServiceGrpc.PolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.DeploymentProfile;
import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.DeploymentProfileInfo;
import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.DeploymentProfileInfo.ScopeAccessType;
import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.GetDeploymentProfileRequest;
import com.vmturbo.common.protobuf.plan.DeploymentProfileServiceGrpc.DeploymentProfileServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ReservationConstraintInfo;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.PlanChanges;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.TopologyAddition;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ConstraintInfoCollection;
import com.vmturbo.common.protobuf.plan.ReservationDTO.Reservation;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationStatus;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate.ReservationInstance;
import com.vmturbo.common.protobuf.plan.TemplateDTO.GetTemplateRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.plan.TemplateServiceGrpc.TemplateServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * A mapper class for convert Reservation related objects between Api DTO with XL DTO.
 */
public class ReservationMapper {

    private static final String PLACEMENT_SUCCEEDED = "PLACEMENT_SUCCEEDED";

    private static final String PLACEMENT_FAILED = "PLACEMENT_FAILED";

    private static final Map<Integer, ReservationConstraintInfo.Type> ENTITY_TYPE_TO_CONSTRAINT_TYPE =
        ImmutableMap.<Integer, ReservationConstraintInfo.Type>builder()
            .put(UIEntityType.DATACENTER.typeNumber(), ReservationConstraintInfo.Type.DATA_CENTER)
            .put(UIEntityType.VIRTUAL_DATACENTER.typeNumber(), ReservationConstraintInfo.Type.VIRTUAL_DATA_CENTER)
            .put(UIEntityType.NETWORK.typeNumber(), ReservationConstraintInfo.Type.NETWORK)
            .build();
    @VisibleForTesting
    static final String ONE_UNTRACKED_PLACEMENT_FAILED = "One untracked placement: Failed\n";

    private final Logger logger = LogManager.getLogger();

    private final RepositoryApi repositoryApi;

    private final GroupServiceBlockingStub groupServiceBlockingStub;

    private final TemplateServiceBlockingStub templateService;

    private final PolicyServiceBlockingStub policyService;

    private final DeploymentProfileServiceBlockingStub deploymentProfileService;

    ReservationMapper(@Nonnull final RepositoryApi repositoryApi,
                      @Nonnull final TemplateServiceBlockingStub templateService,
                      @Nonnull final GroupServiceBlockingStub groupServiceBlockingStub,
                      @Nonnull final PolicyServiceBlockingStub policyService,
                      @Nonnull final DeploymentProfileServiceBlockingStub deploymentProfileService) {
        this.repositoryApi = Objects.requireNonNull(repositoryApi);
        this.templateService = Objects.requireNonNull(templateService);
        this.groupServiceBlockingStub = Objects.requireNonNull(groupServiceBlockingStub);
        this.policyService = Objects.requireNonNull(policyService);
        this.deploymentProfileService = Objects.requireNonNull(deploymentProfileService);
    }

    /**
     * Convert a list of {@link DemandReservationParametersDTO} to one {@link ScenarioChange}.
     *
     * @param placementParameters a list of {@link DemandReservationParametersDTO}.
     * @return list of {@link ScenarioChange}.
     * @throws UnknownObjectException If one of the objects specified in the constraints is unrecognized.
     * @throws InvalidOperationException If arguments are not in the right format (e.g. numeric ids).
     */
    public List<ScenarioChange> placementToScenarioChange(
            @Nonnull final List<DemandReservationParametersDTO> placementParameters)
        throws UnknownObjectException, InvalidOperationException {
        // TODO: right now, it only support one type of templates for initial placement and reservation.
        if (placementParameters.size() != 1) {
            throw new NotSupportedException("Placement parameter size should be one.");
        }
        final PlacementParametersDTO placementDTO = placementParameters.get(0).getPlacementParameters();
        final TopologyAddition topologyAddition = TopologyAddition.newBuilder()
                .setAdditionCount(placementDTO.getCount())
                .setTemplateId(Long.valueOf(placementDTO.getTemplateID()))
                .build();
        final ScenarioChange scenarioTopologyAddition = ScenarioChange.newBuilder()
                .setTopologyAddition(topologyAddition)
                .build();
        final List<ScenarioChange> scenarioChanges = new ArrayList<>();
        scenarioChanges.add(scenarioTopologyAddition);

        final List<ReservationConstraintInfo> reservationConstraints = new ArrayList<>();
        if (placementDTO.getConstraintIDs() != null) {
            for (String constraintId : placementDTO.getConstraintIDs()) {
                try {
                    generateRelateConstraint(Long.valueOf(constraintId))
                        .ifPresent(reservationConstraints::add);
                } catch (NumberFormatException e) {
                    final String message = "Constraint ID must be numeric. Got: " + constraintId;
                    logger.error(message);
                    throw new InvalidOperationException(message);
                }
            }
        }

        final DeploymentParametersDTO deploymentParams =
            placementParameters.get(0).getDeploymentParameters();
        if (deploymentParams != null && !StringUtils.isEmpty(deploymentParams.getDeploymentProfileID())) {
            final long deploymentProfileId;
            try {
                deploymentProfileId = Long.parseLong(deploymentParams.getDeploymentProfileID());
            } catch (NumberFormatException e) {
                final String message = "Deployment profile ID must be numeric. Got: " + deploymentParams.getDeploymentProfileID();
                logger.error(message);
                throw new InvalidOperationException(message);
            }

            final DeploymentProfile deploymentProfile =
                deploymentProfileService.getDeploymentProfile(GetDeploymentProfileRequest.newBuilder()
                    .setDeploymentProfileId(deploymentProfileId)
                    .build());
            for (DeploymentProfileInfo.Scope scope : deploymentProfile.getDeployInfo().getScopesList()) {
                if (scope.getScopeAccessType() == ScopeAccessType.Or) {
                    if (scope.getIdsCount() > 0) {
                        // TODO (roman, Sept 5 2019) OM-50713: Add support for the OR context type.
                        logger.error("Unhandled deployment profile scope: {}", scope);
                    }
                } else if (scope.getScopeAccessType() == ScopeAccessType.And) {
                    for (Long scopeId : scope.getIdsList()) {
                        generateRelateConstraint(scopeId).ifPresent(reservationConstraints::add);
                    }
                }
            }
        }

        if (!reservationConstraints.isEmpty()) {
            scenarioChanges.add(ScenarioChange.newBuilder()
                .setPlanChanges(PlanChanges.newBuilder()
                    .addAllInitialPlacementConstraints(reservationConstraints))
                .build());
        }

        return scenarioChanges;
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
            generateRelateConstraint(constraintId).ifPresent(constraintInfos::add);
        }
        reservationBuilder.setConstraintInfoCollection(ConstraintInfoCollection.newBuilder()
                .addAllReservationConstraintInfo(constraintInfos)
                .build());
        return reservationBuilder.build();
    }

    /**
     * Convert a {@link ScenarioChange} and a list of {@link PlacementInfo} to a
     * {@link DemandReservationApiDTO}.
     *
     * @param topologyAddition contains information about template id and addition count.
     * @param placementInfos contains initial placement results.
     * @return {@link DemandReservationApiDTO}.
     * @throws UnknownObjectException if there are any unknown objects.
     */
    public DemandReservationApiDTO convertToDemandReservationApiDTO(
            @Nonnull TopologyAddition topologyAddition,
            @Nonnull final List<PlacementInfo> placementInfos) throws UnknownObjectException {
        final Template template = templateService.getTemplate(GetTemplateRequest.newBuilder()
                .setTemplateId(topologyAddition.getTemplateId())
                .build()).getTemplate();
        final Map<Long, ServiceEntityApiDTO> serviceEntityMap = getServiceEntityMap(placementInfos);
        /// If there exists an unplaced VM, then the initial placement failed.
        final String placementStatus = placementInfos.size() == topologyAddition.getAdditionCount() ?
            PLACEMENT_SUCCEEDED : PLACEMENT_FAILED;
        DemandReservationApiDTO reservationApiDTO =
                generateDemandReservationApiDTO(topologyAddition, placementStatus);
        final List<DemandEntityInfoDTO> demandEntityInfoDTOS = new ArrayList<>();
        for (PlacementInfo placementInfo : placementInfos) {
            try {
                demandEntityInfoDTOS.add(
                        generateDemandEntityInfoDTO(placementInfo, template, serviceEntityMap));
            } catch (ProviderIdNotRecognizedException e) {
                logger.error("Initial placement failed: " + e.getMessage());
                throw  new UnknownObjectException("Initial placement failed.");
            }
        }
        reservationApiDTO.setDemandEntities(demandEntityInfoDTOS);
        if (placementStatus == PLACEMENT_FAILED) {
            final int failedPlacements =
                    topologyAddition.getAdditionCount() - placementInfos.size();
            final StringBuilder builder = new StringBuilder(failedPlacements);
            for (int i = 0; i < failedPlacements; i++) {
                // TODO (GaryZeng Oct 18, 2019) append the failed placement details when they are available.
                // Appending the messages because UI is expecting detail placement messages
                // and use the messages to calculate failed count.
                // const failed =
                //      data.placementResultMessage !== undefined
                //          ? data.placementResultMessage
                //                .split('\n')
                //                .reduce(
                //                    (count, descr) => count + descr.includes(': Failed'),
                //                    0
                //                )
                //          : data.count; // fallback for XL, which has no placement result msg
                builder.append(ONE_UNTRACKED_PLACEMENT_FAILED);
            }
            reservationApiDTO.setPlacementResultMessage(builder.toString());
        }
        return reservationApiDTO;
    }

    /**
     * Convert {@link Reservation} to {@link DemandReservationApiDTO}. Right now, it only pick the
     * first ReservationTemplate, because currently Reservation only support one type of template.
     *
     * @param reservation {@link Reservation}.
     * @return {@link DemandReservationApiDTO}.
     * @throws UnknownObjectException if there are any unknown objects.
     */
    public DemandReservationApiDTO convertReservationToApiDTO(
            @Nonnull final Reservation reservation) throws UnknownObjectException {
        final DemandReservationApiDTO reservationApiDTO = new DemandReservationApiDTO();
        reservationApiDTO.setUuid(String.valueOf(reservation.getId()));
        reservationApiDTO.setDisplayName(reservation.getName());
        reservationApiDTO.setReserveDateTime(convertProtoDateToString(reservation.getStartDate()));
        reservationApiDTO.setExpireDateTime(convertProtoDateToString(reservation.getExpirationDate()));
        reservationApiDTO.setStatus(reservation.getStatus().toString());
        final List<ReservationTemplate> reservationTemplates =
                reservation.getReservationTemplateCollection().getReservationTemplateList();
        // Because right now, DemandReservationApiDTO support only one type of template for each
        // reservation, it is ok to only pick the first one.
        Optional<ReservationTemplate> reservationTemplate = reservationTemplates.stream().findFirst();
        if (reservationTemplate.isPresent()) {
            try {
                convertToDemandEntityDTO(reservationTemplate.get(),
                        reservationApiDTO);
            } catch (ProviderIdNotRecognizedException e) {
                // If there are providerId not found, it means this reservation is unplaced.
                logger.info("Reservation {} is unplaced: {}", reservation.getId(),
                        e.getMessage());
            }
        }
        return reservationApiDTO;
    }

    /**
     * Convert reservation's placement information to {@link DemandEntityInfoDTO}.
     *
     * @param reservationTemplate {@link ReservationTemplate} contains placement information by template.
     * @param reservationApiDTO {@link DemandReservationApiDTO}
     * @throws UnknownObjectException if there are any unknown objects.
     * @throws ProviderIdNotRecognizedException if there are provider id is not exist.
     */
    private void convertToDemandEntityDTO(@Nonnull final ReservationTemplate reservationTemplate,
                                          @Nonnull final DemandReservationApiDTO reservationApiDTO)
            throws UnknownObjectException, ProviderIdNotRecognizedException {
        reservationApiDTO.setCount(Math.toIntExact(reservationTemplate.getCount()));
        //TODO: need to make sure templates are always available, if templates are deleted, need to
        // mark Reservation not available or also delete related reservations.
        try {
            final Template template = templateService.getTemplate(GetTemplateRequest.newBuilder()
                    .setTemplateId(reservationTemplate.getTemplateId())
                    .build()).getTemplate();
            final List<PlacementInfo> placementInfos = reservationTemplate.getReservationInstanceList().stream()
                    .map(reservationInstance -> {
                        final List<Long> providerIds = reservationInstance.getPlacementInfoList().stream()
                                .map(ReservationInstance.PlacementInfo::getProviderId)
                                .collect(Collectors.toList());
                        return new PlacementInfo(reservationInstance.getEntityId(),
                                ImmutableList.copyOf(providerIds));
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
     * InvalidOperationException. And also it always set Reservation status to FUTURE, during next
     * broadcast time, Topology Processor component will update its status if it is start day comes.
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
        reservationBuilder.setStatus(ReservationStatus.FUTURE);
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
            final PolicyResponse response = policyService.getPolicy(PolicyRequest.newBuilder()
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
     *
     * @param placementInfos contains all initial placement results which only keep ids.
     * @return a map which key is entity id, value is {@link ServiceEntityApiDTO}.
     */
    private Map<Long, ServiceEntityApiDTO> getServiceEntityMap(
            @Nonnull final List<PlacementInfo> placementInfos) {
        final Set<Long> entitiesOid = placementInfos.stream()
                .map(placementInfo -> Sets.newHashSet(placementInfo.getProviderIds()))
                .flatMap(Set::stream)
                .collect(Collectors.toSet());

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
            throws UnknownObjectException, ProviderIdNotRecognizedException {
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
        templateApiDTO.setClassName(UIEntityType.fromType(
                template.getTemplateInfo().getEntityType()).apiStr() + TemplatesUtils.PROFILE);
        templateApiDTO.setUuid(String.valueOf(template.getId()));
        return templateApiDTO;
    }

    private PlacementInfoDTO createPlacementInfoDTO(
            @Nonnull final PlacementInfo placementInfo,
            @Nonnull final Map<Long, ServiceEntityApiDTO> serviceEntityApiDTOMap)
            throws UnknownObjectException, ProviderIdNotRecognizedException {
        PlacementInfoDTO placementInfoApiDTO = new PlacementInfoDTO();
        for (long providerId : placementInfo.getProviderIds()) {
            addResourcesApiDTO(providerId,
                    serviceEntityApiDTOMap, placementInfoApiDTO);
        }
        return placementInfoApiDTO;
    }

    /**
     * Creates related resource objects, based on the entity type. If it is Physical machine, it creates
     * compute resources, if it is Storage, it creates storage resources, if it is Network, it creates
     * network resources.
     *
     * @param providerId oid of provider.
     * @param serviceEntityApiDTOMap a Map which key is oid, value is {@link ServiceEntityApiDTO}.
     * @param placementInfoApiDTO {@link PlacementInfoDTO}.
     * @throws UnknownObjectException if there are entity types are not support.
     * @throws ProviderIdNotRecognizedException if there are provider id is not exist.
     */
    private void addResourcesApiDTO(final long providerId,
                                    @Nonnull Map<Long, ServiceEntityApiDTO> serviceEntityApiDTOMap,
                                    @Nonnull PlacementInfoDTO placementInfoApiDTO)
            throws UnknownObjectException, ProviderIdNotRecognizedException {
        Optional<ServiceEntityApiDTO> serviceEntityApiDTO = Optional.ofNullable(serviceEntityApiDTOMap.get(providerId));
        // if entity id is not present, it means this reservation is unplaced.
        if (!serviceEntityApiDTO.isPresent()) {
            throw  new ProviderIdNotRecognizedException(providerId);
        }
        final int entityType = UIEntityType.fromString(serviceEntityApiDTO.get().getClassName()).typeNumber();
        switch (entityType) {
            case EntityType.PHYSICAL_MACHINE_VALUE:
                final List<ResourceApiDTO> computeResources =
                        placementInfoApiDTO.getComputeResources() != null ?
                                placementInfoApiDTO.getComputeResources() :
                                new ArrayList<>();
                computeResources.add(generateResourcesApiDTO(serviceEntityApiDTO.get()));
                placementInfoApiDTO.setComputeResources(computeResources);
                break;
            case EntityType.STORAGE_VALUE:
                final List<ResourceApiDTO> storageResources =
                        placementInfoApiDTO.getStorageResources() != null ?
                                placementInfoApiDTO.getStorageResources() :
                                new ArrayList<>();
                storageResources.add(generateResourcesApiDTO(serviceEntityApiDTO.get()));
                placementInfoApiDTO.setStorageResources(storageResources);
                break;
            case EntityType.NETWORK_VALUE:
                final List<ResourceApiDTO> networkResources =
                        placementInfoApiDTO.getNetworkResources() != null ?
                                placementInfoApiDTO.getNetworkResources() :
                                new ArrayList<>();
                networkResources.add(generateResourcesApiDTO(serviceEntityApiDTO.get()));
                placementInfoApiDTO.setNetworkResources(networkResources);
                break;
            default:
                throw new UnknownObjectException("Unknown entity type: " + entityType);
        }
    }

    private ResourceApiDTO generateResourcesApiDTO(@Nonnull final ServiceEntityApiDTO serviceEntityApiDTO) {
        final BaseApiDTO providerBaseApiDTO = new BaseApiDTO();
        providerBaseApiDTO.setClassName(serviceEntityApiDTO.getClassName());
        providerBaseApiDTO.setDisplayName(serviceEntityApiDTO.getDisplayName());
        providerBaseApiDTO.setUuid(serviceEntityApiDTO.getUuid());
        final ResourceApiDTO resourceApiDTO = new ResourceApiDTO();
        resourceApiDTO.setProvider(providerBaseApiDTO);
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
        private final List<Long> providerIds;

        /**
         * Constructor.
         *
         * @param entityId The ID of the template entity.
         * @param providerIdList The list of provider OIDs.
         */
        public PlacementInfo(final long entityId, @Nonnull final List<Long> providerIdList) {
            this.entityId = entityId;
            this.providerIds = Collections.unmodifiableList(providerIdList);
        }

        public long getEntityId() {
            return this.entityId;
        }

        public List<Long> getProviderIds() {
            return this.providerIds;
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
