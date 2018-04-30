package com.vmturbo.api.component.external.api.mapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import javax.ws.rs.NotSupportedException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.ServiceEntitiesRequest;
import com.vmturbo.api.component.external.api.util.TemplatesUtils;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.reservation.DemandEntityInfoDTO;
import com.vmturbo.api.dto.reservation.DemandReservationApiDTO;
import com.vmturbo.api.dto.reservation.DemandReservationApiInputDTO;
import com.vmturbo.api.dto.reservation.DemandReservationParametersDTO;
import com.vmturbo.api.dto.reservation.PlacementInfoDTO;
import com.vmturbo.api.dto.reservation.PlacementParametersDTO;
import com.vmturbo.api.dto.template.ResourceApiDTO;
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupID;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.group.PolicyDTO.Policy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyRequest;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyResponse;
import com.vmturbo.common.protobuf.group.PolicyServiceGrpc.PolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.PlanDTO.ReservationConstraintInfo;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.PlanChanges;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.TopologyAddition;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ConstraintInfoCollection;
import com.vmturbo.common.protobuf.plan.ReservationDTO.Reservation;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationStatus;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate.ReservationInstance;
import com.vmturbo.common.protobuf.plan.TemplateDTO.GetTemplateRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.plan.TemplateServiceGrpc.TemplateServiceBlockingStub;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * A mapper class for convert Reservation related objects between Api DTO with XL DTO.
 */
public class ReservationMapper {

    private final Logger logger = LogManager.getLogger();

    private final RepositoryApi repositoryApi;

    private final GroupServiceBlockingStub groupServiceBlockingStub;

    private final TemplateServiceBlockingStub templateService;

    private final PolicyServiceBlockingStub policyService;

    private final String PLACEMENT_SUCCEEDED = "PLACEMENT_SUCCEEDED";

    private final String PLACEMENT_FAILED = "PLACEMENT_FAILED";

    public ReservationMapper(@Nonnull final RepositoryApi repositoryApi,
                             @Nonnull final TemplateServiceBlockingStub templateService,
                             @Nonnull final GroupServiceBlockingStub groupServiceBlockingStub,
                             @Nonnull final PolicyServiceBlockingStub policyService) {
        this.repositoryApi = Objects.requireNonNull(repositoryApi);
        this.templateService = Objects.requireNonNull(templateService);
        this.groupServiceBlockingStub = Objects.requireNonNull(groupServiceBlockingStub);
        this.policyService = Objects.requireNonNull(policyService);
    }

    /**
     * Convert a list of {@link DemandReservationParametersDTO} to one {@link ScenarioChange}.
     *
     * @param placementParameters a list of {@link DemandReservationParametersDTO}.
     * @return list of {@link ScenarioChange}.
     */
    public List<ScenarioChange> placementToScenarioChange(
            @Nonnull final List<DemandReservationParametersDTO> placementParameters)
            throws UnknownObjectException {
        // TODO: right now, it only support one type of templates for initial placement and reservation.
        if (placementParameters.size() != 1) {
            throw new NotSupportedException("Placement parameter size should be one.");
        }
        PlacementParametersDTO placementDTO = placementParameters.get(0).getPlacementParameters();
        TopologyAddition topologyAddition = TopologyAddition.newBuilder()
                .setAdditionCount(placementDTO.getCount())
                .setTemplateId(Long.valueOf(placementDTO.getTemplateID()))
                .build();
        final ScenarioChange ScenarioTopologyAddition = ScenarioChange.newBuilder()
                .setTopologyAddition(topologyAddition)
                .build();
        final List<ScenarioChange> scenarioChanges = new ArrayList<>();
        scenarioChanges.add(ScenarioTopologyAddition);
        if (placementDTO.getConstraintIDs() != null) {
            final PlanChanges planChange = createPlanChanges(placementDTO.getConstraintIDs());
            final ScenarioChange ScenarioPlanChange = ScenarioChange.newBuilder()
                    .setPlanChanges(planChange)
                    .build();
            scenarioChanges.add(ScenarioPlanChange);
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
            constraintInfos.add(generateRelateConstraint(constraintId));
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
                .build());
        final Map<Long, ServiceEntityApiDTO> serviceEntityMap = getServiceEntityMap(placementInfos);
        // if can not find placements, need to set status to placement failed.
        final String placementStatus = placementInfos.isEmpty() ? PLACEMENT_FAILED : PLACEMENT_SUCCEEDED;
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
                    .build());
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
            }
            else {
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
        if (reserveDateStr == null || reserveDateStr == null) {
            throw new InvalidOperationException("Reservation date is missing.");
        }
        // Right now, UI set reservation start date to empty string when start date is current date,
        // after UI fix this issue, we can remove empty string covert here.
        final DateTime reserveDate = reserveDateStr.isEmpty() ? DateTime.now(DateTimeZone.UTC) :
                DateTime.parse(reserveDateStr);
        final DateTime expireDate = DateTime.parse(expireDateStr);
        if (reserveDate.isAfter(expireDate)) {
            throw new InvalidOperationException("Reservation expire date should be after start date.");
        }
        reservationBuilder.setStartDate(reserveDate.getMillis());
        reservationBuilder.setExpirationDate(expireDate.getMillis());
        final DateTime today = DateTime.now(DateTimeZone.UTC);
        if (today.isAfter(expireDate)) {
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
     * Create {@link PlanChanges} object which only contains a list of
     * {@link ReservationConstraintInfo}. The {@link PlanChanges} will contains all constraints
     * which user specified when running initial placements.
     *
     * @param constraintIds a set of constraint ids.
     * @return {@link PlanChanges}
     * @throws UnknownObjectException if there are any unknown objects.
     */
    private PlanChanges createPlanChanges(@Nonnull final Set<String> constraintIds)
            throws UnknownObjectException {
        final PlanChanges.Builder planChanges = PlanChanges.newBuilder();
        final List<ReservationConstraintInfo> constraints = new ArrayList<>();
        for (String constraintId : constraintIds) {
            constraints.add(generateRelateConstraint(Long.valueOf(constraintId)));
        }
        return planChanges
                .addAllInitialPlacementConstraints(constraints)
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
    private ReservationConstraintInfo generateRelateConstraint(final long constraintId)
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
        if (getGroupResponse.hasGroup() && getGroupResponse.getGroup().getType() == Group.Type.CLUSTER) {
            return constraint.setType(ReservationConstraintInfo.Type.CLUSTER).build();
        }
        // TODO: (OM-30821) implement validation check for policy constraint. For example: if reservation
        // entity type is VM, but the policy consumer and provider group doesn't related with VM
        // type, it should throw exception.
        if (getPolicyConstraint(constraintId).isPresent()) {
            return constraint.setType(ReservationConstraintInfo.Type.POLICY).build();
        }
        final ServiceEntityApiDTO serviceEntityApiDTO = repositoryApi.getServiceEntityForUuid(constraintId);
        final int entityType = ServiceEntityMapper.fromUIEntityType(serviceEntityApiDTO.getClassName());
        if (entityType == EntityType.DATACENTER_VALUE) {
            return constraint.setType(ReservationConstraintInfo.Type.DATA_CENTER).build();
        } else if (entityType == EntityType.VIRTUAL_DATACENTER_VALUE) {
            return constraint.setType(ReservationConstraintInfo.Type.VIRTUAL_DATA_CENTER).build();
        } else {
            throw new UnknownObjectException("Unknown type for constraint id: " + constraintId);
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
            return Optional.of(response.getPolicy());
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
        return new DateTime(timestamp, DateTimeZone.UTC).toDate().toString();
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
                repositoryApi.getServiceEntitiesById(
                        ServiceEntitiesRequest.newBuilder(entitiesOid)
                                .build())
                        .entrySet().stream()
                        .filter(entry -> entry.getValue().isPresent())
                        .collect(Collectors.toMap(Entry::getKey, entry -> entry.getValue().get()));
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
        templateApiDTO.setClassName(ServiceEntityMapper.toUIEntityType(
                template.getTemplateInfo().getEntityType()) + TemplatesUtils.PROFILE);
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
        final int entityType = ServiceEntityMapper.fromUIEntityType(serviceEntityApiDTO.get().getClassName());
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
        ResourceApiDTO resourceApiDTO = new ResourceApiDTO();
        BaseApiDTO providerBaseApiDTO = new BaseApiDTO();
        providerBaseApiDTO.setClassName(serviceEntityApiDTO.getClassName());
        providerBaseApiDTO.setDisplayName(serviceEntityApiDTO.getDisplayName());
        providerBaseApiDTO.setUuid(serviceEntityApiDTO.getUuid());
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

        public PlacementInfo(@Nonnull final long entityId, @Nonnull final ImmutableList providerIdList) {
            this.entityId = entityId;
            this.providerIds = providerIdList;
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
        public ProviderIdNotRecognizedException(@Nonnull final long id) {
            super("Provider Id: " + id + " not found.");
        }
    }
}
