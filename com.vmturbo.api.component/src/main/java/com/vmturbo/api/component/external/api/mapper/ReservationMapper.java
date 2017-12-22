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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.ServiceEntitiesRequest;
import com.vmturbo.api.component.external.api.util.TemplatesUtils;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.reservation.DemandEntityInfoDTO;
import com.vmturbo.api.dto.reservation.DemandReservationApiDTO;
import com.vmturbo.api.dto.reservation.DemandReservationParametersDTO;
import com.vmturbo.api.dto.reservation.PlacementInfoDTO;
import com.vmturbo.api.dto.reservation.PlacementParametersDTO;
import com.vmturbo.api.dto.template.ResourceApiDTO;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupID;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.PlanChanges;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.PlanChanges.InitialPlacementConstraint;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.TopologyAddition;
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

    private final String PLACEMENT_SUCCEEDED = "PLACEMENT_SUCCEEDED";

    public ReservationMapper(@Nonnull final RepositoryApi repositoryApi,
                             @Nonnull final TemplateServiceBlockingStub templateService,
                             @Nonnull final GroupServiceBlockingStub groupServiceBlockingStub) {
        this.repositoryApi = Objects.requireNonNull(repositoryApi);
        this.templateService = Objects.requireNonNull(templateService);
        this.groupServiceBlockingStub = Objects.requireNonNull(groupServiceBlockingStub);
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
     * Create {@link PlanChanges} object which only contains a list of
     * {@link InitialPlacementConstraint}. The {@link PlanChanges} will contains all constraints
     * which user specified when running initial placements.
     *
     * @param constraintIds a set of constraint ids.
     * @return {@link PlanChanges}
     * @throws UnknownObjectException if there are any unknown objects.
     */
    private PlanChanges createPlanChanges(@Nonnull final Set<String> constraintIds)
            throws UnknownObjectException {
        final PlanChanges.Builder planChanges = PlanChanges.newBuilder();
        final List<InitialPlacementConstraint> constraints = new ArrayList<>();
        for (String constraintId : constraintIds) {
            constraints.add(generateRelateConstraint(Long.valueOf(constraintId)));
        }
        return planChanges
                .addAllInitialPlacementConstraints(constraints)
                .build();
    }

    /**
     * For input constraint id, try to create {@link InitialPlacementConstraint}. Right now, it only
     * support Cluster, Data center, Virtual data center constraints.
     *
     * @param constraintId id of constraints.
     * @return {@link InitialPlacementConstraint}
     * @throws UnknownObjectException if there are any unknown objects.
     */
    private InitialPlacementConstraint generateRelateConstraint(final long constraintId)
            throws UnknownObjectException {
        final InitialPlacementConstraint.Builder constraint = InitialPlacementConstraint.newBuilder()
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
            return constraint.setType(InitialPlacementConstraint.Type.CLUSTER).build();
        }
        final ServiceEntityApiDTO serviceEntityApiDTO = repositoryApi.getServiceEntityForUuid(constraintId);
        final int entityType = ServiceEntityMapper.fromUIEntityType(serviceEntityApiDTO.getClassName());
        if (entityType == EntityType.DATACENTER_VALUE) {
            return constraint.setType(InitialPlacementConstraint.Type.DATA_CENTER).build();
        } else if (entityType == EntityType.VIRTUAL_DATACENTER_VALUE) {
            return constraint.setType(InitialPlacementConstraint.Type.VIRTUAL_DATA_CENTER).build();
        } else {
            throw new UnknownObjectException("Unknown type for constraint id: " + constraintId);
        }
    }

    /**
     * Convert a {@link ScenarioChange} and a list of {@link PlacementInfo} to a
     * {@link DemandReservationApiDTO}.
     *
     * @param topologyAddition contains information about template id and addition count.
     * @param placementInfos contains initial placement results.
     * @return {@link DemandReservationApiDTO}.
     * @throws UnknownObjectException
     */
    public DemandReservationApiDTO convertToDemandReservationApiDTO(
            @Nonnull TopologyAddition topologyAddition,
            @Nonnull final List<PlacementInfo> placementInfos) throws UnknownObjectException {
        final Template template = templateService.getTemplate(GetTemplateRequest.newBuilder()
                .setTemplateId(topologyAddition.getTemplateId())
                .build());
        final Map<Long, ServiceEntityApiDTO> serviceEntityMap = getServiceEntityMap(placementInfos);
        DemandReservationApiDTO reservationApiDTO = generateDemandReservationApiDTO(topologyAddition);
        final List<DemandEntityInfoDTO> demandEntityInfoDTOS = new ArrayList<>();
        for (PlacementInfo placementInfo : placementInfos) {
            demandEntityInfoDTOS.add(
                    generateDemandEntityInfoDTO(placementInfo, template, serviceEntityMap));
        }
        reservationApiDTO.setDemandEntities(demandEntityInfoDTOS);
        return reservationApiDTO;
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
            @Nonnull final TopologyAddition topologyAddition) {
        DemandReservationApiDTO reservationApiDTO = new DemandReservationApiDTO();
        reservationApiDTO.setCount(topologyAddition.getAdditionCount());
        reservationApiDTO.setStatus(PLACEMENT_SUCCEEDED);
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
        templateApiDTO.setClassName(ServiceEntityMapper.toUIEntityType(
                template.getTemplateInfo().getEntityType()) + TemplatesUtils.PROFILE);
        templateApiDTO.setUuid(String.valueOf(template.getId()));
        return templateApiDTO;
    }

    private PlacementInfoDTO createPlacementInfoDTO(
            @Nonnull final PlacementInfo placementInfo,
            @Nonnull final Map<Long, ServiceEntityApiDTO> serviceEntityApiDTOMap)
            throws UnknownObjectException {
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
     * @throws UnknownObjectException
     */
    private void addResourcesApiDTO(final long providerId,
                                    @Nonnull Map<Long, ServiceEntityApiDTO> serviceEntityApiDTOMap,
                                    @Nonnull PlacementInfoDTO placementInfoApiDTO)
            throws UnknownObjectException {
        ServiceEntityApiDTO serviceEntityApiDTO = Optional.ofNullable(serviceEntityApiDTOMap.get(providerId))
                .orElseThrow(() -> {
                    logger.error("Can not found service entity for oid: " + providerId);
                    return new UnknownObjectException("Initial placement failed");
                });
        final int entityType = ServiceEntityMapper.fromUIEntityType(serviceEntityApiDTO.getClassName());
        switch (entityType) {
            case EntityType.PHYSICAL_MACHINE_VALUE:
                final List<ResourceApiDTO> computeResources =
                        placementInfoApiDTO.getComputeResources() != null ?
                                placementInfoApiDTO.getComputeResources() :
                                new ArrayList<>();
                computeResources.add(generateResourcesApiDTO(serviceEntityApiDTO));
                placementInfoApiDTO.setComputeResources(computeResources);
                break;
            case EntityType.STORAGE_VALUE:
                final List<ResourceApiDTO> storageResources =
                        placementInfoApiDTO.getStorageResources() != null ?
                                placementInfoApiDTO.getStorageResources() :
                                new ArrayList<>();
                storageResources.add(generateResourcesApiDTO(serviceEntityApiDTO));
                placementInfoApiDTO.setStorageResources(storageResources);
                break;
            case EntityType.NETWORK_VALUE:
                final List<ResourceApiDTO> networkResources =
                        placementInfoApiDTO.getNetworkResources() != null ?
                                placementInfoApiDTO.getNetworkResources() :
                                new ArrayList<>();
                networkResources.add(generateResourcesApiDTO(serviceEntityApiDTO));
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
}
