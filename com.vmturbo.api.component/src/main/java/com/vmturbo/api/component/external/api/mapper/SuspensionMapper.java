package com.vmturbo.api.component.external.api.mapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import javax.annotation.Nullable;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;

import com.vmturbo.api.dto.entity.TagApiDTO;
import com.vmturbo.api.dto.group.FilterApiDTO;
import com.vmturbo.api.dto.suspension.BulkActionRequestApiDTO;
import com.vmturbo.api.dto.suspension.BulkActionRequestInputDTO;
import com.vmturbo.api.dto.suspension.ScheduleEntityResponseApiDTO;
import com.vmturbo.api.dto.suspension.ScheduleItemApiDTO;
import com.vmturbo.api.dto.suspension.ScheduleTimeSpansApiDTO;
import com.vmturbo.api.dto.suspension.SuspendItemApiDTO;
import com.vmturbo.api.dto.suspension.SuspendableEntityApiDTO;
import com.vmturbo.api.dto.suspension.SuspendableEntityInputDTO;
import com.vmturbo.api.dto.suspension.SuspendableEntityScheduleApiDTO;
import com.vmturbo.api.dto.suspension.SuspendableEntityUUIDSetDTO;
import com.vmturbo.api.dto.suspension.TimeSpanApiDTO;
import com.vmturbo.api.dto.suspension.WeekDayTimeSpansApiDTO;
import com.vmturbo.api.enums.CloudType;
import com.vmturbo.api.enums.LogicalOperator;
import com.vmturbo.api.enums.SuspensionActionType;
import com.vmturbo.api.enums.SuspensionEntityType;
import com.vmturbo.api.enums.SuspensionState;
import com.vmturbo.api.enums.SuspensionTimeSpanState;
import com.vmturbo.api.pagination.ScheduleTimeSpansOrderBy;
import com.vmturbo.api.pagination.ScheduleTimeSpansPaginationRequest;
import com.vmturbo.api.pagination.SuspensionEntitiesOrderBy;
import com.vmturbo.api.pagination.SuspensionEntitiesPaginationRequest;
import com.vmturbo.auth.api.authorization.UserContextUtils;
import com.vmturbo.common.protobuf.suspension.SuspensionEntityOuterClass;
import com.vmturbo.common.protobuf.suspension.SuspensionEntityOuterClass.CurrencyAmount;
import com.vmturbo.common.protobuf.suspension.SuspensionEntityOuterClass.SuspensionEntity;
import com.vmturbo.common.protobuf.suspension.SuspensionEntityOuterClass.SuspensionEntityRequest;
import com.vmturbo.common.protobuf.suspension.SuspensionEntityOuterClass.SuspensionEntityResolvedScope;
import com.vmturbo.common.protobuf.suspension.SuspensionEntityOuterClass.SuspensionEntityResolvedScopeRequest;
import com.vmturbo.common.protobuf.suspension.SuspensionEntityOuterClass.SuspensionEntitySchedule;
import com.vmturbo.common.protobuf.suspension.SuspensionEntityOuterClass.SuspensionEntityTags;
import com.vmturbo.common.protobuf.suspension.SuspensionFilter;
import com.vmturbo.common.protobuf.suspension.SuspensionScheduleEntity.SuspensionAttachEntitiesRequest;
import com.vmturbo.common.protobuf.suspension.SuspensionScheduleEntity.SuspensionAttachEntitiesResponse;
import com.vmturbo.common.protobuf.suspension.SuspensionScheduleEntity.SuspensionDetachEntitiesRequest;
import com.vmturbo.common.protobuf.suspension.SuspensionScheduleEntity.SuspensionDetachEntitiesResponse;
import com.vmturbo.common.protobuf.suspension.SuspensionScheduleEntity.SuspensionGetEntitiesRequest;
import com.vmturbo.common.protobuf.suspension.SuspensionScheduleEntity.SuspensionGetEntitiesResponse;
import com.vmturbo.common.protobuf.suspension.SuspensionScheduleEntity.SuspensionScheduleEntityError;
import com.vmturbo.common.protobuf.suspension.SuspensionScheduleEntity.SuspensionUpdateEntitiesRequest;
import com.vmturbo.common.protobuf.suspension.SuspensionScheduleEntity.SuspensionUpdateEntitiesResponse;
import com.vmturbo.common.protobuf.suspension.SuspensionTimeSpanSchedule.CreateTimespanScheduleRequest;
import com.vmturbo.common.protobuf.suspension.SuspensionTimeSpanSchedule.DeleteTimespanScheduleRequest;
import com.vmturbo.common.protobuf.suspension.SuspensionTimeSpanSchedule.ListTimespanScheduleRequest;
import com.vmturbo.common.protobuf.suspension.SuspensionTimeSpanSchedule.TimeOfDay;
import com.vmturbo.common.protobuf.suspension.SuspensionTimeSpanSchedule.Timespan;
import com.vmturbo.common.protobuf.suspension.SuspensionTimeSpanSchedule.TimespanSchedule;
import com.vmturbo.common.protobuf.suspension.SuspensionTimeSpanSchedule.TimespanScheduleByIDRequest;
import com.vmturbo.common.protobuf.suspension.SuspensionTimeSpanSchedule.TimespanScheduleOrderBy;
import com.vmturbo.common.protobuf.suspension.SuspensionTimeSpanSchedule.TimespanState;
import com.vmturbo.common.protobuf.suspension.SuspensionTimeSpanSchedule.UpdateTimespanScheduleRequest;
import com.vmturbo.common.protobuf.suspension.SuspensionTimeSpanSchedule.WeekDayTimespans;
import com.vmturbo.common.protobuf.suspension.SuspensionToggle.SuspensionToggleEntityData;
import com.vmturbo.common.protobuf.suspension.SuspensionToggle.SuspensionToggleEntityRequest;

/**
 * SuspensionMapper.
 * Used to convert data from grpc to api type or api to grpc type.
 */
public class SuspensionMapper {
    private final int validPaginationMaxLimit = 500;

    private final int validPaginationDefaultLimit = 300;

    private final int apiPaginationDefaultLimit;

    private final int apiPaginationMaxLimit;

    /**
     * BiMap with GRPC suspension entity state to API suspension entity state.
     */
    public static final BiMap<SuspensionEntityOuterClass.SuspensionEntityState, SuspensionState> GRPC_TO_API_SUSPENSION_STATE = ImmutableBiMap.of(
                    SuspensionEntityOuterClass.SuspensionEntityState.SUSPENSION_ENTITY_STATE_STOPPED, SuspensionState.STOPPED,
                    SuspensionEntityOuterClass.SuspensionEntityState.SUSPENSION_ENTITY_STATE_RUNNING, SuspensionState.RUNNING,
                    SuspensionEntityOuterClass.SuspensionEntityState.SUSPENSION_ENTITY_STATE_STARTING, SuspensionState.STARTING,
                    SuspensionEntityOuterClass.SuspensionEntityState.SUSPENSION_ENTITY_STATE_STOPPING, SuspensionState.STOPPING,
                    SuspensionEntityOuterClass.SuspensionEntityState.SUSPENSION_ENTITY_STATE_TERMINATING, SuspensionState.TERMINATING,
                    SuspensionEntityOuterClass.SuspensionEntityState.SUSPENSION_ENTITY_STATE_TERMINATED, SuspensionState.TERMINATED,
                    SuspensionEntityOuterClass.SuspensionEntityState.SUSPENSION_ENTITY_STATE_MISSING, SuspensionState.MISSING);

    /**
     * Constructor for the SuspensionMapper.
     */
    public SuspensionMapper(@Nullable final int apiPaginationMaxLimit,
                            @Nullable final int apiPaginationDefaultLimit) {
        this.apiPaginationMaxLimit = apiPaginationMaxLimit < 1 ? validPaginationMaxLimit : apiPaginationMaxLimit;
        this.apiPaginationDefaultLimit = apiPaginationDefaultLimit < 1 ? validPaginationDefaultLimit : apiPaginationDefaultLimit;
    }

    /**
     * Constructor with default values of SuspensionMapper.
     */
    public SuspensionMapper() {
        this.apiPaginationMaxLimit = validPaginationMaxLimit;
        this.apiPaginationDefaultLimit = validPaginationDefaultLimit;
    }

    /**
     * Gets api suspension entity state based on grpc suspension entity state.
     * @param state the grpc suspension entity state.
     * @return the api state.
     */
    public static SuspensionState getApiEntityState(SuspensionEntityOuterClass.SuspensionEntityState state) {
        return GRPC_TO_API_SUSPENSION_STATE.get(state);
    }

    /**
     * Gets grpc suspension entity state based on api suspension entity state.
     * @param state the api suspension entity state.
     * @return the grpc state.
     */
    public static SuspensionEntityOuterClass.SuspensionEntityState getGrpcEntityState(SuspensionState state) {
        return GRPC_TO_API_SUSPENSION_STATE.inverse().get(state);
    }

    /**
     * BiMap with GRPC suspension entity types to API suspension entity type.
     */
    public static final BiMap<SuspensionEntityOuterClass.SuspensionEntityType, SuspensionEntityType> GRPC_TO_API_SUSPENSION_TYPE = ImmutableBiMap.of(
                    SuspensionEntityOuterClass.SuspensionEntityType.SUSPENSION_ENTITY_TYPE_COMPUTE, SuspensionEntityType.VirtualMachine);

    /**
     * Gets api suspension entity type based on grpc suspension entity type.
     * @param type the grpc suspension entity type.
     * @return the api type.
     */
    public static SuspensionEntityType getApiEntityType(SuspensionEntityOuterClass.SuspensionEntityType type) {
        return GRPC_TO_API_SUSPENSION_TYPE.get(type);
    }

    /**
     * Gets grpc suspension entity type based on api suspension entity type.
     * @param type the api suspension entity type.
     * @return the grpc type.
     */
    public static SuspensionEntityOuterClass.SuspensionEntityType getGrpcEntityType(SuspensionEntityType type) {
        return GRPC_TO_API_SUSPENSION_TYPE.inverse().get(type);
    }

    /**
     * BiMap with GRPC suspension entity provider to API suspension entity provider.
     */
    public static final BiMap<SuspensionEntityOuterClass.SuspensionEntityProvider, CloudType> GRPC_TO_API_PROVIDER = ImmutableBiMap.of(
                    SuspensionEntityOuterClass.SuspensionEntityProvider.SUSPENSION_ENTITY_PROVIDER_AWS, CloudType.AWS,
                    SuspensionEntityOuterClass.SuspensionEntityProvider.SUSPENSION_ENTITY_PROVIDER_AZURE, CloudType.AZURE,
                    SuspensionEntityOuterClass.SuspensionEntityProvider.SUSPENSION_ENTITY_PROVIDER_GCP, CloudType.GCP);

    /**
     * Gets api suspension entity provider based on grpc suspension entity provider.
     * @param provider the grpc entity provider.
     * @return the api provider.
     */
    public static CloudType getProvider(SuspensionEntityOuterClass.SuspensionEntityProvider provider) {
        return GRPC_TO_API_PROVIDER.get(provider);
    }

    /**
     * Gets grpc suspension entity provider based on api suspension entity provider.
     * @param type the api entity provider.
     * @return the grpc provider.
     */
    public static SuspensionEntityOuterClass.SuspensionEntityProvider getGrpcProvider(CloudType provider) {
        return GRPC_TO_API_PROVIDER.inverse().get(provider);
    }

    /**
     * BiMap with API SuspensionEntitiesOrderBy to GRPC SuspensionEntityOrderBy.
     */
    public static final BiMap<SuspensionEntitiesOrderBy, SuspensionEntityOuterClass.SuspensionEntityOrderBy> API_TO_GRPC_SUSPENSION_ORDER_BY = ImmutableBiMap.of(
                    SuspensionEntitiesOrderBy.DISPLAY_NAME, SuspensionEntityOuterClass.SuspensionEntityOrderBy.SUSPENSION_ENTITY_ORDER_BY_DISPLAY_NAME,
                    SuspensionEntitiesOrderBy.ENTITY_TYPE, SuspensionEntityOuterClass.SuspensionEntityOrderBy.SUSPENSION_ENTITY_ORDER_BY_TYPE,
                    SuspensionEntitiesOrderBy.STATE, SuspensionEntityOuterClass.SuspensionEntityOrderBy.SUSPENSION_ENTITY_ORDER_BY_STATE,
                    SuspensionEntitiesOrderBy.ACCOUNT_NAME, SuspensionEntityOuterClass.SuspensionEntityOrderBy.SUSPENSION_ENTITY_ORDER_BY_ACCOUNT_NAME,
                    SuspensionEntitiesOrderBy.PROVIDER, SuspensionEntityOuterClass.SuspensionEntityOrderBy.SUSPENSION_ENTITY_ORDER_BY_PROVIDER,
                    SuspensionEntitiesOrderBy.REGION_NAME, SuspensionEntityOuterClass.SuspensionEntityOrderBy.SUSPENSION_ENTITY_ORDER_BY_REGION_NAME,
                    SuspensionEntitiesOrderBy.INSTANCE_TYPE, SuspensionEntityOuterClass.SuspensionEntityOrderBy.SUSPENSION_ENTITY_ORDER_BY_INSTANCE_TYPE,
                    SuspensionEntitiesOrderBy.COST, SuspensionEntityOuterClass.SuspensionEntityOrderBy.SUSPENSION_ENTITY_ORDER_BY_COST,
                    SuspensionEntitiesOrderBy.SCHEDULE_NAME, SuspensionEntityOuterClass.SuspensionEntityOrderBy.SUSPENSION_ENTITY_ORDER_BY_SCHEDULE_DISPLAY_NAME);

    /**
     * Gets api entity type based on grpc entity type.
     * @param type the grpc entity type.
     * @return the api type.
     */
    public static SuspensionEntityOuterClass.SuspensionEntityOrderBy getOrderBy(SuspensionEntitiesOrderBy orderBy) {
        return API_TO_GRPC_SUSPENSION_ORDER_BY.getOrDefault(orderBy, SuspensionEntityOuterClass.SuspensionEntityOrderBy.SUSPENSION_ENTITY_ORDER_BY_DISPLAY_NAME);
    }

    /**
     * BiMap with filters applicable for suspension entity as string filters.
     */
    public static final BiMap<String, SuspensionFilter.ExpType> STRING_OPERATORS = ImmutableBiMap.of(
                    "EQ", SuspensionFilter.ExpType.EXP_TYPE_EQ,
                    "NEQ", SuspensionFilter.ExpType.EXP_TYPE_NEQ,
                    "RXEQ", SuspensionFilter.ExpType.EXP_TYPE_RX_EQ,
                    "RXNEQ", SuspensionFilter.ExpType.EXP_TYPE_RX_NEQ);

    /**
     * BiMap with filters applicable for suspension entity as int filters.
     */
    public static final BiMap<String, SuspensionFilter.ExpType> NUMBER_OPERATORS = ImmutableBiMap.of(
                    "EQ", SuspensionFilter.ExpType.EXP_TYPE_EQ,
                    "NEQ", SuspensionFilter.ExpType.EXP_TYPE_NEQ,
                    "GT", SuspensionFilter.ExpType.EXP_TYPE_GT,
                    "LT", SuspensionFilter.ExpType.EXP_TYPE_LT,
                    "GTE", SuspensionFilter.ExpType.EXP_TYPE_GTE,
                    "LTE", SuspensionFilter.ExpType.EXP_TYPE_LTE);

    /**
     * The map from GRPC suspension logical operator to API suspension logical operator string.
     */
    public static final BiMap<LogicalOperator, SuspensionFilter.LogicalOperator> GRPC_TO_API_LOGICAL_OPERATOR = ImmutableBiMap.of(
            LogicalOperator.AND, SuspensionFilter.LogicalOperator.AND,
            LogicalOperator.OR, SuspensionFilter.LogicalOperator.OR);

    /**
     * Gets the grpc suspension logical operator based on api suspension logical operator.
     * @param op the api suspension logical operator.
     * @return the grpc type.
     */
    public static SuspensionFilter.LogicalOperator getGrpcLogicalOperator(LogicalOperator op) {
        return GRPC_TO_API_LOGICAL_OPERATOR.getOrDefault(op, SuspensionFilter.LogicalOperator.AND);
    }

    /**
     * Creates SuspensionEntityRequest based on the filters provided in the SuspendableEntityInputDTO.
     * @param SuspendableEntityInputDTO the query param for get suspension entities api.
     * @return SuspensionEntityRequest.
     */
    public SuspensionEntityRequest createSuspensionEntityRequestFromCriteriaList(SuspendableEntityInputDTO entityInputDTO) {
        HashMap<String, String> fieldMap = new HashMap<>();
        fieldMap.put("displayName", "string");
        fieldMap.put("accountName", "string");
        fieldMap.put("regionName", "string");
        fieldMap.put("instanceType", "string");
        fieldMap.put("scheduleName", "string");
        fieldMap.put("tags", "string");
        fieldMap.put("cost", "double");

        HashMap<String, List<SuspensionFilter.StringFilter>> protoStringMap = new HashMap<>();
        HashMap<String, List<SuspensionFilter.DoubleFilter>> protoDoubleMap = new HashMap<>();
        for (FilterApiDTO filterApiDTO : entityInputDTO.getCriteriaList()) {
            if (fieldMap.get(filterApiDTO.getFilterType()) == "string") {
                if (!STRING_OPERATORS.containsKey(filterApiDTO.getExpType())) {
                    throw new IllegalArgumentException("Invalid expression type for string filter");
                }
                if (protoStringMap.get(filterApiDTO.getFilterType()) == null) {
                    protoStringMap.put(filterApiDTO.getFilterType(), new ArrayList<>());
                }
                protoStringMap.get(filterApiDTO.getFilterType()).add(SuspensionFilter.StringFilter.newBuilder()
                        .setExpType(STRING_OPERATORS.get(filterApiDTO.getExpType()))
                        .addValues(filterApiDTO.getExpVal()).build());
            } else if (fieldMap.get(filterApiDTO.getFilterType()) == "double") {
                if (!NUMBER_OPERATORS.containsKey(filterApiDTO.getExpType())) {
                    throw new IllegalArgumentException("Invalid expression type for double filter");
                }
                if (protoDoubleMap.get(filterApiDTO.getFilterType()) == null) {
                    protoDoubleMap.put(filterApiDTO.getFilterType(), new ArrayList<>());
                }
                protoDoubleMap.get(filterApiDTO.getFilterType()).add(SuspensionFilter.DoubleFilter.newBuilder()
                        .setExpType(NUMBER_OPERATORS.get(filterApiDTO.getExpType()))
                        .addValues(Double.parseDouble(filterApiDTO.getExpVal())).build());
            } else {
                throw new IllegalArgumentException("Invalid filter type " + filterApiDTO.getFilterType());
            }
        }
        SuspensionEntityRequest.Builder requestBuilder = SuspensionEntityRequest.getDefaultInstance().newBuilder();
        if (protoStringMap.get("displayName") != null) {
            requestBuilder.addAllDisplayName(protoStringMap.get("displayName"));
        }
        if (protoStringMap.get("accountName") != null) {
            requestBuilder.addAllAccountNames(protoStringMap.get("accountName"));
        }
        if (protoStringMap.get("regionName") != null) {
            requestBuilder.addAllRegionNames(protoStringMap.get("regionName"));
        }
        if (protoStringMap.get("instanceType") != null) {
            requestBuilder.addAllInstanceTypes(protoStringMap.get("instanceType"));
        }
        if (protoStringMap.get("scheduleName") != null) {
            requestBuilder.addAllScheduleNames(protoStringMap.get("scheduleName"));
        }
        if (protoStringMap.get("tags") != null) {
            requestBuilder.addAllTags(protoStringMap.get("tags"));
        }
        if (protoDoubleMap.get("cost") != null) {
            requestBuilder.addAllCost(protoDoubleMap.get("cost"));
        }
        return requestBuilder.build();
    }

    /**
     * Creates SuspensionEntityRequest based on the filters provided in the SuspendableEntityInputDTO and also pagination query params.
     * @param SuspendableEntityInputDTO to provide all the filters to be applied on the suspension entities.
     * @param SuspensionEntitiesPaginationRequest to provide all the pagination query params to apply on suspension entities to be fetched.
     * @return SuspensionEntityRequest.
     */
    public SuspensionEntityRequest createSuspensionEntityRequest(SuspendableEntityInputDTO entityInputDTO,
                                             SuspensionEntitiesPaginationRequest paginationRequest) {

        SuspensionEntityRequest.Builder requestBuilder = SuspensionEntityRequest.getDefaultInstance().newBuilder();
        if (entityInputDTO.hasCriteriaList()) {
            requestBuilder.mergeFrom(createSuspensionEntityRequestFromCriteriaList(entityInputDTO));
        }
        if (entityInputDTO.hasEntityTypes()) {
            for (SuspensionEntityType suspensionEntityType : entityInputDTO.getEntityTypes()) {
                requestBuilder.addType(getGrpcEntityType(suspensionEntityType));
            }
        }
        if (entityInputDTO.hasStatus()) {
            for (SuspensionState suspensionState : entityInputDTO.getStatus()) {
                requestBuilder.addStatus(getGrpcEntityState(suspensionState));
            }
        }
        if (entityInputDTO.hasProviders()) {
            for (CloudType provider : entityInputDTO.getProviders()) {
                requestBuilder.addProviders(getGrpcProvider(provider));
            }
        }
        requestBuilder.setLogicalOperator(getGrpcLogicalOperator(entityInputDTO.getLogicalOperator()));
        if (paginationRequest != null) {
            if (paginationRequest.getCursor() != null) {
                if (paginationRequest.getCursor().isPresent()) {
                    requestBuilder.setCursor(paginationRequest.getCursor().get());
                }
            }
            if (paginationRequest.getOrderBy() != null) {
                requestBuilder.setOrderBy(getOrderBy(paginationRequest.getOrderBy()));
            }
            requestBuilder.setDescending(!paginationRequest.isAscending());
            if (paginationRequest.hasLimit()) {
                requestBuilder.setLimit(paginationRequest.getLimit());
            } else {
                requestBuilder.setLimit(apiPaginationDefaultLimit);
            }
        }
        return requestBuilder.build();
    }

    /**
     * Append SuspensionEntityRequest based on the filters provided in the SuspendableEntityInputDTO and also pagination query params with group scopes.
     * @param SuspendableEntityInputDTO to provide all the filters to be applied on the suspension entities.
     * @param SuspensionEntitiesPaginationRequest to provide all the pagination query params to apply on suspension entities to be fetched.
     * @parms scopes OIDs to be appended to the SuspensionEntityRequest.
     * @return SuspensionEntityRequest.
     */
    public SuspensionEntityRequest createSuspensionEntityRequestWithScope(SuspendableEntityInputDTO entityInputDTO,
                                             SuspensionEntitiesPaginationRequest paginationRequest, List<Long> scopeOids) {
        SuspensionEntityRequest request = SuspensionEntityRequest.getDefaultInstance();
        SuspensionEntityRequest.Builder requestBuilder = request.newBuilder(createSuspensionEntityRequest(entityInputDTO, paginationRequest));
        if (scopeOids.size() > 0) {
            requestBuilder.addAllScopes(scopeOids);
        }
        return requestBuilder.build();
    }

    /**
     * Append SuspensionEntityRequest based on the filters provided in the SuspendableEntityInputDTO and also pagination query params with entity scopes.
     * @param SuspendableEntityInputDTO to provide all the filters to be applied on the suspension entities.
     * @param SuspensionEntitiesPaginationRequest to provide all the pagination query params to apply on suspension entities to be fetched.
     * @parms entity uuids to be appended to the SuspensionEntityRequest.
     * @return SuspensionEntityRequest.
     */
    public SuspensionEntityRequest createSuspensionEntityRequestWithResolvedScopes(SuspendableEntityInputDTO entityInputDTO,
                                             SuspensionEntitiesPaginationRequest paginationRequest, List<Long> scopeEntityOids) {
        SuspensionEntityRequest.Builder requestBuilder = SuspensionEntityRequest.getDefaultInstance().newBuilder(createSuspensionEntityRequest(entityInputDTO, paginationRequest));
        if (scopeEntityOids.size() > 0) {
            SuspensionEntityResolvedScope.Builder scopeBuilder = SuspensionEntityResolvedScope.getDefaultInstance().newBuilder();
            for (Long scopeOids : scopeEntityOids) {
                SuspensionEntityResolvedScopeRequest.Builder scopeRequestBuilder = SuspensionEntityResolvedScopeRequest.getDefaultInstance().newBuilder();
                scopeRequestBuilder.setOid(scopeOids);
                scopeRequestBuilder.setType(SuspensionEntityOuterClass.SuspensionEntityType.SUSPENSION_ENTITY_TYPE_COMPUTE);
                scopeBuilder.addEntities(scopeRequestBuilder.build());
            }
            scopeBuilder.setOid(Long.valueOf(1));
            requestBuilder.addResolvedScopes(scopeBuilder.build());
        }

        return requestBuilder.build();
    }

    /**
     * Converts list of SuspensionEntity grpc class into SuspendableEntityApiDTO class.
     * @param List of SuspensionEntity class.
     * @return List of SuspendableEntityApiDTO class.
     */
    public List<SuspendableEntityApiDTO> convertToSuspendableEntityApiDTO(List<SuspensionEntity> entities) {
        List<SuspendableEntityApiDTO> entityList = new ArrayList<SuspendableEntityApiDTO>();
        for (SuspensionEntity entity : entities) {
            SuspendableEntityApiDTO objt = new SuspendableEntityApiDTO();
            if (entity.hasState()) {
                objt.setState(getApiEntityState(entity.getState()));
            }
            if (entity.hasEntityType()) {
                objt.setEntityType(getApiEntityType(entity.getEntityType()));
            }
            if (entity.hasProvider()) {
                objt.setProvider(getProvider(entity.getProvider()));
            }
            if (entity.hasOid()) {
                objt.setUuid(Long.toString(entity.getOid()));
            }
            if (entity.hasName()) {
                objt.setDisplayName(entity.getName());
            }
            if (entity.hasInstanceType()) {
                objt.setInstanceType(entity.getInstanceType());
            }
            if (entity.hasAccountOid()) {
                objt.setAccountOID(entity.getAccountOid());
            }
            if (entity.hasAccountName()) {
                objt.setAccountName(entity.getAccountName());
            }
            if (entity.hasSuspendable()) {
                objt.setSuspendable(entity.getSuspendable());
            }
            if (entity.hasRegionOid()) {
                objt.setRegionOID(entity.getRegionOid());
            }
            if (entity.hasRegionName()) {
                objt.setRegionName(entity.getRegionName());
            }
            if (entity.getTagsCount() > 0) {
                objt.setTags(convertToTagApiDTO(entity.getTagsList()));
            }
            if (entity.getSchedulesCount() > 0) {
                objt.setSchedules(convertToSuspendableEntityScheduleApiDTO(entity.getSchedulesList()));
            }
            if (entity.hasCost()) {
                CurrencyAmount costObj = entity.getCost();
                if (costObj.hasAmount()) {
                    objt.setCost(costObj.getAmount());
                }
            }

            entityList.add(objt);
        }
        return entityList;
    }

    /**
     * Converts list of Tags from SuspensionEntityTags grpc class into TagApiDTO class.
     * @param List of Tags from SuspensionEntityTags grpc.
     * @return List of TagApiDTO class.
     */
    public List<TagApiDTO> convertToTagApiDTO(List<SuspensionEntityTags> tags) {
        List<TagApiDTO> tagsList = new ArrayList<TagApiDTO>();
        for (SuspensionEntityTags tag: tags) {
            TagApiDTO objt = new TagApiDTO();
            if (tag.hasKey()) {
                objt.setKey(tag.getKey());
            }
            if (tag.getValuesCount() > 0 ) {
                objt.setValues(tag.getValuesList());
            }
            tagsList.add(objt);
        }
        return tagsList;
    }

    /**
     * Converts list of Schedules from SuspensionEntitySchedule grpc class into SuspendableEntityScheduleApiDTO class.
     * @param List of Schedules from SuspensionEntitySchedule grpc.
     * @return List of SuspendableEntityScheduleApiDTO class.
     */
    public List<SuspendableEntityScheduleApiDTO> convertToSuspendableEntityScheduleApiDTO(List<SuspensionEntitySchedule> schedules) {
        List<SuspendableEntityScheduleApiDTO> schedulesList = new ArrayList<SuspendableEntityScheduleApiDTO>();
        for (SuspensionEntitySchedule schedule: schedules) {
            SuspendableEntityScheduleApiDTO objt = new SuspendableEntityScheduleApiDTO();
            if (schedule.hasName()) {
                objt.setDisplayName(schedule.getName());
            }
            if (schedule.hasOid()) {
                objt.setScheduleUUID(Long.toString(schedule.getOid()));
            }
            schedulesList.add(objt);
        }
        return schedulesList;
    }

    /**
     * Map with API suspension action to GRPC Toggle Action type.
     */
    public static final BiMap<SuspensionActionType, SuspensionToggleEntityRequest.Action> API_ACTION_TO_GRPC_GRPC  = ImmutableBiMap.of(
                    SuspensionActionType.START, SuspensionToggleEntityRequest.Action.ACTION_START,
                    SuspensionActionType.STOP, SuspensionToggleEntityRequest.Action.ACTION_STOP);

    /**
     * Gets grpc suspension entity action,  based on api suspension entity action.
     * @param action the api suspension action.
     * @return the grpc toggle action.
     */
    public static SuspensionToggleEntityRequest.Action getGrpcToggleActionType(SuspensionActionType action) {
        return API_ACTION_TO_GRPC_GRPC.get(action);
    }

    /**
     * This exception is thrown when the request is invalid.
     */
    public static class InvalidRequest extends Exception {
        InvalidRequest(String message) {
            super(message);
        }
    }

    /**
     * Converts BulkActionRequestInputDTO of class into SuspensionToggleEntityRequest class.
     *
     * @param BulkActionRequestInputDTO class.
     * @return SuspensionToggleEntityRequest class.
     */
    public SuspensionToggleEntityRequest createSuspensionToggleEntityRequest(BulkActionRequestInputDTO bulkActionInputDTO)
            throws InvalidRequest {

        if (!bulkActionInputDTO.hasEntityUuids()) {
            throw new InvalidRequest("entity uuids cannot be empty");
        }

        List<Long> entityOIDs = new ArrayList<>();
        for (String s : bulkActionInputDTO.getEntityUuids()) {
            Long entityOID = Long.parseLong(s);
            if (entityOID == 0) {
                continue;
            }
            entityOIDs.add(entityOID);
        }

        if (entityOIDs.size() == 0) {
            throw new InvalidRequest("invalid entity uuids");
        }

        SuspensionToggleEntityRequest.Builder requestBuilder = SuspensionToggleEntityRequest.getDefaultInstance().newBuilder();

        requestBuilder.addAllEntityOids(entityOIDs);

        requestBuilder.setAction(getGrpcToggleActionType(bulkActionInputDTO.getAction()));

        Optional<String> userID = UserContextUtils.getCurrentUserId();
        if (userID.isPresent()) {
            requestBuilder.setUserId(userID.get());
        }

        String userName = UserContextUtils.getCurrentUserName();
        requestBuilder.setUserName(userName);

        return requestBuilder.build();
    }

    /**
     * Converts list of SuspensionToggleEntityData class into BulkActionRequestApiDTO class.
     * @param List of SuspensionToggleEntityData class.
     * @return List of BulkActionRequestApiDTO class.
     */
    public List<BulkActionRequestApiDTO> convertToBulkActionRequestApiDTO(List<SuspensionToggleEntityData> entities) {
        List<BulkActionRequestApiDTO> list = new ArrayList<BulkActionRequestApiDTO>();
        for (SuspensionToggleEntityData entity : entities) {
            BulkActionRequestApiDTO data = new BulkActionRequestApiDTO();
            if (entity.hasEntityOid()) {
                data.setEntityUUID(Long.toString(entity.getEntityOid()));
            }
            if (entity.hasError()) {
                data.setError(entity.getError());
            }
            if (entity.hasState()) {
                data.setState(getApiEntityState(entity.getState()));
            }
            if (!entity.hasState() && !entity.hasError()) {
                data.setError("Unknown entity state");
            }

            list.add(data);
        }
        return list;
    }


    /**
     * The map from API suspension time span state to GRPC time span state.
     */
    public static final BiMap<SuspensionTimeSpanState, TimespanState> API_TS_STATE_TO_GRPC  = ImmutableBiMap.of(
            SuspensionTimeSpanState.ON, TimespanState.TS_ON,
            SuspensionTimeSpanState.OFF, TimespanState.TS_OFF,
            SuspensionTimeSpanState.IGNORE, TimespanState.TS_IGNORE);

    /**
     * Gets the grpc time span state for the schedule.
     * @param state the api time span suspension state.
     * @return the grpc time span state.
     */
    public static TimespanState getGrpcTsState(SuspensionTimeSpanState state) {
        return API_TS_STATE_TO_GRPC.get(state);
    }

    /**
     * Gets the grpc time span state for the schedule.
     * @param state the grpc time span suspension state.
     * @return the api time span state.
     */
    public static SuspensionTimeSpanState getApiTsState(TimespanState state) {
        return API_TS_STATE_TO_GRPC.inverse().get(state);
    }

    /**
     * converts the ScheduleTimeSpansApiDTO of class into CreateTimespanScheduleRequest class.
     *
     * @param scheduleApiDTO instance of ScheduleTimeSpansApiDTO class.
     * @return CreateTimespanScheduleRequest class.
     * @throws InvalidRequest in case of bad request
     * @throws NumberFormatException if invalid number is encountered
     */
    public CreateTimespanScheduleRequest toCreateTimespanScheduleRequest(
            ScheduleTimeSpansApiDTO scheduleApiDTO)
            throws Exception {
        if (scheduleApiDTO.getDisplayName().isEmpty()) {
            throw new InvalidRequest("schedule name cannot be empty");
        }
        if (scheduleApiDTO.getTimeZone().isEmpty()) {
            throw new InvalidRequest("time zone cannot be empty");
        }
        CreateTimespanScheduleRequest.Builder requestBuilder = CreateTimespanScheduleRequest.getDefaultInstance().newBuilder();
        requestBuilder.setName(scheduleApiDTO.getDisplayName());
        requestBuilder.setTimezone(scheduleApiDTO.getTimeZone());
        requestBuilder.setDescription(scheduleApiDTO.getDescription());
        String timespanType = scheduleApiDTO.getTimeSpans().getType();
        WeekDayTimeSpansApiDTO weekDayTimeSpansApiDTO = new WeekDayTimeSpansApiDTO();
        if (timespanType.equals(weekDayTimeSpansApiDTO.getType())) {
            WeekDayTimeSpansApiDTO weekDayTimespans = scheduleApiDTO.getTimeSpans() instanceof WeekDayTimeSpansApiDTO
                    ? ((WeekDayTimeSpansApiDTO)scheduleApiDTO.getTimeSpans()) : null;
            requestBuilder.setTimespans(toGrpcWeekDayTimespans(weekDayTimespans));
            return requestBuilder.build();
        }
        throw new InvalidRequest(String.format("expected time span group type: %s, got: %s", weekDayTimeSpansApiDTO.getType(), timespanType));
    }


    /**
     * converts the timeSpanSchedule_Uuid of class into TimespanScheduleByIDRequest class.
     *
     * @param timeSpanSchedule_Uuid This is representing the unique time span based schedule.
     * @return ScheduleTimeSpansApiDTO the get schedule object.
     * @throws Exception when any error occurs
     */
    public TimespanScheduleByIDRequest toGetTimespanScheduleRequest(String timeSpanSchedule_Uuid)
            throws Exception {
        if (timeSpanSchedule_Uuid.isEmpty()) {
            throw new InvalidRequest("timeSpanSchedule_Uuid cannot be empty");
        }

        TimespanScheduleByIDRequest.Builder requestBuilder = TimespanScheduleByIDRequest.getDefaultInstance().newBuilder();

        requestBuilder.setScheduleOid(Long.parseLong(timeSpanSchedule_Uuid));

        return requestBuilder.build();
    }

    /**
     * converts the ScheduleTimeSpansApiDTO of class into UpdateTimespanScheduleRequest class.
     *
     * @param scheduleApiDTO instance of ScheduleTimeSpansApiDTO class.
     * @return UpdateTimespanScheduleRequest class.
     * @throws InvalidRequest in case of bad request
     * @throws NumberFormatException if invalid number is encountered
     */
    public UpdateTimespanScheduleRequest toUpdateTimespanScheduleRequest(String uuid,
            ScheduleTimeSpansApiDTO scheduleApiDTO)
            throws Exception {
        if (uuid.isEmpty()) {
            throw new InvalidRequest("uuid cannot be empty");
        }
        if (scheduleApiDTO.getDisplayName().isEmpty()) {
            throw new InvalidRequest("schedule name cannot be empty");
        }
        if (scheduleApiDTO.getTimeZone().isEmpty()) {
            throw new InvalidRequest("timezone cannot be empty");
        }
        UpdateTimespanScheduleRequest.Builder requestBuilder = UpdateTimespanScheduleRequest.getDefaultInstance().newBuilder();
        requestBuilder.setOid(Long.parseLong(uuid));
        requestBuilder.setName(scheduleApiDTO.getDisplayName());
        requestBuilder.setTimezone(scheduleApiDTO.getTimeZone());
        requestBuilder.setDescription(scheduleApiDTO.getDescription());
        String timespanType = scheduleApiDTO.getTimeSpans().getType();
        WeekDayTimeSpansApiDTO weekDayTimeSpansApiDTO = new WeekDayTimeSpansApiDTO();
        if (timespanType.equals(weekDayTimeSpansApiDTO.getType())) {
            WeekDayTimeSpansApiDTO weekDayTimespans = scheduleApiDTO.getTimeSpans() instanceof WeekDayTimeSpansApiDTO
                    ? ((WeekDayTimeSpansApiDTO)scheduleApiDTO.getTimeSpans()) : null;
            requestBuilder.setTimespans(toGrpcWeekDayTimespans(weekDayTimespans));
            return requestBuilder.build();
        }
        throw new InvalidRequest(String.format("expected timespan group type: %s, got: %s", weekDayTimeSpansApiDTO.getType(), timespanType));
    }

    /**
     * converts the String into DeleteTimespanScheduleRequest class.
     *
     * @param timeSpanSchedule_Uuid This is representing the unique time span based schedule.
     * @return DeleteTimespanScheduleRequest class.
     * @throws NumberFormatException if invalid number is encountered
     */
    public DeleteTimespanScheduleRequest toApiDeleteTimespanScheduleRequest(
            String timeSpanSchedule_Uuid)
            throws Exception {

        DeleteTimespanScheduleRequest.Builder requestBuilder = DeleteTimespanScheduleRequest.getDefaultInstance().newBuilder();

        requestBuilder.setScheduleOid(Long.parseLong(timeSpanSchedule_Uuid));

        return requestBuilder.build();
    }


    /**
     * converts the instance of TimespanSchedule class into ScheduleTimeSpansApiDTO class.
     *
     * @param grpcSchedule instance of TimespanSchedule class.
     * @return ScheduleTimeSpansApiDTO class.
     */
    public ScheduleTimeSpansApiDTO toApiScheduleTimeSpansApiDTO(
            TimespanSchedule grpcSchedule) {

        ScheduleTimeSpansApiDTO scheduleApiDTO = new ScheduleTimeSpansApiDTO();

        if (grpcSchedule.hasOid()) {
            scheduleApiDTO.setUuid(Long.toString(grpcSchedule.getOid()));
        }

        if (grpcSchedule.hasName()) {
            scheduleApiDTO.setDisplayName(grpcSchedule.getName());
        }

        if (grpcSchedule.hasDescription()) {
            scheduleApiDTO.setDescription(grpcSchedule.getDescription());
        }

        if (grpcSchedule.hasTimezone()) {
            scheduleApiDTO.setTimeZone(grpcSchedule.getTimezone());
        }

        if (grpcSchedule.hasTimespans()) {
            scheduleApiDTO.setTimeSpans(toApiWeekDayTimespans(grpcSchedule.getTimespans()));
        }

        return scheduleApiDTO;
    }


    /**
     * The map from API time span schedule order by fields to GRPC time span schedule order by.
     */
    public static final BiMap<ScheduleTimeSpansOrderBy, TimespanScheduleOrderBy> API_TO_GRPC_SCHEDULE_ORDER_BY = ImmutableBiMap.of(
            ScheduleTimeSpansOrderBy.DISPLAY_NAME, TimespanScheduleOrderBy.TIMESPAN_SCHEDULE_ORDER_BY_DISPLAY_NAME);

    /**
     * Gets the gRPC order by type from api schedule order by.
     * @param orderBy the api timespan schedule order by type.
     * @return the gRPC type.
     */
    public static TimespanScheduleOrderBy getScheduleOrderBy(ScheduleTimeSpansOrderBy orderBy) {
        return API_TO_GRPC_SCHEDULE_ORDER_BY.getOrDefault(orderBy, TimespanScheduleOrderBy.TIMESPAN_SCHEDULE_ORDER_BY_DISPLAY_NAME);
    }

    /**
     * creates ListTimespanScheduleRequest based on the pagination query params.
     * @param paginationRequest to provide all the pagination query params to apply on time span schedules to be fetched.
     * @return ListTimespanScheduleRequest.
     */
    public ListTimespanScheduleRequest toListTimespanScheduleRequest(
            ScheduleTimeSpansPaginationRequest paginationRequest) {

        ListTimespanScheduleRequest.Builder requestBuilder = ListTimespanScheduleRequest.getDefaultInstance().newBuilder();
        if (paginationRequest == null) {
            return requestBuilder.build();
        }
        if (paginationRequest.getCursor() != null && paginationRequest.getCursor().isPresent()) {
            requestBuilder.setCursor(paginationRequest.getCursor().get());
        }
        if (paginationRequest.getOrderBy() != null) {
            requestBuilder.setOrderBy(getScheduleOrderBy(paginationRequest.getOrderBy()));
        }
        requestBuilder.setDescending(!paginationRequest.isAscending());
        final int limit = paginationRequest.hasLimit() ? paginationRequest.getLimit() : apiPaginationDefaultLimit;
        requestBuilder.setLimit(limit);
        return requestBuilder.build();
    }

    /**
     * converts the String of format hh:mm into TimeOfDay class.
     *
     * @param time String.
     * @return TimeOfDay class.
     * @throws NumberFormatException if invalid number is encountered
     */
    private TimeOfDay toGrpcTimeOfDay(String time) throws Exception {
        String[] hm = time.split(":");
        if (2 > hm.length) {
            throw new InvalidRequest("time not in expected format (hh:mm)");
        }
        TimeOfDay.Builder tod = TimeOfDay.getDefaultInstance().newBuilder();
        tod.setHours(Integer.parseInt(hm[0]));
        tod.setMinutes(Integer.parseInt(hm[1]));
        return tod.build();
    }

    /**
     * converts the API WeekDayTimeSpansApiDTO into gRPC WeekDayTimespans class.
     *
     * @param timeSpans instance of WeekDayTimeSpansApiDTO class.
     * @return WeekDayTimespans class.
     * @throws NumberFormatException if invalid number is encountered in begins/ends.
     * @throws InvalidRequest in case non-supported ScheduleItem class type is encountered in policy.
     */
    private WeekDayTimespans toGrpcWeekDayTimespans(WeekDayTimeSpansApiDTO timeSpans) throws Exception {
        WeekDayTimespans.Builder builder = WeekDayTimespans.getDefaultInstance().newBuilder();
        builder.addAllSunday(toGrpcTimespans(timeSpans.getSunday()));
        builder.addAllMonday(toGrpcTimespans(timeSpans.getMonday()));
        builder.addAllTuesday(toGrpcTimespans(timeSpans.getTuesday()));
        builder.addAllWednesday(toGrpcTimespans(timeSpans.getWednesday()));
        builder.addAllThursday(toGrpcTimespans(timeSpans.getThursday()));
        builder.addAllFriday(toGrpcTimespans(timeSpans.getFriday()));
        builder.addAllSaturday(toGrpcTimespans(timeSpans.getSaturday()));
        return builder.build();
    }

    /**
     * converts the list of API TimeSpanApiDTO instance into gRPC Time span list class.
     *
     * @param timeSpans TimeSpanApiDTO list.
     * @return Timespan class list.
     * @throws NumberFormatException if invalid number is encountered in begins/ends.
     * @throws InvalidRequest in case non-supported ScheduleItem class type is encountered in policy.
     */
    private List<Timespan> toGrpcTimespans(List<TimeSpanApiDTO> timeSpans) throws Exception {
        List<Timespan> grpcTimespans = new ArrayList<>();
        if (null == timeSpans) {
            return grpcTimespans;
        }
        Iterator<TimeSpanApiDTO> iter = timeSpans.iterator();
        while (iter.hasNext()) {
            grpcTimespans.add(toGrpcTimespan(iter.next()));
        }
        return grpcTimespans;
    }

    /**
     * converts individual API TimeSpanApiDTO instance into gRPC Time span class.
     *
     * @param timeSpanApiDTO instance of TimeSpanApiDTO class.
     * @return Timespan class.
     * @throws NumberFormatException if invalid number is encountered in begins/ends.
     * @throws InvalidRequest in case non-supported ScheduleItem class type is encountered in policy.
     */
    private Timespan toGrpcTimespan(TimeSpanApiDTO timeSpanApiDTO) throws Exception {
        Timespan.Builder ts = Timespan.getDefaultInstance().newBuilder();
        ts.setBegins(toGrpcTimeOfDay(timeSpanApiDTO.getBegins()));
        ts.setEnds(toGrpcTimeOfDay(timeSpanApiDTO.getEnds()));
        ts.setState(apiPolicyToTimespanState(timeSpanApiDTO.getPolicy()));
        return ts.build();
    }

    /**
     * converts API policy request ScheduleItemApiDTO instance into gRPC TimespanState enum.
     *
     * @param scheduleItemApiDTO instance of ScheduleItemApiDTO class.
     * @return TimespanState enum.
     * @throws InvalidRequest in case non-supported ScheduleItem type is encountered
     */
    private TimespanState apiPolicyToTimespanState(ScheduleItemApiDTO scheduleItemApiDTO) throws Exception {
        String policyType = scheduleItemApiDTO.getType();
        SuspendItemApiDTO suspendItemApiDTO = new SuspendItemApiDTO();
        if (policyType.equals(suspendItemApiDTO.getType())) {
            SuspendItemApiDTO suspendPolicy = scheduleItemApiDTO instanceof SuspendItemApiDTO
                    ? ((SuspendItemApiDTO)scheduleItemApiDTO) : null;
            return getGrpcTsState(suspendPolicy.getState());
        }
        throw new InvalidRequest(String.format("expected policy type: %s, got: %s", suspendItemApiDTO.getType(), policyType));
    }

    /**
     * converts gRPC WeekDayTimespans instance into API WeekDayTimeSpansApiDTO class.
     *
     * @param timeSpans instance of WeekDayTimespans class.
     * @return WeekDayTimeSpansApiDTO class.
     */
    private WeekDayTimeSpansApiDTO toApiWeekDayTimespans(WeekDayTimespans timeSpans) {
        WeekDayTimeSpansApiDTO weekTimespan = new WeekDayTimeSpansApiDTO();
        weekTimespan.setSunday(toApiTimespanArray(timeSpans.getSundayList()));
        weekTimespan.setMonday(toApiTimespanArray(timeSpans.getMondayList()));
        weekTimespan.setTuesday(toApiTimespanArray(timeSpans.getTuesdayList()));
        weekTimespan.setWednesday(toApiTimespanArray(timeSpans.getWednesdayList()));
        weekTimespan.setThursday(toApiTimespanArray(timeSpans.getThursdayList()));
        weekTimespan.setFriday(toApiTimespanArray(timeSpans.getFridayList()));
        weekTimespan.setSaturday(toApiTimespanArray(timeSpans.getSaturdayList()));
        return weekTimespan;
    }

    /**
     * converts list of gRPC Time span instance into API TimeSpanApiDTO class list.
     *
     * @param timeSpans list of gRPC Timespan.
     * @return list of TimeSpanApiDTO.
     */
    private List<TimeSpanApiDTO> toApiTimespanArray(List<Timespan> timeSpans) {
        List<TimeSpanApiDTO> apiTimespans = new ArrayList<>();
        Iterator<Timespan> iter = timeSpans.iterator();
        while (iter.hasNext()) {
            apiTimespans.add(toApiTimespan(iter.next()));
        }
        return apiTimespans;
    }

    /**
     * converts individual gRPC Timespan instance into API TimeSpanApiDTO class.
     *
     * @param timespan instance of gRPC timespan.
     * @return TimeSpanApiDTO class.
     */
    private TimeSpanApiDTO toApiTimespan(Timespan timespan) {
        TimeSpanApiDTO ts = new TimeSpanApiDTO();
        ts.setBegins(toApiTimeOfDay(timespan.getBegins()));
        ts.setEnds(toApiTimeOfDay(timespan.getEnds()));

        SuspendItemApiDTO suspendPolicy = new SuspendItemApiDTO();
        suspendPolicy.setState(getApiTsState(timespan.getState()));
        ts.setPolicy(suspendPolicy);
        return ts;
    }

    /**
     * converts TimeOfDay class instance into hh:mm format string.
     *
     * @param tod instance of TimeOfDay.
     * @return String in format hh:mm
     */
    private String toApiTimeOfDay(TimeOfDay tod) {
        return String.format("%02d:%02d", tod.getHours(),
                tod.getMinutes());
    }

    /**
     * converts the SuspendableEntityUUIDSetDTO of class into SuspensionAttachEntitiesRequest class.
     *
     * @param entityUuids instance of SuspendableEntityUUIDSetDTO class.
     * @return SuspensionScheduleEntityRequest class.
     * @throws InvalidRequest in case of bad request
     */
    public SuspensionAttachEntitiesRequest toSuspensionAttachEntitiesRequest(String timeSpanSchedule_Uuid,
                                                                         SuspendableEntityUUIDSetDTO entityUuids)
            throws Exception {
        if (!entityUuids.hasEntityUuids()) {
            throw new InvalidRequest("entity uuids list cannot be empty");
        }

        SuspensionAttachEntitiesRequest.Builder requestBuilder = SuspensionAttachEntitiesRequest.getDefaultInstance().newBuilder();
        requestBuilder.setScheduleOid(Long.parseLong(timeSpanSchedule_Uuid));
        for (String entityUuid : entityUuids.getEntityUuids()) {
            requestBuilder.addEntityOids(Long.parseLong(entityUuid));
        }
        return requestBuilder.build();
    }


    /**
     * converts the list of SuspensionAttachEntitiesResponse class into ScheduleEntityResponseApiDTO class.
     * @param resp of SuspensionAttachEntitiesResponse class.
     * @return List of ScheduleEntityResponseApiDTO class.
     */
    public List<ScheduleEntityResponseApiDTO> toScheduleEntityResponseApiDTOForAttach(SuspensionAttachEntitiesResponse resp) {
        List<ScheduleEntityResponseApiDTO> list = new ArrayList<ScheduleEntityResponseApiDTO>();
        for (SuspensionScheduleEntityError entity : resp.getErrorList()) {
            ScheduleEntityResponseApiDTO data = new ScheduleEntityResponseApiDTO();
            if (entity.hasEntityOid()) {
                data.setEntityUUID(Long.toString(entity.getEntityOid()));
            }
            if (entity.hasError()) {
                data.setError(entity.getError());
            }
            list.add(data);
        }
        return list;
    }

    /**
     * converts the SuspendableEntityUUIDSetDTO of class into SuspensionUpdateEntitiesRequest class.
     *
     * @param entityUuids instance of SuspendableEntityUUIDSetDTO class.
     * @return SuspensionScheduleEntityRequest class.
     * @throws InvalidRequest in case of bad request
     */
    public SuspensionUpdateEntitiesRequest toSuspensionUpdateEntitiesRequest(String timeSpanSchedule_Uuid,
                                                                             SuspendableEntityUUIDSetDTO entityUuids)
            throws Exception {
        if (!entityUuids.hasEntityUuids()) {
            throw new InvalidRequest("entity uuids list cannot be empty");
        }

        SuspensionUpdateEntitiesRequest.Builder requestBuilder = SuspensionUpdateEntitiesRequest.getDefaultInstance().newBuilder();
        requestBuilder.setScheduleOid(Long.parseLong(timeSpanSchedule_Uuid));
        for (String entityUuid : entityUuids.getEntityUuids()) {
            requestBuilder.addEntityOids(Long.parseLong(entityUuid));
        }
        return requestBuilder.build();
    }

    /**
     * converts the list of SuspensionUpdateEntitiesResponse class into ScheduleEntityResponseApiDTO class.
     * @param resp of SuspensionUpdateEntitiesResponse class.
     * @return List of ScheduleEntityResponseApiDTO class.
     */
    public List<ScheduleEntityResponseApiDTO> toScheduleEntityResponseApiDTOForUpdate(SuspensionUpdateEntitiesResponse resp) {
        List<ScheduleEntityResponseApiDTO> list = new ArrayList<ScheduleEntityResponseApiDTO>();
        for (SuspensionScheduleEntityError entity : resp.getErrorList()) {
            ScheduleEntityResponseApiDTO data = new ScheduleEntityResponseApiDTO();
            if (entity.hasEntityOid()) {
                data.setEntityUUID(Long.toString(entity.getEntityOid()));
            }
            if (entity.hasError()) {
                data.setError(entity.getError());
            }
            list.add(data);
        }
        return list;
    }

    /**
     * converts the SuspendableEntityUUIDSetDTO of class into toSuspensionScheduleEntityRequest class.
     *
     * @param entityUuids instance of SuspendableEntityUUIDSetDTO class.
     * @return SuspensionScheduleEntityRequest class.
     * @throws InvalidRequest in case of bad request
     */
    public SuspensionDetachEntitiesRequest toSuspensionDetachEntitiesRequest(String timeSpanSchedule_Uuid,
                                                                         SuspendableEntityUUIDSetDTO entityUuids)
            throws Exception {

        SuspensionDetachEntitiesRequest.Builder requestBuilder = SuspensionDetachEntitiesRequest.getDefaultInstance().newBuilder();
        requestBuilder.setScheduleOid(Long.parseLong(timeSpanSchedule_Uuid));
        if (entityUuids.hasEntityUuids()) {
            for (String entityUuid : entityUuids.getEntityUuids()) {
                requestBuilder.addEntityOids(Long.parseLong(entityUuid));
            }
        }
        return requestBuilder.build();
    }

    /**
     * converts SuspensionDetachEntitiesResponse class into the list of ScheduleEntityResponseApiDTO class.
     * @param resp of SuspensionDetachEntitiesResponse class.
     * @return List of ScheduleEntityResponseApiDTO class.
     */
    public List<ScheduleEntityResponseApiDTO> toScheduleEntityResponseApiDTOForDetach(SuspensionDetachEntitiesResponse resp) {
        List<ScheduleEntityResponseApiDTO> list = new ArrayList<ScheduleEntityResponseApiDTO>();
        for (SuspensionScheduleEntityError entity : resp.getErrorList()) {
            ScheduleEntityResponseApiDTO data = new ScheduleEntityResponseApiDTO();
            if (entity.hasEntityOid()) {
                data.setEntityUUID(Long.toString(entity.getEntityOid()));
            }
            if (entity.hasError()) {
                data.setError(entity.getError());
            }
            list.add(data);
        }
        return list;
    }

    /**
     * converts the timeSpanSchedule_Uuid of string into SuspensionGetEntitiesRequest class.
     *
     * @param timeSpanSchedule_Uuid This is representing the unique time span based schedule.
     * @return SuspensionGetEntitiesRequest the get entities list object.
     * @throws Exception when any error occurs.
     */
    public SuspensionGetEntitiesRequest toGetEntitiesRequest(
            String timeSpanSchedule_Uuid)
            throws Exception {

        SuspensionGetEntitiesRequest.Builder requestBuilder = SuspensionGetEntitiesRequest.getDefaultInstance().newBuilder();

        requestBuilder.setScheduleOid(Long.parseLong(timeSpanSchedule_Uuid));

        return requestBuilder.build();
    }


    /**
     * converts the SuspensionGetEntitiesResponse of class into SuspendableEntityUUIDSetDTO class.
     */
    public SuspendableEntityUUIDSetDTO toSuspendableEntityUUIDSetDTO(
            SuspensionGetEntitiesResponse grpcSchedule)
            throws Exception {

        SuspendableEntityUUIDSetDTO timespansEntities = new SuspendableEntityUUIDSetDTO();

        List<String> entityUuidsArr = new ArrayList<String>(grpcSchedule.getEntityOidsCount());

        for (long entityUuid : grpcSchedule.getEntityOidsList()) {
            entityUuidsArr.add(String.valueOf(entityUuid));
        }

        timespansEntities.setEntityUuids(entityUuidsArr);

        return timespansEntities;
    }
}
