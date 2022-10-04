package com.vmturbo.api.component.external.api.mapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

import javax.annotation.Nullable;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;

import com.vmturbo.api.dto.entity.TagApiDTO;
import com.vmturbo.api.dto.group.FilterApiDTO;
import com.vmturbo.api.dto.suspension.BulkActionRequestApiDTO;
import com.vmturbo.api.dto.suspension.BulkActionRequestInputDTO;
import com.vmturbo.api.dto.suspension.SuspendableEntityApiDTO;
import com.vmturbo.api.dto.suspension.SuspendableEntityInputDTO;
import com.vmturbo.api.enums.CloudType;
import com.vmturbo.api.enums.LogicalOperator;
import com.vmturbo.api.enums.SuspensionActionType;
import com.vmturbo.api.enums.SuspensionEntityType;
import com.vmturbo.api.enums.SuspensionState;
import com.vmturbo.api.pagination.SuspensionEntitiesOrderBy;
import com.vmturbo.api.pagination.SuspensionEntitiesPaginationRequest;
import com.vmturbo.auth.api.authorization.UserContextUtils;
import com.vmturbo.common.protobuf.suspension.SuspensionEntityOuterClass;
import com.vmturbo.common.protobuf.suspension.SuspensionEntityOuterClass.CurrencyAmount;
import com.vmturbo.common.protobuf.suspension.SuspensionEntityOuterClass.SuspensionEntity;
import com.vmturbo.common.protobuf.suspension.SuspensionEntityOuterClass.SuspensionEntityRequest;
import com.vmturbo.common.protobuf.suspension.SuspensionEntityOuterClass.SuspensionEntityResolvedScope;
import com.vmturbo.common.protobuf.suspension.SuspensionEntityOuterClass.SuspensionEntityResolvedScopeRequest;
import com.vmturbo.common.protobuf.suspension.SuspensionEntityOuterClass.SuspensionEntityTags;
import com.vmturbo.common.protobuf.suspension.SuspensionFilter;
import com.vmturbo.common.protobuf.suspension.SuspensionToggle.SuspensionToggleEntityData;
import com.vmturbo.common.protobuf.suspension.SuspensionToggle.SuspensionToggleEntityRequest;

/**
 * SuspensionMapper.
 * This is used convert data from grpc to api type or api to grpc type.
 */
public class SuspensionMapper {
    private final int validPaginationMaxLimit = 500;

    private final int validPaginationDefaultLimit = 300;

    private final int apiPaginationDefaultLimit;

    private final int apiPaginationMaxLimit;

    /**
     * The map from GRPC suspension entity state to API suspension entity state.
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
     * constructor for the SuspensionMapper.
     */
    public SuspensionMapper(@Nullable final int apiPaginationMaxLimit,
                            @Nullable final int apiPaginationDefaultLimit) {
        this.apiPaginationMaxLimit = apiPaginationMaxLimit < 1 ? validPaginationMaxLimit : apiPaginationMaxLimit;
        this.apiPaginationDefaultLimit = apiPaginationDefaultLimit < 1 ? validPaginationDefaultLimit : apiPaginationDefaultLimit;
    }

    /**
     * constructor with default values of SuspensionMapper.
     */
    public SuspensionMapper() {
        this.apiPaginationMaxLimit = validPaginationMaxLimit;
        this.apiPaginationDefaultLimit = validPaginationDefaultLimit;
    }

    /**
     * Gets the api suspension entity state based on grpc suspension entity state.
     * @param state the grpc suspension entity state.
     * @return the api state.
     */
    public static SuspensionState getApiEntityState(SuspensionEntityOuterClass.SuspensionEntityState state) {
        return GRPC_TO_API_SUSPENSION_STATE.get(state);
    }

    /**
     * Gets the grpc suspension entity state based on api suspension entity state.
     * @param state the api suspension entity state.
     * @return the grpc state.
     */
    public static SuspensionEntityOuterClass.SuspensionEntityState getGrpcEntityState(SuspensionState state) {
        return GRPC_TO_API_SUSPENSION_STATE.inverse().get(state);
    }

    /**
     * The map from GRPC suspension entity types to API suspension entity type.
     */
    public static final BiMap<SuspensionEntityOuterClass.SuspensionEntityType, SuspensionEntityType> GRPC_TO_API_SUSPENSION_TYPE = ImmutableBiMap.of(
                    SuspensionEntityOuterClass.SuspensionEntityType.SUSPENSION_ENTITY_TYPE_COMPUTE, SuspensionEntityType.VirtualMachine);

    /**
     * Gets the api suspension entity type based on grpc suspension entity type.
     * @param type the grpc suspension entity type.
     * @return the api type.
     */
    public static SuspensionEntityType getApiEntityType(SuspensionEntityOuterClass.SuspensionEntityType type) {
        return GRPC_TO_API_SUSPENSION_TYPE.get(type);
    }

    /**
     * Gets the grpc suspension entity type based on api suspension entity type.
     * @param type the api suspension entity type.
     * @return the grpc type.
     */
    public static SuspensionEntityOuterClass.SuspensionEntityType getGrpcEntityType(SuspensionEntityType type) {
        return GRPC_TO_API_SUSPENSION_TYPE.inverse().get(type);
    }

    /**
     * The map from GRPC suspension entity provider to API suspension entity provider.
     */
    public static final BiMap<SuspensionEntityOuterClass.SuspensionEntityProvider, CloudType> GRPC_TO_API_PROVIDER = ImmutableBiMap.of(
                    SuspensionEntityOuterClass.SuspensionEntityProvider.SUSPENSION_ENTITY_PROVIDER_AWS, CloudType.AWS,
                    SuspensionEntityOuterClass.SuspensionEntityProvider.SUSPENSION_ENTITY_PROVIDER_AZURE, CloudType.AZURE,
                    SuspensionEntityOuterClass.SuspensionEntityProvider.SUSPENSION_ENTITY_PROVIDER_GCP, CloudType.GCP);

    /**
     * Gets the api suspension entity provider based on grpc suspension entity provider.
     * @param provider the grpc entity provider.
     * @return the api provider.
     */
    public static CloudType getProvider(SuspensionEntityOuterClass.SuspensionEntityProvider provider) {
        return GRPC_TO_API_PROVIDER.get(provider);
    }

    /**
     * Gets the grpc suspension entity provider based on api suspension entity provider.
     * @param type the api entity provider.
     * @return the grpc provider.
     */
    public static SuspensionEntityOuterClass.SuspensionEntityProvider getGrpcProvider(CloudType provider) {
        return GRPC_TO_API_PROVIDER.inverse().get(provider);
    }

    /**
     * The map from API suspension entity order by fields to GRPC suspension entity order by.
     */
    public static final BiMap<SuspensionEntitiesOrderBy, SuspensionEntityOuterClass.SuspensionEntityOrderBy> API_TO_GRPC_SUSPENSION_ORDER_BY = ImmutableBiMap.of(
                    SuspensionEntitiesOrderBy.DISPLAY_NAME, SuspensionEntityOuterClass.SuspensionEntityOrderBy.SUSPENSION_ENTITY_ORDER_BY_DISPLAY_NAME,
                    SuspensionEntitiesOrderBy.ENTITY_TYPE, SuspensionEntityOuterClass.SuspensionEntityOrderBy.SUSPENSION_ENTITY_ORDER_BY_TYPE,
                    SuspensionEntitiesOrderBy.STATE, SuspensionEntityOuterClass.SuspensionEntityOrderBy.SUSPENSION_ENTITY_ORDER_BY_STATE,
                    SuspensionEntitiesOrderBy.ACCOUNT_NAME, SuspensionEntityOuterClass.SuspensionEntityOrderBy.SUSPENSION_ENTITY_ORDER_BY_ACCOUNT_NAME,
                    SuspensionEntitiesOrderBy.PROVIDER, SuspensionEntityOuterClass.SuspensionEntityOrderBy.SUSPENSION_ENTITY_ORDER_BY_PROVIDER,
                    SuspensionEntitiesOrderBy.REGION_NAME, SuspensionEntityOuterClass.SuspensionEntityOrderBy.SUSPENSION_ENTITY_ORDER_BY_REGION_NAME,
                    SuspensionEntitiesOrderBy.INSTANCE_TYPE, SuspensionEntityOuterClass.SuspensionEntityOrderBy.SUSPENSION_ENTITY_ORDER_BY_INSTANCE_TYPE,
                    SuspensionEntitiesOrderBy.COST, SuspensionEntityOuterClass.SuspensionEntityOrderBy.SUSPENSION_ENTITY_ORDER_BY_COST);

    /**
     * Gets the api entity type based on grpc entity type.
     * @param type the grpc entity type.
     * @return the api type.
     */
    public static SuspensionEntityOuterClass.SuspensionEntityOrderBy getOrderBy(SuspensionEntitiesOrderBy orderBy) {
        return API_TO_GRPC_SUSPENSION_ORDER_BY.getOrDefault(orderBy, SuspensionEntityOuterClass.SuspensionEntityOrderBy.SUSPENSION_ENTITY_ORDER_BY_DISPLAY_NAME);
    }

    /**
     * The filters applicable for suspension entity as string filters.
     */
    public static final BiMap<String, SuspensionFilter.ExpType> STRING_OPERATORS = ImmutableBiMap.of(
                    "EQ", SuspensionFilter.ExpType.EXP_TYPE_EQ,
                    "NEQ", SuspensionFilter.ExpType.EXP_TYPE_NEQ,
                    "RXEQ", SuspensionFilter.ExpType.EXP_TYPE_RX_EQ,
                    "RXNEQ", SuspensionFilter.ExpType.EXP_TYPE_RX_NEQ);

    /**
     * The filters applicable for suspension entity as int filters.
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
     * creates SuspensionEntityRequest based on the filters provided in the SuspendableEntityInputDTO.
     * @param SuspendableEntityInputDTO the query param for get suspension entities api.
     * @return SuspensionEntityRequest.
     */
    public SuspensionEntityRequest createSuspensionEntityRequestFromCriteriaList(SuspendableEntityInputDTO entityInputDTO) {
        HashMap<String, String> fieldMap = new HashMap<>();
        fieldMap.put("displayName", "string");
        fieldMap.put("accountName", "string");
        fieldMap.put("regionName", "string");
        fieldMap.put("instanceType", "string");
        fieldMap.put("tags", "string");
        fieldMap.put("cost", "double");

        HashMap<String, List<SuspensionFilter.StringFilter>> protoStringMap = new HashMap<>();
        HashMap<String, List<SuspensionFilter.DoubleFilter>> protoDoubleMap = new HashMap<>();
        List<FilterApiDTO> criteriaList = entityInputDTO.getCriteriaList();
        for (int i = 0; i < criteriaList.size(); ++i) {
            if (fieldMap.get(criteriaList.get(i).getFilterType()) == "string") {
                if (!STRING_OPERATORS.containsKey(criteriaList.get(i).getExpType())) {
                    throw new IllegalArgumentException("Invalid expression type for string filter");
                }
                if (protoStringMap.get(criteriaList.get(i).getFilterType()) == null) {
                    protoStringMap.put(criteriaList.get(i).getFilterType(), new ArrayList<>());
                }
                protoStringMap.get(criteriaList.get(i).getFilterType()).add(SuspensionFilter.StringFilter.newBuilder()
                        .setExpType(STRING_OPERATORS.get(criteriaList.get(i).getExpType()))
                        .addValues(criteriaList.get(i).getExpVal()).build());
            } else if (fieldMap.get(criteriaList.get(i).getFilterType()) == "double") {
                if (!NUMBER_OPERATORS.containsKey(criteriaList.get(i).getExpType())) {
                    throw new IllegalArgumentException("Invalid expression type for double filter");
                }
                if (protoDoubleMap.get(criteriaList.get(i).getFilterType()) == null) {
                    protoDoubleMap.put(criteriaList.get(i).getFilterType(), new ArrayList<>());
                }
                protoDoubleMap.get(criteriaList.get(i).getFilterType()).add(SuspensionFilter.DoubleFilter.newBuilder()
                        .setExpType(NUMBER_OPERATORS.get(criteriaList.get(i).getExpType()))
                        .addValues(Double.parseDouble(criteriaList.get(i).getExpVal())).build());
            } else {
                throw new IllegalArgumentException("Invalid filter type " + criteriaList.get(i).getFilterType());
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
        if (protoStringMap.get("tags") != null) {
            requestBuilder.addAllTags(protoStringMap.get("tags"));
        }
        if (protoDoubleMap.get("cost") != null) {
            requestBuilder.addAllCost(protoDoubleMap.get("cost"));
        }
        return requestBuilder.build();
    }

    /**
     * creates SuspensionEntityRequest based on the filters provided in the SuspendableEntityInputDTO and also pagination query params.
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
            for (int i = 0; i < entityInputDTO.getEntityTypes().size(); ++i) {
                requestBuilder.addType(getGrpcEntityType(entityInputDTO.getEntityTypes().get(i)));
            }
        }
        if (entityInputDTO.hasStatus()) {
            for (int i = 0; i < entityInputDTO.getStatus().size(); ++i) {
                requestBuilder.addStatus(getGrpcEntityState(entityInputDTO.getStatus().get(i)));
            }
        }
        if (entityInputDTO.hasProviders()) {
            for (int i = 0; i < entityInputDTO.getProviders().size(); ++i) {
                requestBuilder.addProviders(getGrpcProvider(entityInputDTO.getProviders().get(i)));
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
     * to append the SuspensionEntityRequest based on the filters provided in the SuspendableEntityInputDTO and also pagination query params with group scopes.
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
     * to append the SuspensionEntityRequest based on the filters provided in the SuspendableEntityInputDTO and also pagination query params with entity scopes.
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
            for (int i = 0; i < scopeEntityOids.size(); ++i) {
                SuspensionEntityResolvedScopeRequest.Builder scopeRequestBuilder = SuspensionEntityResolvedScopeRequest.getDefaultInstance().newBuilder();
                scopeRequestBuilder.setOid(scopeEntityOids.get(i));
                scopeRequestBuilder.setType(SuspensionEntityOuterClass.SuspensionEntityType.SUSPENSION_ENTITY_TYPE_COMPUTE);
                scopeBuilder.addEntities(scopeRequestBuilder.build());
            }
            scopeBuilder.setOid(Long.valueOf(1));
            requestBuilder.addResolvedScopes(scopeBuilder.build());
        }

        return requestBuilder.build();
    }

    /**
     * converts the list of SuspensionEntity grpc class into SuspendableEntityApiDTO class.
     * @param List of SuspensionEntity class.
     * @return List of SuspendableEntityApiDTO class.
     */
    public List<SuspendableEntityApiDTO> convertToSuspendableEntityApiDTO(List<SuspensionEntity> entities) {
        List<SuspendableEntityApiDTO> entityList = new ArrayList<SuspendableEntityApiDTO>();
        for (int i = 0; i < entities.size(); ++i) {
            SuspensionEntity entity = entities.get(i);
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
     * converts the list of Tags from SuspensionEntityTags grpc class into TagApiDTO class.
     * @param List of Tags from SuspensionEntityTags grpc.
     * @return List of TagApiDTO class.
     */
    public List<TagApiDTO> convertToTagApiDTO(List<SuspensionEntityTags> tags) {
        List<TagApiDTO> tagsList = new ArrayList<TagApiDTO>();
        for (int i = 0; i < tags.size(); ++i) {
            SuspensionEntityTags tag = tags.get(i);
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
     * The map from API suspension action to GRPC Toggle Action type.
     */
    public static final BiMap<SuspensionActionType, SuspensionToggleEntityRequest.Action> API_ACTION_TO_GRPC_GRPC  = ImmutableBiMap.of(
                    SuspensionActionType.START, SuspensionToggleEntityRequest.Action.ACTION_START,
                    SuspensionActionType.STOP, SuspensionToggleEntityRequest.Action.ACTION_STOP);

    /**
     * Gets the grpc suspension entity action based on api suspension entity action.
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
     * converts the BulkActionRequestInputDTO of class into SuspensionToggleEntityRequest class.
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
     * converts the list of SuspensionToggleEntityData class into BulkActionRequestApiDTO class.
     * @param List of SuspensionToggleEntityData class.
     * @return List of BulkActionRequestApiDTO class.
     */
    public List<BulkActionRequestApiDTO> convertToBulkActionRequestApiDTO(List<SuspensionToggleEntityData> entities) {
        List<BulkActionRequestApiDTO> list = new ArrayList<BulkActionRequestApiDTO>();
        for (int i = 0; i < entities.size(); ++i) {
            SuspensionToggleEntityData entity = entities.get(i);
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
}
