package com.vmturbo.api.component.external.api.mapper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.collect.ImmutableList;

import io.grpc.Context;

import junit.framework.TestCase;

import org.junit.Test;
import org.springframework.security.core.context.SecurityContextHolder;

import com.vmturbo.api.dto.entity.TagApiDTO;
import com.vmturbo.api.dto.group.FilterApiDTO;
import com.vmturbo.api.dto.suspension.BulkActionRequestApiDTO;
import com.vmturbo.api.dto.suspension.BulkActionRequestInputDTO;
import com.vmturbo.api.dto.suspension.ScheduleEntityResponseApiDTO;
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
import com.vmturbo.auth.api.authorization.jwt.SecurityConstant;
import com.vmturbo.common.protobuf.suspension.SuspensionEntityOuterClass;
import com.vmturbo.common.protobuf.suspension.SuspensionEntityOuterClass.SuspensionEntity;
import com.vmturbo.common.protobuf.suspension.SuspensionEntityOuterClass.SuspensionEntityRequest;
import com.vmturbo.common.protobuf.suspension.SuspensionEntityOuterClass.SuspensionEntityRequest.Builder;
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
import com.vmturbo.common.protobuf.suspension.SuspensionToggle;

/**
 * Verifies the mappers and conversion functions in {@link SuspensionMapper}.
 */
public class SuspensionMapperTest extends TestCase {

    private SuspensionMapper testSuspensionMapper = new SuspensionMapper();

    /**
     * verifies the conversion of state from grpc to api.
     */
    @Test
    public void testGetApiEntityState() {
        assertNull(testSuspensionMapper.getApiEntityState(SuspensionEntityOuterClass.SuspensionEntityState.SUSPENSION_ENTITY_STATE_UNSPECIFIED));
        assertEquals(SuspensionState.STOPPED, testSuspensionMapper.getApiEntityState(SuspensionEntityOuterClass.SuspensionEntityState.SUSPENSION_ENTITY_STATE_STOPPED));
        assertEquals(SuspensionState.RUNNING, testSuspensionMapper.getApiEntityState(SuspensionEntityOuterClass.SuspensionEntityState.SUSPENSION_ENTITY_STATE_RUNNING));
        assertEquals(SuspensionState.STARTING, testSuspensionMapper.getApiEntityState(SuspensionEntityOuterClass.SuspensionEntityState.SUSPENSION_ENTITY_STATE_STARTING));
        assertEquals(SuspensionState.STOPPING, testSuspensionMapper.getApiEntityState(SuspensionEntityOuterClass.SuspensionEntityState.SUSPENSION_ENTITY_STATE_STOPPING));
        assertEquals(SuspensionState.TERMINATING, testSuspensionMapper.getApiEntityState(SuspensionEntityOuterClass.SuspensionEntityState.SUSPENSION_ENTITY_STATE_TERMINATING));
        assertEquals(SuspensionState.TERMINATED, testSuspensionMapper.getApiEntityState(SuspensionEntityOuterClass.SuspensionEntityState.SUSPENSION_ENTITY_STATE_TERMINATED));
        assertEquals(SuspensionState.MISSING, testSuspensionMapper.getApiEntityState(SuspensionEntityOuterClass.SuspensionEntityState.SUSPENSION_ENTITY_STATE_MISSING));
    }

    /**
     * verifies the conversion of state from api to grpc.
     */
    @Test
    public void testGetGrpcEntityState() {
        assertEquals(SuspensionEntityOuterClass.SuspensionEntityState.SUSPENSION_ENTITY_STATE_STOPPED, testSuspensionMapper.getGrpcEntityState(SuspensionState.STOPPED));
        assertEquals(SuspensionEntityOuterClass.SuspensionEntityState.SUSPENSION_ENTITY_STATE_RUNNING, testSuspensionMapper.getGrpcEntityState(SuspensionState.RUNNING));
        assertEquals(SuspensionEntityOuterClass.SuspensionEntityState.SUSPENSION_ENTITY_STATE_STARTING, testSuspensionMapper.getGrpcEntityState(SuspensionState.STARTING));
        assertEquals(SuspensionEntityOuterClass.SuspensionEntityState.SUSPENSION_ENTITY_STATE_STOPPING, testSuspensionMapper.getGrpcEntityState(SuspensionState.STOPPING));
        assertEquals(SuspensionEntityOuterClass.SuspensionEntityState.SUSPENSION_ENTITY_STATE_TERMINATING, testSuspensionMapper.getGrpcEntityState(SuspensionState.TERMINATING));
        assertEquals(SuspensionEntityOuterClass.SuspensionEntityState.SUSPENSION_ENTITY_STATE_TERMINATED, testSuspensionMapper.getGrpcEntityState(SuspensionState.TERMINATED));
        assertEquals(SuspensionEntityOuterClass.SuspensionEntityState.SUSPENSION_ENTITY_STATE_MISSING, testSuspensionMapper.getGrpcEntityState(SuspensionState.MISSING));
    }

    /**
     * verifies the conversion of type from grpc to api.
     */
    @Test
    public void testGetApiEntityType() {
        assertNull(testSuspensionMapper.getApiEntityType(SuspensionEntityOuterClass.SuspensionEntityType.SUSPENSION_ENTITY_TYPE_UNSPECIFIED));
        assertEquals(SuspensionEntityType.VirtualMachine, testSuspensionMapper.getApiEntityType(SuspensionEntityOuterClass.SuspensionEntityType.SUSPENSION_ENTITY_TYPE_COMPUTE));
    }

    /**
     * verifies the conversion of type from api to grpc.
     */
    @Test
    public void testGetGrpcEntityType() {
        assertEquals(SuspensionEntityOuterClass.SuspensionEntityType.SUSPENSION_ENTITY_TYPE_COMPUTE, testSuspensionMapper.getGrpcEntityType(SuspensionEntityType.VirtualMachine));
    }

    /**
     * verifies the conversion of provider from grpc to api.
     */
    @Test
    public void testGetProvider() {
        assertEquals(CloudType.AWS, testSuspensionMapper.getProvider(SuspensionEntityOuterClass.SuspensionEntityProvider.SUSPENSION_ENTITY_PROVIDER_AWS));
        assertEquals(CloudType.AZURE, testSuspensionMapper.getProvider(SuspensionEntityOuterClass.SuspensionEntityProvider.SUSPENSION_ENTITY_PROVIDER_AZURE));
        assertEquals(CloudType.GCP, testSuspensionMapper.getProvider(SuspensionEntityOuterClass.SuspensionEntityProvider.SUSPENSION_ENTITY_PROVIDER_GCP));
        assertNull(testSuspensionMapper.getProvider(SuspensionEntityOuterClass.SuspensionEntityProvider.SUSPENSION_ENTITY_PROVIDER_UNSPECIFIED));
    }

    /**
     * verifies the conversion of provider from api to grpc.
     */
    @Test
    public void testGetGrpcProvider() {
        assertEquals(SuspensionEntityOuterClass.SuspensionEntityProvider.SUSPENSION_ENTITY_PROVIDER_AWS, testSuspensionMapper.getGrpcProvider(CloudType.AWS));
        assertEquals(SuspensionEntityOuterClass.SuspensionEntityProvider.SUSPENSION_ENTITY_PROVIDER_AZURE, testSuspensionMapper.getGrpcProvider(CloudType.AZURE));
        assertEquals(SuspensionEntityOuterClass.SuspensionEntityProvider.SUSPENSION_ENTITY_PROVIDER_GCP, testSuspensionMapper.getGrpcProvider(CloudType.GCP));
    }

    /**
     * verifies the conversion of column to sort by from api to grpc.
     */
    @Test
    public void testGetOrderBy() {
        assertEquals(SuspensionEntityOuterClass.SuspensionEntityOrderBy.SUSPENSION_ENTITY_ORDER_BY_DISPLAY_NAME, testSuspensionMapper.getOrderBy(SuspensionEntitiesOrderBy.DISPLAY_NAME));
        assertEquals(SuspensionEntityOuterClass.SuspensionEntityOrderBy.SUSPENSION_ENTITY_ORDER_BY_TYPE, testSuspensionMapper.getOrderBy(SuspensionEntitiesOrderBy.ENTITY_TYPE));
        assertEquals(SuspensionEntityOuterClass.SuspensionEntityOrderBy.SUSPENSION_ENTITY_ORDER_BY_STATE, testSuspensionMapper.getOrderBy(SuspensionEntitiesOrderBy.STATE));
        assertEquals(SuspensionEntityOuterClass.SuspensionEntityOrderBy.SUSPENSION_ENTITY_ORDER_BY_ACCOUNT_NAME, testSuspensionMapper.getOrderBy(SuspensionEntitiesOrderBy.ACCOUNT_NAME));
        assertEquals(SuspensionEntityOuterClass.SuspensionEntityOrderBy.SUSPENSION_ENTITY_ORDER_BY_PROVIDER, testSuspensionMapper.getOrderBy(SuspensionEntitiesOrderBy.PROVIDER));
        assertEquals(SuspensionEntityOuterClass.SuspensionEntityOrderBy.SUSPENSION_ENTITY_ORDER_BY_REGION_NAME, testSuspensionMapper.getOrderBy(SuspensionEntitiesOrderBy.REGION_NAME));
        assertEquals(SuspensionEntityOuterClass.SuspensionEntityOrderBy.SUSPENSION_ENTITY_ORDER_BY_INSTANCE_TYPE, testSuspensionMapper.getOrderBy(SuspensionEntitiesOrderBy.INSTANCE_TYPE));
        assertEquals(SuspensionEntityOuterClass.SuspensionEntityOrderBy.SUSPENSION_ENTITY_ORDER_BY_COST, testSuspensionMapper.getOrderBy(SuspensionEntitiesOrderBy.COST));
    }

    /**
     * verifies the conversion of LogicalOperator from api to gRPC.
     */
    @Test
    public void testGetGrpcLogicalOperator() {
        assertEquals(SuspensionFilter.LogicalOperator.AND,
                testSuspensionMapper.getGrpcLogicalOperator(LogicalOperator.AND));
        assertEquals(SuspensionFilter.LogicalOperator.OR,
                testSuspensionMapper.getGrpcLogicalOperator(LogicalOperator.OR));
    }

    /**
     * verifies the conversion of SuspendableEntityInputDTO from api to SuspensionEntityRequest of grpc.
     */
    @Test
    public void testCreateSuspensionEntityRequest() throws Exception {
        List<String> stringList = new ArrayList<String>();
        stringList.add("Value1");
        stringList.add("Value2");
        List<Long> longList = new ArrayList<Long>();
        Long l1 = new Long(1);
        Long l2 = new Long(2);
        longList.add(l1);
        longList.add(l2);

        SuspendableEntityInputDTO entityInputDTO = new SuspendableEntityInputDTO();

        List<CloudType> providers = new ArrayList<CloudType>();
        providers.add(CloudType.AWS);
        entityInputDTO.setProviders(providers);

        List<SuspensionState> states = new ArrayList<SuspensionState>();
        states.add(SuspensionState.STOPPED);
        entityInputDTO.setStatus(states);

        List<SuspensionEntityType> types = new ArrayList<SuspensionEntityType>();
        types.add(SuspensionEntityType.VirtualMachine);
        entityInputDTO.setEntityTypes(types);

        Builder requestBuilder = SuspensionEntityRequest.getDefaultInstance().newBuilder();

        requestBuilder.addType(SuspensionEntityOuterClass.SuspensionEntityType.SUSPENSION_ENTITY_TYPE_COMPUTE);
        requestBuilder.addStatus(SuspensionEntityOuterClass.SuspensionEntityState.SUSPENSION_ENTITY_STATE_STOPPED);
        requestBuilder.setOrderBy(SuspensionEntityOuterClass.SuspensionEntityOrderBy.SUSPENSION_ENTITY_ORDER_BY_DISPLAY_NAME);
        requestBuilder.addProviders(SuspensionEntityOuterClass.SuspensionEntityProvider.SUSPENSION_ENTITY_PROVIDER_AWS);
        requestBuilder.setLogicalOperator(SuspensionFilter.LogicalOperator.AND);
        requestBuilder.setLimit(300);
        requestBuilder.setDescending(false);
        SuspensionEntitiesPaginationRequest paginationRequest = new SuspensionEntitiesPaginationRequest();
        assertEquals(requestBuilder.build(), testSuspensionMapper.createSuspensionEntityRequest(entityInputDTO, paginationRequest));

    }

    /**
     * verifies the conversion of array of SuspensionEntity from grpc to array of SuspendableEntityApiDTO of api.
     */
    @Test
    public void testConvertToSuspendableEntityApiDTO() {
        SuspensionEntity entity1 = SuspensionEntity.newBuilder()
               .setOid(1)
               .setName("testName")
               .setSuspendable(false)
               .setAccountOid(1)
               .setRegionOid(1)
               .setProvider(SuspensionEntityOuterClass.SuspensionEntityProvider.SUSPENSION_ENTITY_PROVIDER_AWS)
               .setState(SuspensionEntityOuterClass.SuspensionEntityState.SUSPENSION_ENTITY_STATE_STOPPED)
               .setEntityType(SuspensionEntityOuterClass.SuspensionEntityType.SUSPENSION_ENTITY_TYPE_COMPUTE)
               .setAccountName("testAccountName")
               .setInstanceType("testInstanceType")
               .setRegionName("testRegionName")
               .build();

        List<SuspensionEntity> entities = new ArrayList<SuspensionEntity>();
        entities.add(entity1);

        List<SuspendableEntityApiDTO> entityApiDTOs = testSuspensionMapper.convertToSuspendableEntityApiDTO(entities);
        assertEquals(1, entityApiDTOs.size());
        assertEquals("testName", entityApiDTOs.get(0).getDisplayName());
        assertEquals(SuspensionEntityType.VirtualMachine, entityApiDTOs.get(0).getEntityType());
        assertEquals(SuspensionState.STOPPED, entityApiDTOs.get(0).getState());
        assertEquals(Long.valueOf(1), entityApiDTOs.get(0).getAccountOID());
        assertEquals(CloudType.AWS, entityApiDTOs.get(0).getProvider());
        assertEquals(Long.valueOf(1), entityApiDTOs.get(0).getRegionOID());
        assertEquals("1", entityApiDTOs.get(0).getUuid());
        assertEquals("testInstanceType", entityApiDTOs.get(0).getInstanceType());
        assertEquals("testAccountName", entityApiDTOs.get(0).getAccountName());
        assertEquals("testRegionName", entityApiDTOs.get(0).getRegionName());
    }

    /**
     * verifies if the scope oids are getting added to the SuspensionEntityRequest being sent to suspension service.
     */
    @Test
    public void testCreateSuspensionEntityRequestWithScope() throws Exception {
        Builder requestBuilder = SuspensionEntityRequest.getDefaultInstance().newBuilder();
        requestBuilder.addScopes(Long.valueOf(1));
        requestBuilder.setOrderBy(SuspensionEntityOuterClass.SuspensionEntityOrderBy.SUSPENSION_ENTITY_ORDER_BY_DISPLAY_NAME);
        requestBuilder.setLimit(300);
       requestBuilder.setLogicalOperator(SuspensionFilter.LogicalOperator.OR);
        requestBuilder.setDescending(false);
        List<Long> scopeoids = new ArrayList<Long>();
        scopeoids.add(Long.valueOf(1));
        SuspensionEntitiesPaginationRequest paginationRequest = new SuspensionEntitiesPaginationRequest();
        SuspendableEntityInputDTO entityInputDTO = new SuspendableEntityInputDTO();
        entityInputDTO.setLogicalOperator(LogicalOperator.OR);
        assertEquals(requestBuilder.build(), testSuspensionMapper.createSuspensionEntityRequestWithScope(entityInputDTO, paginationRequest, scopeoids));
    }

    /**
     * verifies if the resolved scope oids are getting added to the SuspensionEntityRequest being sent to suspension service.
     */
    @Test
    public void testSuspensionEntityRequestWithResolvedScopes() {

        SuspensionEntityOuterClass.SuspensionEntityResolvedScopeRequest.Builder scopeReqestBuilder = SuspensionEntityOuterClass.SuspensionEntityResolvedScopeRequest.getDefaultInstance().newBuilder();
        scopeReqestBuilder.setOid(Long.valueOf(1));
        scopeReqestBuilder.setType(SuspensionEntityOuterClass.SuspensionEntityType.SUSPENSION_ENTITY_TYPE_COMPUTE);

        SuspensionEntityOuterClass.SuspensionEntityResolvedScope.Builder scopeBuilder = SuspensionEntityOuterClass.SuspensionEntityResolvedScope.getDefaultInstance().newBuilder();
        scopeBuilder.setOid(Long.valueOf(1));
        scopeBuilder.addEntities(scopeReqestBuilder.build());

        Builder requestBuilder = SuspensionEntityRequest.getDefaultInstance().newBuilder();
        requestBuilder.setOrderBy(SuspensionEntityOuterClass.SuspensionEntityOrderBy.SUSPENSION_ENTITY_ORDER_BY_DISPLAY_NAME);
        requestBuilder.setLogicalOperator(SuspensionFilter.LogicalOperator.AND);
        requestBuilder.setLimit(300);
        requestBuilder.setDescending(false);
        requestBuilder.addResolvedScopes(scopeBuilder.build());

        SuspendableEntityInputDTO entityInputDTO = new SuspendableEntityInputDTO();
        List<Long> scopeoids = new ArrayList<Long>();
        scopeoids.add(Long.valueOf(1));

        SuspensionEntitiesPaginationRequest paginationRequest = new SuspensionEntitiesPaginationRequest();
        assertEquals(requestBuilder.build(), testSuspensionMapper.createSuspensionEntityRequestWithResolvedScopes(entityInputDTO, paginationRequest, scopeoids));
    }

    /**
     * verifies the conversion of suspension entity tags from grpc to array of Tags api.
     */
    @Test
    public void testConvertToTagApiDTO() {
        List<String> values = new ArrayList<String>();
        values.add("value1");
        values.add("value2");

        List<SuspensionEntityTags> tags = new ArrayList<SuspensionEntityTags>();
        SuspensionEntityTags tag = SuspensionEntityTags.newBuilder()
                .setKey("testKey")
                .addAllValues(values)
                .build();
        tags.add(tag);
        List<TagApiDTO> tagsApiDTOs = testSuspensionMapper.convertToTagApiDTO(tags);
        assertEquals(1, tagsApiDTOs.size() );
        assertEquals("testKey", tagsApiDTOs.get(0).getKey());
        assertEquals(values, tagsApiDTOs.get(0).getValues());
    }

    /**
     * verifies the conversion of suspension entity schedules from grpc to array of schedules attached to entities in api.
     */
    @Test
    public void testConvertToSuspendableEntityScheduleApiDTO() {

        List<SuspensionEntitySchedule> schedules = new ArrayList<SuspensionEntitySchedule>();
        SuspensionEntitySchedule schedule = SuspensionEntitySchedule.newBuilder()
                .setOid(1234)
                .setName("testName")
                .build();
        schedules.add(schedule);
        List<SuspendableEntityScheduleApiDTO> suspendableEntityScheduleApiDTOs = testSuspensionMapper.convertToSuspendableEntityScheduleApiDTO(schedules);
        assertEquals(1, suspendableEntityScheduleApiDTOs.size() );
        assertEquals("testName", suspendableEntityScheduleApiDTOs.get(0).getDisplayName());
        assertEquals("1234", suspendableEntityScheduleApiDTOs.get(0).getScheduleUUID());
    }

    /**
     * verifies the conversion of suspension Toggle Action from API to grpc toggle action type.
     */
    @Test
    public void testGetGrpcToggleActionType() {
        assertEquals(SuspensionToggle.SuspensionToggleEntityRequest.Action.ACTION_STOP, testSuspensionMapper.getGrpcToggleActionType(SuspensionActionType.STOP));
        assertEquals(SuspensionToggle.SuspensionToggleEntityRequest.Action.ACTION_START, testSuspensionMapper.getGrpcToggleActionType(SuspensionActionType.START));
    }

    /**
     * verifies the creation of toggle action request from API to grpc toggle action request.
     */
    @Test
    public void testCreateSuspensionToggleEntityRequest() throws SuspensionMapper.InvalidRequest {

        BulkActionRequestInputDTO bulkActionRequestInputDTO = new BulkActionRequestInputDTO();
        bulkActionRequestInputDTO.setAction(SuspensionActionType.STOP);
        List<String> entityUuids = new ArrayList<String>();
        entityUuids.add("1234");
        bulkActionRequestInputDTO.setEntityUuids(entityUuids);

        // clear the security context
        SecurityContextHolder.getContext().setAuthentication(null);
        // set a test grpc context
        Context testContext = Context.current().withValue(SecurityConstant.USER_ID_CTX_KEY, "USER")
                .withValue(SecurityConstant.USER_UUID_KEY, "1")
                .withValue(SecurityConstant.USER_ROLES_KEY, ImmutableList.of("ADMIN", "TEST"));
        Context previous = testContext.attach();
        try {
            SuspensionToggle.SuspensionToggleEntityRequest toggleEntityRequest = testSuspensionMapper.createSuspensionToggleEntityRequest(bulkActionRequestInputDTO);
            assertEquals(SuspensionToggle.SuspensionToggleEntityRequest.Action.ACTION_STOP, toggleEntityRequest.getAction());
            assertEquals(1, toggleEntityRequest.getEntityOidsCount());
            assertEquals(Optional.ofNullable(Long.valueOf(1234)), Optional.ofNullable(toggleEntityRequest.getEntityOids(0)));
            assertEquals("1", toggleEntityRequest.getUserId());
            assertEquals("USER", toggleEntityRequest.getUserName());
        } catch (Exception e) {
            throw new SuspensionMapper.InvalidRequest("invalid entity uuids");
        }
        testContext.detach(previous);
    }

    /**
     * verifies the creation of suspension toggle action request from API to grpc toggle action request.
     * for entity oids empty scenerio
     */
    @Test
    public void testCreateSuspensionToggleEntityRequestEmpty() throws SuspensionMapper.InvalidRequest {
        BulkActionRequestInputDTO bulkActionRequestInputDTO = new BulkActionRequestInputDTO();
        bulkActionRequestInputDTO.setAction(SuspensionActionType.STOP);
        try {
            SuspensionToggle.SuspensionToggleEntityRequest toggleEntityRequest = testSuspensionMapper.createSuspensionToggleEntityRequest(bulkActionRequestInputDTO);
        } catch (Exception e) {
            assertEquals("entity uuids cannot be empty", e.getMessage());
        }
    }

    /**
     * verifies the creation of suspension toggle action request from API to grpc toggle action request.
     * for invalid entity uuids scenerio
     */
    @Test
    public void testCreateSuspensionToggleEntityRequestInvalid() throws SuspensionMapper.InvalidRequest {
        BulkActionRequestInputDTO bulkActionRequestInputDTO = new BulkActionRequestInputDTO();
        bulkActionRequestInputDTO.setAction(SuspensionActionType.STOP);
        List<String> entityUuids = new ArrayList<String>();
        entityUuids.add("0");
        bulkActionRequestInputDTO.setEntityUuids(entityUuids);
        try {
            SuspensionToggle.SuspensionToggleEntityRequest toggleEntityRequest = testSuspensionMapper.createSuspensionToggleEntityRequest(bulkActionRequestInputDTO);
        } catch (Exception e) {
            assertEquals("invalid entity uuids", e.getMessage());
        }
    }

    /**
     * verifies the conversion of suspension toggle action request from grpc to API toggle action request.
     */
    @Test
    public void testConvertToBulkActionRequestApiDTO() {
        List<SuspensionToggle.SuspensionToggleEntityData> toggleEntityDataList = new ArrayList<SuspensionToggle.SuspensionToggleEntityData>();
        SuspensionToggle.SuspensionToggleEntityData toggleEntityData = SuspensionToggle.SuspensionToggleEntityData.newBuilder()
                .setEntityOid(Long.valueOf(1234))
                .setState(SuspensionEntityOuterClass.SuspensionEntityState.SUSPENSION_ENTITY_STATE_RUNNING)
                .setError("test")
                .build();

        toggleEntityDataList.add(toggleEntityData);
        List<BulkActionRequestApiDTO> bulkActionRequestApiDTOs = testSuspensionMapper.convertToBulkActionRequestApiDTO(toggleEntityDataList);
        assertEquals(1, bulkActionRequestApiDTOs.size());
        assertEquals("1234", bulkActionRequestApiDTOs.get(0).getEntityUUID());
        assertEquals(SuspensionState.RUNNING, bulkActionRequestApiDTOs.get(0).getState());
        assertEquals("test", bulkActionRequestApiDTOs.get(0).getError());
    }

    /**
     * verifies the creation of SuspensionEntityRequest from criterialist of api input.
     */
    @Test
    public void testCreateSuspensionEntityRequestFromCriteriaList() {
        FilterApiDTO displayNameFilter = new FilterApiDTO();
        displayNameFilter.setFilterType("displayName");
        displayNameFilter.setExpVal("test");
        displayNameFilter.setExpType("EQ");
        List<FilterApiDTO> filters = new ArrayList<FilterApiDTO>();
        filters.add(displayNameFilter);
        FilterApiDTO costFilter = new FilterApiDTO();
        costFilter.setFilterType("cost");
        costFilter.setExpVal("10.56");
        costFilter.setExpType("EQ");
        filters.add(costFilter);
        SuspendableEntityInputDTO suspendableEntityInputDTO = new SuspendableEntityInputDTO();
        suspendableEntityInputDTO.setCriteriaList(filters);
        Builder requestBuilder = SuspensionEntityRequest.getDefaultInstance().newBuilder();
        requestBuilder.addDisplayName(SuspensionFilter.StringFilter.newBuilder().setExpType(SuspensionFilter.ExpType.EXP_TYPE_EQ).addValues("test").build());
        requestBuilder.addCost(SuspensionFilter.DoubleFilter.newBuilder().setExpType(SuspensionFilter.ExpType.EXP_TYPE_EQ).addValues(10.56).build());
        assertEquals(requestBuilder.build(), testSuspensionMapper.createSuspensionEntityRequestFromCriteriaList(suspendableEntityInputDTO));
    }

    /**
     * verifies the invalid filter type error while creating SuspensionEntityRequest from criterialist of api input.
     */
    @Test
    public void testCreateSuspensionEntityRequestFromCriteriaListInvalidFilter() {
        FilterApiDTO testFilter = new FilterApiDTO();
        testFilter.setFilterType("test");
        testFilter.setExpVal("test");
        testFilter.setExpType("EQ");
        List<FilterApiDTO> filters = new ArrayList<FilterApiDTO>();
        filters.add(testFilter);
        SuspendableEntityInputDTO suspendableEntityInputDTO = new SuspendableEntityInputDTO();
        suspendableEntityInputDTO.setCriteriaList(filters);
        try {
            testSuspensionMapper.createSuspensionEntityRequestFromCriteriaList(suspendableEntityInputDTO);
            TestCase.fail("IllegalArgumentException should have been thrown");
        } catch (Exception e) {
            assertEquals("Invalid filter type test", e.getMessage());
        }
    }

    /**
     * verifies the invalid expression type error for string while creating SuspensionEntityRequest from criterialist of api input.
     */
    @Test
    public void testCreateSuspensionEntityRequestFromCriteriaListInvalidExpTypeString() {
        FilterApiDTO testFilter = new FilterApiDTO();
        testFilter.setFilterType("displayName");
        testFilter.setExpVal("test");
        testFilter.setExpType("TEST");
        List<FilterApiDTO> filters = new ArrayList<FilterApiDTO>();
        filters.add(testFilter);
        SuspendableEntityInputDTO suspendableEntityInputDTO = new SuspendableEntityInputDTO();
        suspendableEntityInputDTO.setCriteriaList(filters);
        try {
            testSuspensionMapper.createSuspensionEntityRequestFromCriteriaList(suspendableEntityInputDTO);
            TestCase.fail("IllegalArgumentException should have been thrown");
        } catch (IllegalArgumentException e) {
            assertEquals("Invalid expression type for string filter", e.getMessage());
        }
    }


    /**
     * verifies the invalid expression type double for string while creating SuspensionEntityRequest from criterialist of api input.
     */
    @Test
    public void testCreateSuspensionEntityRequestFromCriteriaListInvalidExpTypeDouble() {
        FilterApiDTO testFilter = new FilterApiDTO();
        testFilter.setFilterType("cost");
        testFilter.setExpVal("test");
        testFilter.setExpType("TEST");
        List<FilterApiDTO> filters = new ArrayList<FilterApiDTO>();
        filters.add(testFilter);
        SuspendableEntityInputDTO suspendableEntityInputDTO = new SuspendableEntityInputDTO();
        suspendableEntityInputDTO.setCriteriaList(filters);
        try {
            testSuspensionMapper.createSuspensionEntityRequestFromCriteriaList(suspendableEntityInputDTO);
            TestCase.fail("IllegalArgumentException should have been thrown");
        } catch (IllegalArgumentException e) {
            assertEquals("Invalid expression type for double filter", e.getMessage());
        }
    }

    /**
     * verifies the conversion of API time span state to gRPC time span state.
     */
    @Test
    public void testGetGrpcTsState() {
        assertEquals(TimespanState.TS_ON, testSuspensionMapper.getGrpcTsState(SuspensionTimeSpanState.ON));
        assertEquals(TimespanState.TS_OFF, testSuspensionMapper.getGrpcTsState(SuspensionTimeSpanState.OFF));
        assertEquals(TimespanState.TS_IGNORE, testSuspensionMapper.getGrpcTsState(SuspensionTimeSpanState.IGNORE));
    }

    /**
     * verifies the conversion of gRPC time span state to API time span state.
     */
    @Test
    public void testGetApiTsState() {
        assertEquals(SuspensionTimeSpanState.ON, testSuspensionMapper.getApiTsState(TimespanState.TS_ON));
        assertEquals(SuspensionTimeSpanState.OFF, testSuspensionMapper.getApiTsState(TimespanState.TS_OFF));
        assertEquals(SuspensionTimeSpanState.IGNORE, testSuspensionMapper.getApiTsState(TimespanState.TS_IGNORE));
    }

    /**
     * verifies the conversion of API time spans based schedule creation to gRPC request.
     */
    @Test
    public void testToCreateTimespanScheduleRequest() {
        SuspendItemApiDTO policy = new SuspendItemApiDTO();
        policy.setState(SuspensionTimeSpanState.ON);
        TimeSpanApiDTO timeSpanApiDTO = new TimeSpanApiDTO();
        timeSpanApiDTO.setPolicy(policy);
        timeSpanApiDTO.setBegins("00:15");
        timeSpanApiDTO.setEnds("01:00");
        List<TimeSpanApiDTO> timespans = new ArrayList<>();
        timespans.add(timeSpanApiDTO);
        WeekDayTimeSpansApiDTO weekDayTimeSpansApiDTO = new WeekDayTimeSpansApiDTO();
        weekDayTimeSpansApiDTO.setSaturday(timespans);
        String uuid = "random_uuid";
        String name = "dummy_name";
        String description = "some_random_description";
        String timeZone = "IST";
        ScheduleTimeSpansApiDTO scheduleTimeSpansApiDTO = new ScheduleTimeSpansApiDTO(uuid, name,
                description, timeZone, weekDayTimeSpansApiDTO);

        CreateTimespanScheduleRequest.Builder want = CreateTimespanScheduleRequest.getDefaultInstance().newBuilder();
        want.setDescription(description);
        want.setName(name);
        want.setDescription(description);
        want.setTimezone(timeZone);
        TimeOfDay.Builder beginsTod = TimeOfDay.getDefaultInstance().newBuilder();
        beginsTod.setHours(0);
        beginsTod.setMinutes(15);
        TimeOfDay.Builder endTod = TimeOfDay.getDefaultInstance().newBuilder();
        endTod.setHours(1);
        endTod.setMinutes(0);

        Timespan.Builder timespan = Timespan.getDefaultInstance().newBuilder();
        timespan.setBegins(beginsTod.build());
        timespan.setEnds(endTod.build());
        timespan.setState(TimespanState.TS_ON);
        List<Timespan> tsList = new ArrayList<>();
        tsList.add(timespan.build());

        WeekDayTimespans.Builder grpcTimespans = WeekDayTimespans.getDefaultInstance().newBuilder();
        grpcTimespans.addAllSaturday(tsList);
        want.setTimespans(grpcTimespans.build());

        try {
            CreateTimespanScheduleRequest got = testSuspensionMapper.toCreateTimespanScheduleRequest(scheduleTimeSpansApiDTO);
            assertEquals(want.build(), got);
        } catch (Exception e) {
            TestCase.fail();
        }
    }

    /**
     * verifies the conversion of gRPC time span based schedule creation to API response format.
     */
    @Test
    public void testToApiScheduleTimeSpansApiDTO() {
        SuspendItemApiDTO policy = new SuspendItemApiDTO();
        policy.setState(SuspensionTimeSpanState.ON);
        TimeSpanApiDTO timeSpanApiDTO = new TimeSpanApiDTO();
        timeSpanApiDTO.setPolicy(policy);
        timeSpanApiDTO.setBegins("00:15");
        timeSpanApiDTO.setEnds("01:00");
        List<TimeSpanApiDTO> timespans = new ArrayList<>();
        timespans.add(timeSpanApiDTO);
        WeekDayTimeSpansApiDTO weekDayTimeSpansApiDTO = new WeekDayTimeSpansApiDTO();
        weekDayTimeSpansApiDTO.setSaturday(timespans);
        String uuid = "1234";
        String name = "dummy_name";
        String description = "some_random_description";
        String timeZone = "IST";
        ScheduleTimeSpansApiDTO want = new ScheduleTimeSpansApiDTO(uuid, name,
                description, timeZone, weekDayTimeSpansApiDTO);

        TimespanSchedule.Builder input = TimespanSchedule.getDefaultInstance().newBuilder();
        input.setDescription(description);
        input.setOid(1234);
        input.setName(name);
        input.setDescription(description);
        input.setTimezone(timeZone);
        TimeOfDay.Builder beginsTod = TimeOfDay.getDefaultInstance().newBuilder();
        beginsTod.setHours(0);
        beginsTod.setMinutes(15);
        TimeOfDay.Builder endTod = TimeOfDay.getDefaultInstance().newBuilder();
        endTod.setHours(1);
        endTod.setMinutes(0);

        Timespan.Builder timespan = Timespan.getDefaultInstance().newBuilder();
        timespan.setBegins(beginsTod.build());
        timespan.setEnds(endTod.build());
        timespan.setState(TimespanState.TS_ON);
        List<Timespan> tsList = new ArrayList<>();
        tsList.add(timespan.build());

        WeekDayTimespans.Builder grpcTimespans = WeekDayTimespans.getDefaultInstance().newBuilder();
        grpcTimespans.addAllSaturday(tsList);
        input.setTimespans(grpcTimespans.build());

        try {
            ScheduleTimeSpansApiDTO got = testSuspensionMapper.toApiScheduleTimeSpansApiDTO(input.build());
            ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
            String wantJSON = ow.writeValueAsString(want);
            String gotJSON = ow.writeValueAsString(got);
            assertEquals(wantJSON, gotJSON);
        } catch (Exception e) {
            TestCase.fail();
        }
    }

    /**
     * verifies the conversion of API params based schedule delete to gRPC request.
     */
    @Test
    public void testToApiTimespanScheduleByIDRequest() {
        String timeSpanSchedule_Uuid = "22";

        DeleteTimespanScheduleRequest.Builder want = DeleteTimespanScheduleRequest.getDefaultInstance().newBuilder();
        want.setScheduleOid(Long.parseLong(timeSpanSchedule_Uuid));

        try {
            DeleteTimespanScheduleRequest got = testSuspensionMapper.toApiDeleteTimespanScheduleRequest(timeSpanSchedule_Uuid);
            assertEquals(want.build(), got);
        } catch (Exception e) {
            TestCase.fail();
        }
    }

    /**
     * verifies the conversion of API time spans based schedule update to gRPC request.
     */
    @Test
    public void testToUpdateTimespanScheduleRequest() {
        SuspendItemApiDTO policy = new SuspendItemApiDTO();
        policy.setState(SuspensionTimeSpanState.ON);
        TimeSpanApiDTO timeSpanApiDTO = new TimeSpanApiDTO();
        timeSpanApiDTO.setPolicy(policy);
        timeSpanApiDTO.setBegins("00:15");
        timeSpanApiDTO.setEnds("01:00");
        List<TimeSpanApiDTO> timespans = new ArrayList<>();
        timespans.add(timeSpanApiDTO);
        WeekDayTimeSpansApiDTO weekDayTimeSpansApiDTO = new WeekDayTimeSpansApiDTO();
        weekDayTimeSpansApiDTO.setSaturday(timespans);
        String uuid = "1234";
        String name = "dummy_name";
        String description = "some_random_description";
        String timeZone = "IST";
        ScheduleTimeSpansApiDTO scheduleTimeSpansApiDTO = new ScheduleTimeSpansApiDTO(uuid, name,
                description, timeZone, weekDayTimeSpansApiDTO);

        UpdateTimespanScheduleRequest.Builder want = UpdateTimespanScheduleRequest.getDefaultInstance().newBuilder();
        want.setOid(1234);
        want.setDescription(description);
        want.setName(name);
        want.setDescription(description);
        want.setTimezone(timeZone);
        TimeOfDay.Builder beginsTod = TimeOfDay.getDefaultInstance().newBuilder();
        beginsTod.setHours(0);
        beginsTod.setMinutes(15);
        TimeOfDay.Builder endTod = TimeOfDay.getDefaultInstance().newBuilder();
        endTod.setHours(1);
        endTod.setMinutes(0);

        Timespan.Builder timespan = Timespan.getDefaultInstance().newBuilder();
        timespan.setBegins(beginsTod.build());
        timespan.setEnds(endTod.build());
        timespan.setState(TimespanState.TS_ON);
        List<Timespan> tsList = new ArrayList<>();
        tsList.add(timespan.build());

        WeekDayTimespans.Builder grpcTimespans = WeekDayTimespans.getDefaultInstance().newBuilder();
        grpcTimespans.addAllSaturday(tsList);
        want.setTimespans(grpcTimespans.build());

        try {
            UpdateTimespanScheduleRequest got = testSuspensionMapper.toUpdateTimespanScheduleRequest(uuid, scheduleTimeSpansApiDTO);
            assertEquals(want.build(), got);
        } catch (Exception e) {
            TestCase.fail();
        }
    }

    /**
     * verifies the conversion of column to sort by from api to grpc.
     */
    public void testGetScheduleOrderBy() {
        assertEquals(TimespanScheduleOrderBy.TIMESPAN_SCHEDULE_ORDER_BY_DISPLAY_NAME, testSuspensionMapper.getScheduleOrderBy(ScheduleTimeSpansOrderBy.DISPLAY_NAME));
        assertEquals(TimespanScheduleOrderBy.TIMESPAN_SCHEDULE_ORDER_BY_DISPLAY_NAME, testSuspensionMapper.getScheduleOrderBy(null));
    }

    /**
     * verifies the conversion of API list time span schedules request to gRPC ListTimespanScheduleRequest.
     */
    public void testToListTimespanScheduleRequest() {
        ListTimespanScheduleRequest.Builder requestBuilder = ListTimespanScheduleRequest.getDefaultInstance().newBuilder();
        requestBuilder.setOrderBy(TimespanScheduleOrderBy.TIMESPAN_SCHEDULE_ORDER_BY_DISPLAY_NAME);
        requestBuilder.setLimit(300);
        requestBuilder.setDescending(false);
        try {
            ScheduleTimeSpansPaginationRequest paginationRequest = new ScheduleTimeSpansPaginationRequest(null, 300, true, ScheduleTimeSpansOrderBy.DISPLAY_NAME);
            assertEquals(requestBuilder.build(), testSuspensionMapper.toListTimespanScheduleRequest( paginationRequest));
        } catch (Exception e) {
            TestCase.fail();
        }
    }

    /**
     * verifies the conversion of API params based schedule get to gRPC request.
     */
    @Test
    public void testToGetTimespanScheduleRequest() {
        String timeSpanSchedule_Uuid = "22";

        TimespanScheduleByIDRequest.Builder want = TimespanScheduleByIDRequest.getDefaultInstance().newBuilder();
        want.setScheduleOid(Long.parseLong(timeSpanSchedule_Uuid));

        try {
            TimespanScheduleByIDRequest got = testSuspensionMapper.toGetTimespanScheduleRequest(timeSpanSchedule_Uuid);
            assertEquals(want.build(), got);
        } catch (Exception e) {
            TestCase.fail();
        }
    }

    /**
     * verifies the conversion of path params and body of attach entity to schedule api to grpc request for attach time span based schedule.
     */
    @Test
    public void testToSuspensionAttachEntitiesRequest() {
        String scheduleUuid = "1234";
        String entityUuid = "5678";
        SuspensionAttachEntitiesRequest.Builder want = SuspensionAttachEntitiesRequest.getDefaultInstance().newBuilder();
        want.setScheduleOid(Long.parseLong(scheduleUuid));
        want.addEntityOids(Long.parseLong(entityUuid));

        SuspendableEntityUUIDSetDTO suspendableEntityUUIDSetDTO = new SuspendableEntityUUIDSetDTO();
        suspendableEntityUUIDSetDTO.setEntityUuids(Arrays.asList(entityUuid));

        try {
            SuspensionAttachEntitiesRequest got = testSuspensionMapper.toSuspensionAttachEntitiesRequest(scheduleUuid, suspendableEntityUUIDSetDTO);
            assertEquals(want.build(), got);
        } catch (Exception e) {
            TestCase.fail();
        }
    }

    /**
     * verifies the conversion of path params and body of attach schedule to entity api to grpc request for attach time span based schedule.
     * With entity empty error
     */
    @Test
    public void testToSuspensionAttachEntitiesRequestEntityError() throws Exception {
        String scheduleUuid = "5678";
        SuspensionAttachEntitiesRequest.Builder want = SuspensionAttachEntitiesRequest.getDefaultInstance().newBuilder();
        want.setScheduleOid(Long.parseLong(scheduleUuid));
        SuspendableEntityUUIDSetDTO suspendableEntityUUIDSetDTO = new SuspendableEntityUUIDSetDTO();

        try {
            SuspensionAttachEntitiesRequest got = testSuspensionMapper.toSuspensionAttachEntitiesRequest(scheduleUuid, suspendableEntityUUIDSetDTO);
            TestCase.fail("IllegalArgumentException should have been thrown");
        } catch (SuspensionMapper.InvalidRequest e) {
            assertEquals("entity uuids list cannot be empty", e.getMessage());
        }
    }

    /**
     * verifies the response of attach schedule grpc method to api response.
     */
    @Test
    public void testToScheduleEntityResponseApiDTOForAttach() {
        List<ScheduleEntityResponseApiDTO> scheduleEntityList = new ArrayList<ScheduleEntityResponseApiDTO>();
        ScheduleEntityResponseApiDTO scheduleEntity = new ScheduleEntityResponseApiDTO();
        scheduleEntity.setEntityUUID("1234");
        scheduleEntity.setError("test");

        scheduleEntityList.add(scheduleEntity);
        SuspensionAttachEntitiesResponse.Builder input = SuspensionAttachEntitiesResponse.getDefaultInstance().newBuilder();
        SuspensionScheduleEntityError data = SuspensionScheduleEntityError.getDefaultInstance().newBuilder().setEntityOid(1234).setError("test").build();
        input.addError(data);
        List<ScheduleEntityResponseApiDTO> apiDtos = testSuspensionMapper.toScheduleEntityResponseApiDTOForAttach(input.build());
        assertEquals(1, apiDtos.size());
        assertEquals("1234", apiDtos.get(0).getEntityUUID());
        assertEquals("test", apiDtos.get(0).getError());

    }

    /**
     * verifies the conversion of path params and body of put schedule entity api to grpc request for update time span based schedule.
     */
    @Test
    public void testToSuspensionUpdateEntitiesRequest() {
        String scheduleUuid = "1234";
        String entityUuid = "5678";
        SuspensionUpdateEntitiesRequest.Builder want = SuspensionUpdateEntitiesRequest.getDefaultInstance().newBuilder();
        want.setScheduleOid(Long.parseLong(scheduleUuid));
        want.addEntityOids(Long.parseLong(entityUuid));

        SuspendableEntityUUIDSetDTO suspendableEntityUUIDSetDTO = new SuspendableEntityUUIDSetDTO();
        suspendableEntityUUIDSetDTO.setEntityUuids(Arrays.asList(entityUuid));

        try {
            SuspensionUpdateEntitiesRequest got = testSuspensionMapper.toSuspensionUpdateEntitiesRequest(scheduleUuid, suspendableEntityUUIDSetDTO);
            assertEquals(want.build(), got);
        } catch (Exception e) {
            TestCase.fail();
        }
    }

    /**
     * verifies the conversion of path params and body of update schedule entity api to grpc request for update time span based schedule.
     * With entity empty error
     */
    @Test
    public void testToSuspensionUpdateEntitiesRequestEntityError() throws Exception {
        String scheduleUuid = "5678";
        SuspensionUpdateEntitiesRequest.Builder want = SuspensionUpdateEntitiesRequest.getDefaultInstance().newBuilder();
        want.setScheduleOid(Long.parseLong(scheduleUuid));
        SuspendableEntityUUIDSetDTO suspendableEntityUUIDSetDTO = new SuspendableEntityUUIDSetDTO();

        try {
            SuspensionUpdateEntitiesRequest got = testSuspensionMapper.toSuspensionUpdateEntitiesRequest(scheduleUuid, suspendableEntityUUIDSetDTO);
            TestCase.fail("IllegalArgumentException should have been thrown");
        } catch (SuspensionMapper.InvalidRequest e) {
            assertEquals("entity uuids list cannot be empty", e.getMessage());
        }
    }

    /**
     * verifies the response of update time span based schedule attached to entities grpc method to api response.
     */
    @Test
    public void testToScheduleEntityResponseApiDTOForUpdate() {
        List<ScheduleEntityResponseApiDTO> scheduleEntityList = new ArrayList<ScheduleEntityResponseApiDTO>();
        ScheduleEntityResponseApiDTO scheduleEntity = new ScheduleEntityResponseApiDTO();
        scheduleEntity.setEntityUUID("1234");
        scheduleEntity.setError("test");

        scheduleEntityList.add(scheduleEntity);
        SuspensionUpdateEntitiesResponse.Builder input = SuspensionUpdateEntitiesResponse.getDefaultInstance().newBuilder();
        SuspensionScheduleEntityError data = SuspensionScheduleEntityError.getDefaultInstance().newBuilder().setEntityOid(1234).setError("test").build();
        input.addError(data);
        List<ScheduleEntityResponseApiDTO> apiDtos = testSuspensionMapper.toScheduleEntityResponseApiDTOForUpdate(input.build());
        assertEquals(1, apiDtos.size());
        assertEquals("1234", apiDtos.get(0).getEntityUUID());
        assertEquals("test", apiDtos.get(0).getError());

    }

    /**
     * verifies the conversion of path params and body of detach entity to schedule api to grpc request for detach time span based schedule.
     */
    @Test
    public void testToSuspensionDetachEntitiesRequest() {
        String scheduleUuid = "1234";
        String entityUuid = "5678";
        SuspensionDetachEntitiesRequest.Builder want = SuspensionDetachEntitiesRequest.getDefaultInstance().newBuilder();
        want.setScheduleOid(Long.parseLong(scheduleUuid));
        want.addEntityOids(Long.parseLong(entityUuid));

        SuspendableEntityUUIDSetDTO suspendableEntityUUIDSetDTO = new SuspendableEntityUUIDSetDTO();
        suspendableEntityUUIDSetDTO.setEntityUuids(Arrays.asList(entityUuid));

        try {
            SuspensionDetachEntitiesRequest got = testSuspensionMapper.toSuspensionDetachEntitiesRequest(scheduleUuid, suspendableEntityUUIDSetDTO);
            assertEquals(want.build(), got);
        } catch (Exception e) {
            TestCase.fail();
        }
    }

    /**
     * verifies the conversion of path params and body of detach schedule to entity api to grpc request for detach time span based schedule.
     */
    @Test
    public void testToSuspensionDetachEntitiesRequestWithNoEntities() throws Exception {
        String scheduleUuid = "5678";
        SuspensionDetachEntitiesRequest.Builder want = SuspensionDetachEntitiesRequest.getDefaultInstance().newBuilder();
        want.setScheduleOid(Long.parseLong(scheduleUuid));
        SuspendableEntityUUIDSetDTO suspendableEntityUUIDSetDTO = new SuspendableEntityUUIDSetDTO();

        try {
            SuspensionDetachEntitiesRequest got = testSuspensionMapper.toSuspensionDetachEntitiesRequest(scheduleUuid, suspendableEntityUUIDSetDTO);
            assertEquals(want.build(), got);
        } catch (Exception e) {
            TestCase.fail();
        }
    }

    /**
     * verifies the response of detach schedule grpc method to api response.
     */
    @Test
    public void testToScheduleEntityResponseApiDTOForDetach() {
        List<ScheduleEntityResponseApiDTO> scheduleEntityList = new ArrayList<ScheduleEntityResponseApiDTO>();
        ScheduleEntityResponseApiDTO scheduleEntity = new ScheduleEntityResponseApiDTO();
        scheduleEntity.setEntityUUID("1234");
        scheduleEntity.setError("test");

        scheduleEntityList.add(scheduleEntity);
        SuspensionDetachEntitiesResponse.Builder input = SuspensionDetachEntitiesResponse.getDefaultInstance().newBuilder();
        SuspensionScheduleEntityError data = SuspensionScheduleEntityError.getDefaultInstance().newBuilder().setEntityOid(1234).setError("test").build();
        input.addError(data);
        List<ScheduleEntityResponseApiDTO> apiDtos = testSuspensionMapper.toScheduleEntityResponseApiDTOForDetach(input.build());
        assertEquals(1, apiDtos.size());
        assertEquals("1234", apiDtos.get(0).getEntityUUID());
        assertEquals("test", apiDtos.get(0).getError());
    }

    /**
     * verifies the conversion of API params based SuspensionGetEntitiesRequest to gRPC request.
     */
    @Test
    public void testToGetEntitiesRequest() {
        String timeSpanSchedule_Uuid = "22";

        SuspensionGetEntitiesRequest.Builder want = SuspensionGetEntitiesRequest.getDefaultInstance().newBuilder();
        want.setScheduleOid(Long.parseLong(timeSpanSchedule_Uuid));

        try {
            SuspensionGetEntitiesRequest got = testSuspensionMapper.toGetEntitiesRequest(timeSpanSchedule_Uuid);
            assertEquals(want.build(), got);
        } catch (Exception e) {
            TestCase.fail();
        }
    }


    /**
     * verifies the conversion of API params based SuspensionGetEntitiesRequest to gRPC request.
     */
    @Test
    public void testToSuspendableEntityUUIDSetDTO() {

        long l = 22;
        java.util.List<java.lang.Long> list = new ArrayList<java.lang.Long>(1);
        list.add(l);

        SuspensionGetEntitiesResponse.Builder input = SuspensionGetEntitiesResponse.getDefaultInstance().newBuilder();
        input.addEntityOids(l);

        List<String> entityUuidsArr = new ArrayList<String>(1);
        entityUuidsArr.add("22");

        SuspendableEntityUUIDSetDTO want = new SuspendableEntityUUIDSetDTO();
        want.setEntityUuids(entityUuidsArr);

        try {
            SuspendableEntityUUIDSetDTO got = testSuspensionMapper.toSuspendableEntityUUIDSetDTO(input.build());
            ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
            String wantJSON = ow.writeValueAsString(want);
            String gotJSON = ow.writeValueAsString(got);
            assertEquals(wantJSON, gotJSON);
        } catch (Exception e) {
            TestCase.fail();
        }
    }
}
