package com.vmturbo.api.component.external.api.mapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.google.common.collect.ImmutableList;

import io.grpc.Context;

import junit.framework.TestCase;

import org.junit.Test;
import org.springframework.security.core.context.SecurityContextHolder;

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
import com.vmturbo.auth.api.authorization.jwt.SecurityConstant;
import com.vmturbo.common.protobuf.suspension.SuspensionEntityOuterClass;
import com.vmturbo.common.protobuf.suspension.SuspensionEntityOuterClass.SuspensionEntity;
import com.vmturbo.common.protobuf.suspension.SuspensionEntityOuterClass.SuspensionEntityRequest;
import com.vmturbo.common.protobuf.suspension.SuspensionEntityOuterClass.SuspensionEntityRequest.Builder;
import com.vmturbo.common.protobuf.suspension.SuspensionEntityOuterClass.SuspensionEntityTags;
import com.vmturbo.common.protobuf.suspension.SuspensionFilter;
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
}
