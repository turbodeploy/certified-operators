package com.vmturbo.api.component.external.api.service;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.vmturbo.api.component.external.api.mapper.PolicyMapper;
import com.vmturbo.api.dto.policy.PolicyApiDTO;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.common.protobuf.group.PolicyDTO.Policy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo.MergePolicy;
import com.vmturbo.common.protobuf.group.PolicyDTOMoles.PolicyServiceMole;
import com.vmturbo.common.protobuf.group.PolicyServiceGrpc;
import com.vmturbo.components.api.test.GrpcTestServer;

/**
 * Unit test for {@link PoliciesService}.
 */
public class PoliciesServiceTest {

    /**
     * JUnit rule to help represent expected exceptions in tests.
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private PoliciesService policiesService;

    /**
     * Object used to mock a PolicyMapper.
     */
    @Mock
    private PolicyMapper policyMapper;

    private PolicyServiceMole policyServiceSpy = spy(new PolicyServiceMole());

    private GroupServiceMole groupServiceSpy = spy(new GroupServiceMole());

    /**
     * Test gRPC server to mock out gRPC dependencies.
     */
    @Rule
    public GrpcTestServer grpcServer =
            GrpcTestServer.newServer(policyServiceSpy, groupServiceSpy);

    private static final long CONTEXT_ID = 7777777;

    /**
     * Startup method to run before every test.
     */
    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);

        policiesService =
                new PoliciesService(
                        PolicyServiceGrpc.newBlockingStub(grpcServer.getChannel()),
                        GroupServiceGrpc.newBlockingStub(grpcServer.getChannel()),
                        policyMapper);
    }

    /**
     * Test for {@link PoliciesService#getPolicyByUuid(String)}.
     * When there is no Policy with the uuid provided.
     *
     * @throws Exception on exception occurred
     */
    @Test
    public void testGetPolicyByNonexistingUuid() throws Exception {
        Long testUuid = 1234L;
        final PolicyDTO.SinglePolicyRequest request = PolicyDTO.SinglePolicyRequest.newBuilder()
                .setPolicyId(testUuid)
                .build();
        // Return empty response, which will trigger the exception
        PolicyDTO.PolicyResponse response = PolicyDTO.PolicyResponse.newBuilder().build();
        when(policyServiceSpy.getPolicy(eq(request))).thenReturn(response);
        expectedException.expect(UnknownObjectException.class);
        policiesService.getPolicyByUuid(testUuid.toString());
    }

    /**
     * Test for {@link PoliciesService#getPolicyByUuid(String)}.
     * When there is a Policy with the uuid provided.
     *
     * @throws Exception on exception occurred
     */
    @Test
    public void testGetPolicyByUuid() throws Exception {
        Long testUuidLong = 123456789L;
        final PolicyDTO.SinglePolicyRequest request = PolicyDTO.SinglePolicyRequest.newBuilder()
                .setPolicyId(testUuidLong)
                .build();
        final PolicyApiDTO mapperResponse = new PolicyApiDTO();
        mapperResponse.setUuid(testUuidLong.toString());
        PolicyDTO.PolicyResponse response = PolicyDTO.PolicyResponse.newBuilder()
                .setPolicy(Policy.newBuilder()
                        .setId(testUuidLong)
                        .setPolicyInfo(PolicyInfo.getDefaultInstance())
                        .build())
                .build();
        when(policyMapper.policyToApiDto(eq(Collections.singletonList(response.getPolicy())),
                any())).thenReturn(Collections.singletonList(mapperResponse));
        when(policyServiceSpy.getPolicy(eq(request))).thenReturn(response);
        PolicyApiDTO policyApiDTO = policiesService.getPolicyByUuid(testUuidLong.toString());
        Assert.assertEquals(testUuidLong, Long.valueOf(policyApiDTO.getUuid()));
    }

    /**
     * Simple test for getPolicies(). Tests basic functionality.
     *
     * @throws Exception on error
     */
    @Test
    public void testGetPolicies() throws Exception {
        // GIVEN
        Long policyId = 10L;
        Set<Long> groupIds = new HashSet<>();
        groupIds.add(1L);
        groupIds.add(2L);
        groupIds.add(3L);
        Policy policy = Policy.newBuilder()
                .setId(policyId)
                .setPolicyInfo(PolicyInfo.newBuilder()
                        .setMerge(MergePolicy.newBuilder()
                                .addAllMergeGroupIds(groupIds)
                                .build())
                        .build())
                .build();
        PolicyDTO.PolicyResponse policyResponse = PolicyDTO.PolicyResponse.newBuilder()
                .setPolicy(policy)
                .build();
        when(policyServiceSpy.getPolicies(PolicyDTO.PolicyRequest.getDefaultInstance()))
                .thenReturn(Collections.singletonList(policyResponse));

        List<Grouping> groupings = new ArrayList<>();
        groupings.add(Grouping.newBuilder().setId(1L).build());
        groupings.add(Grouping.newBuilder().setId(2L).build());
        groupings.add(Grouping.newBuilder().setId(3L).build());
        when(groupServiceSpy.getGroups(
                GetGroupsRequest.newBuilder()
                        .setGroupFilter(GroupFilter.newBuilder()
                                .addAllId(groupIds)
                                .setIncludeHidden(true))
                        .build()))
                .thenReturn(groupings);

        PolicyApiDTO policyApiDTO = new PolicyApiDTO();
        policyApiDTO.setUuid(String.valueOf(policyId));
        List<PolicyApiDTO> policyMapperResponse = new ArrayList<>();
        policyMapperResponse.add(policyApiDTO);
        when(policyMapper.policyToApiDto(Collections.singletonList(policy),
                        Sets.newHashSet(groupings)))
                .thenReturn(policyMapperResponse);

        // WHEN
        List<PolicyApiDTO> response = policiesService.getPolicies();

        // THEN
        Assert.assertEquals(1, response.size());
        Assert.assertEquals(String.valueOf(policyId), response.get(0).getUuid());
    }
}