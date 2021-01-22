package com.vmturbo.api.component.external.api.util;

import static org.hamcrest.beans.SamePropertyValuesAs.samePropertyValuesAs;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;

import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;

import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.auth.api.authorization.jwt.JwtCallCredential;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO.PROVIDER;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.OptimizationMetadata;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers.StaticMembersByType;
import com.vmturbo.common.protobuf.topology.ApiEntityType;

/**
 * Test {@link ApiUtils#generateJWTCallCredential()} and {@link ApiUtils#getClientIp(HttpServletRequest)}
 *
 */
public class ApiUtilsTest {
    private static final String TEST_TOKEN = "test token";
    private static final String _10_0_0_200 = "10.0.0.200";
    private static final String _10_0_0_1 = "10.0.0.1";
    private static final String X_FORWARDED_FOR = "X-FORWARDED-FOR";
    private final JwtCallCredential jwtCallCredential = new JwtCallCredential(TEST_TOKEN);
    private GroupExpander groupExpander = Mockito.mock(GroupExpander.class);

    @Test
    public void testGenerateJWTCallCredential() throws Exception {
        // setup authentication object
        Set<GrantedAuthority> grantedAuths = new HashSet<>();
        grantedAuths.add(new SimpleGrantedAuthority("ROLE_NONADMINISTRATOR"));
        AuthUserDTO user = new AuthUserDTO(PROVIDER.LOCAL, "admin", null, null,
                "testUUID", TEST_TOKEN, new ArrayList<>(), null);
        Authentication authentication = new UsernamePasswordAuthenticationToken(user, "***", grantedAuths);

        // populate security context
        SecurityContextHolder.getContext().setAuthentication(authentication);

        // verify the JWTCallCredential is returned
        assertTrue(ApiUtils.generateJWTCallCredential().isPresent());
        JwtCallCredential returnJwtCallCredential = ApiUtils.generateJWTCallCredential().get();

        // verify the JWTCallCredential has the same JWT token as before
        assertThat(returnJwtCallCredential, samePropertyValuesAs(jwtCallCredential));

        // clear the security context
        SecurityContextHolder.getContext().setAuthentication(null);
    }

    @Test
    public void testGetClientIpWithRemoteAddr() {
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getHeader(X_FORWARDED_FOR)).thenReturn("");
        when(request.getRemoteAddr()).thenReturn(_10_0_0_1);
        Optional<String> ipAddress = ApiUtils.getClientIp(request);
        assertEquals("IP address should be available", _10_0_0_1, ipAddress.get());
    }

    @Test
    public void testGetClientIpWithForwardHeader() {
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getHeader(X_FORWARDED_FOR)).thenReturn(_10_0_0_1);
        when(request.getRemoteAddr()).thenReturn(_10_0_0_200);
        Optional<String> ipAddress = ApiUtils.getClientIp(request);
        assertEquals("IP address should be available", _10_0_0_1, ipAddress.get());
    }

    public static ActionEntity createActionEntity(long id) {
        // set some fake type for now
        final int defaultEntityType = 1;
        return ActionEntity.newBuilder()
                    .setId(id)
                    .setType(defaultEntityType)
                    .build();
    }

    public static ActionEntity createActionEntity(long id, int type) {
        return ActionEntity.newBuilder()
                    .setId(id)
                    .setType(type)
                    .build();
    }

    /**
     * Test globalTempGroup of environmentType Hyrid returns non empty optional of relatedType.
     */
    @Test
    public void testGetGlobalTempGroupEntityTypeWithTempGlobalHybridGroup() {
        //GIVEN
        when(groupExpander.getGroup(eq("1234"))).thenReturn(
                Optional.of(Grouping.newBuilder()
                        .setDefinition(GroupDefinition.newBuilder().setIsTemporary(true)
                                .setOptimizationMetadata(OptimizationMetadata.newBuilder()
                                        .setIsGlobalScope(true)
                                        .setEnvironmentType(EnvironmentType.HYBRID))
                                .setStaticGroupMembers(StaticMembers.newBuilder()
                                        .addMembersByType(StaticMembersByType
                                                .newBuilder()
                                                .setType(MemberType
                                                        .newBuilder()
                                                        .setEntity(ApiEntityType.PHYSICAL_MACHINE
                                                                .typeNumber())))))
                        .build()
                ));
        //THEN
        assertTrue(ApiUtils.getGlobalTempGroupEntityType(
                Collections.singleton("1234"), groupExpander).isPresent());
    }

    /**
     * Test globalTempGroup of environmentType non Hybrid returns empty optional.
     */
    @Test
    public void testGetGlobalTempGroupEntityTypeWithTempGlobalGroupWithNonHybridGroup() {
        //GIVEN
        when(groupExpander.getGroup(eq("1234"))).thenReturn(
                Optional.of(Grouping.newBuilder()
                        .setDefinition(GroupDefinition.newBuilder().setIsTemporary(true)
                                .setOptimizationMetadata(OptimizationMetadata.newBuilder()
                                        .setIsGlobalScope(true)
                                        .setEnvironmentType(EnvironmentType.ON_PREM))
                                .setStaticGroupMembers(StaticMembers.newBuilder()
                                        .addMembersByType(StaticMembersByType
                                                .newBuilder()
                                                .setType(MemberType
                                                        .newBuilder()
                                                        .setEntity(ApiEntityType.PHYSICAL_MACHINE
                                                                .typeNumber())))))
                        .build()
                ));
        //THEN
        assertFalse(ApiUtils.getGlobalTempGroupEntityType(
                Collections.singleton("1234"), groupExpander).isPresent());
    }

    /**
     * Test containsGlobalScope function.
     */
    @Test
    public void testContainsGlobalScope() {
        //GIVEN
        when(groupExpander.getGroup(eq("1234"))).thenReturn(
                Optional.of(Grouping.newBuilder()
                        .setDefinition(GroupDefinition.newBuilder().setIsTemporary(true)
                                .setOptimizationMetadata(OptimizationMetadata.newBuilder()
                                        .setIsGlobalScope(true)
                                        .setEnvironmentType(EnvironmentType.ON_PREM)))
                        .build()
                ));
        when(groupExpander.getGroup(eq("5678"))).thenReturn(
                Optional.of(Grouping.newBuilder().build()));
        //THEN
        Set<String> groups1 = Collections.singleton(UuidMapper.UI_REAL_TIME_MARKET_STR);
        assertTrue(ApiUtils.containsGlobalScope(groups1, groupExpander));
        Set<String> groups2 = new HashSet<>();
        groups2.add("1234");
        Set<String> groups3 = new HashSet<>();
        groups3.add("5678");
        assertTrue(ApiUtils.containsGlobalScope(groups2, groupExpander));
        assertFalse(ApiUtils.containsGlobalScope(groups3, groupExpander));
    }
}
