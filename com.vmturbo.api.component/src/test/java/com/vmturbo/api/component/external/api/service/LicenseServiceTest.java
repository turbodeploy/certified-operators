package com.vmturbo.api.component.external.api.service;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.junit.Before;
import org.junit.Rule;
import org.mockito.Mockito;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.client.RestTemplate;

import io.grpc.Channel;

import com.vmturbo.auth.api.usermgmt.AuthUserDTO;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO.PROVIDER;
import com.vmturbo.common.protobuf.licensing.LicenseCheckServiceGrpc;
import com.vmturbo.common.protobuf.licensing.LicenseCheckServiceGrpc.LicenseCheckServiceBlockingStub;
import com.vmturbo.common.protobuf.licensing.LicenseManagerServiceGrpc;
import com.vmturbo.common.protobuf.licensing.LicenseManagerServiceGrpc.LicenseManagerServiceBlockingStub;
import com.vmturbo.common.protobuf.licensing.LicensingMoles.LicenseCheckServiceMole;
import com.vmturbo.common.protobuf.licensing.LicensingMoles.LicenseManagerServiceMole;
import com.vmturbo.components.api.test.GrpcTestServer;

/**
 * Test the LicenseService.
 * {@link LicenseService}
 */
public class LicenseServiceTest {

    private static String licenseString = "<?xml version=\"1.0\"?>\n" +
            " \n" +
            "<!-- VMTurbo license file; license created: 2015-03-12 -->\n" +
            " \n" +
            "<license>\n" +
            "  <first-name>Saipriya</first-name>\n" +
            "  <last-name>Balasubramanian</last-name>\n" +
            "  <email>saipriya.balasubramanian@turbonomic.com</email>\n" +
            "  <num-sockets>200</num-sockets>\n" +
            "  <expiration-date>2050-01-31</expiration-date>\n" +
            "  <lock-code>1944c723f8bcaf1ed6831a4f9d865794</lock-code>\n" +
            "  <feature FeatureName=\"storage\"/>\n" +
            "  <feature FeatureName=\"fabric\"/>\n" +
            "  <feature FeatureName=\"network_control\"/>\n" +
            "  <feature FeatureName=\"public_cloud\"/>\n" +
            "  <feature FeatureName=\"cloud_cost\"/>\n" +
            "  <feature FeatureName=\"container_control\"/>\n" +
            "  <feature FeatureName=\"app_control\"/>\n" +
            "  <feature FeatureName=\"applications\"/>\n" +
            "  <feature FeatureName=\"historical_data\"/>\n" +
            "  <feature FeatureName=\"multiple_vc\"/>\n" +
            "  <feature FeatureName=\"scoped_user_view\"/>\n" +
            "  <feature FeatureName=\"customized_views\"/>\n" +
            "  <feature FeatureName=\"group_editor\"/>\n" +
            "  <feature FeatureName=\"vmturbo_api\"/>\n" +
            "  <feature FeatureName=\"automated_actions\"/>\n" +
            "  <feature FeatureName=\"active_directory\"/>\n" +
            "  <feature FeatureName=\"custom_reports\"/>\n" +
            "  <feature FeatureName=\"planner\"/>\n" +
            "  <feature FeatureName=\"optimizer\"/>\n" +
            "  <feature FeatureName=\"full_policy\"/>\n" +
            "  <feature FeatureName=\"loadbalancer\"/>\n" +
            "  <feature FeatureName=\"deploy\"/>\n" +
            "  <feature FeatureName=\"aggregation\"/>\n" +
            "  <feature FeatureName=\"cloud_targets\"/>\n" +
            "  <feature FeatureName=\"cluster_flattening\"/>\n" +
            " \n" +
            "</license>";

    private RestTemplate restTemplate = Mockito.mock(RestTemplate.class);
    private ResponseEntity<String> result = Mockito.mock(ResponseEntity.class);
    private LicenseService licenseService;

    @Before
    public void init() throws Exception {
        Channel channelMock = Mockito.mock(Channel.class);
        LicenseManagerServiceBlockingStub licenseManagerService = LicenseManagerServiceGrpc.newBlockingStub(channelMock);
        LicenseCheckServiceBlockingStub licenseCheckService = LicenseCheckServiceGrpc.newBlockingStub(channelMock);

        licenseService = new LicenseService("auth", 9400, restTemplate,
                licenseManagerService, licenseCheckService);
        HttpHeaders headers = new HttpHeaders();
        headers.setAccept(ImmutableList.of(MediaType.APPLICATION_JSON));
        when(result.getBody()).thenReturn(licenseString);
        when(restTemplate.exchange("http://auth:9400/license", HttpMethod.GET, new HttpEntity<>(headers),
                String.class)).thenReturn(result);
        when(restTemplate.exchange("http://auth:9400/license", HttpMethod.POST, new HttpEntity<>(headers),
                String.class)).thenReturn(result);
    }

    /**
     * Performs the actual logon from the token passed though.
     */
    private void logon(String role) throws Exception {
        // Local authentication
        Set<GrantedAuthority> grantedAuths = new HashSet<>();
        for (String r : role.split("\\|")) {
            grantedAuths.add(new SimpleGrantedAuthority("ROLE" + "_" + r.toUpperCase()));
        }
        AuthUserDTO authUserDTO = new AuthUserDTO(PROVIDER.LOCAL, "admin", "admin00",
                "1.1.1.1", "uuid", "token", Lists.newArrayList());
        SecurityContextHolder.getContext().setAuthentication(
                new UsernamePasswordAuthenticationToken(authUserDTO, "admin00", grantedAuths));
    }
}
