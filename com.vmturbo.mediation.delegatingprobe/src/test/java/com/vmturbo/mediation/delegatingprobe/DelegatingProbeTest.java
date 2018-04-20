package com.vmturbo.mediation.delegatingprobe;

import static org.junit.Assert.assertEquals;

import javax.annotation.Nonnull;

import io.swagger.annotations.ApiOperation;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.json.GsonHttpMessageConverter;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;
import org.springframework.web.util.UriComponentsBuilder;

import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.api.test.IntegrationTestServer;
import com.vmturbo.mediation.delegatingprobe.DelegatingProbe.DelegatingDiscoveryRequest;
import com.vmturbo.platform.common.builders.CommodityBuilders;
import com.vmturbo.platform.common.builders.EntityBuilders;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;

/**
 * Unit test for {@link DelegatingProbe}.
 */
public class DelegatingProbeTest {

    private final static EntityDTO virtualMachine = EntityBuilders.virtualMachine("vm")
        .selling(CommodityBuilders.vCpuMHz().capacity(100.0))
        .build();

    private final static EntityDTO host = EntityBuilders.physicalMachine("pm")
        .selling(CommodityBuilders.vCpuMHz().capacity(42.0))
        .build();

    @Rule
    public TestName testName = new TestName();

    private IntegrationTestServer server;

    @Before
    public void startup() throws Exception {
        server = new IntegrationTestServer(testName, ContextConfiguration.class);
    }

    @After
    public void cleanup() throws Exception {
        server.close();
    }
    @Test
    public void testDiscoveryDelegation() throws Exception {
        final String uri = UriComponentsBuilder.newInstance()
            .scheme("http")
            .host("localhost")
            .port(server.connectionConfig().getPort())
            .path("test")
            .build()
            .toUriString();

        final DelegatingProbe delegatingProbe = new DelegatingProbe();
        final DelegatingProbeAccount account = new DelegatingProbeAccount("delegating-probe",
            uri, "discover");

        // First discovery should return VM
        final DiscoveryResponse firstResponse = delegatingProbe.discoverTarget(account);
        assertEquals(EntityType.VIRTUAL_MACHINE, firstResponse.getEntityDTOList().get(0).getEntityType());
        assertEquals(1, firstResponse.getEntityDTOCount());

        // Second discovery should return host
        final DiscoveryResponse secondResponse = delegatingProbe.discoverTarget(account);
        assertEquals(EntityType.PHYSICAL_MACHINE, secondResponse.getEntityDTOList().get(0).getEntityType());
        assertEquals(1, firstResponse.getEntityDTOCount());
    }

    /**
     * Nested configuration for Spring context.
     */
    @Configuration
    @EnableWebMvc
    protected static class ContextConfiguration extends WebMvcConfigurerAdapter {
        @Bean
        public TestController testController() {
            return new TestController();
        }

        @Bean
        public GsonHttpMessageConverter gsonHttpMessageConverter() {
            final GsonHttpMessageConverter msgConverter = new GsonHttpMessageConverter();
            msgConverter.setGson(ComponentGsonFactory.createGson());
            return msgConverter;
        }
    }

    @RestController
    public static class TestController {
        /**
         * Get discovery response.
         *
         * @param request The request.
         * @return The discovery response.
         */
        @ApiOperation(value = "Run a discovery")
        @RequestMapping(path = "/test/discover",
            method = RequestMethod.POST,
            consumes = {MediaType.APPLICATION_JSON_VALUE},
            produces = {MediaType.APPLICATION_OCTET_STREAM_VALUE})
        @ResponseBody
        public @Nonnull
        ResponseEntity<byte[]> discover(@RequestBody DelegatingDiscoveryRequest request) throws Exception {
            if (request.getDiscoveryIndex() == 0) {
                // First discovery returns the VM
                return new ResponseEntity<>(DiscoveryResponse.newBuilder()
                    .addEntityDTO(virtualMachine)
                    .build().toByteArray(), HttpStatus.OK);
            } else {
                // Second discovery returns the host
                return new ResponseEntity<>(DiscoveryResponse.newBuilder()
                    .addEntityDTO(host)
                    .build().toByteArray(), HttpStatus.OK);
            }

        }
    }
}