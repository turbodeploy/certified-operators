package com.vmturbo.protoc.spring.rest.test.echo;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.GsonHttpMessageConverter;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.AnnotationConfigWebContextLoader;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

import com.google.common.collect.ImmutableList;

import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.protoc.spring.rest.testServices.Echo;
import com.vmturbo.protoc.spring.rest.testServices.EchoREST;
import com.vmturbo.protoc.spring.rest.testServices.EchoREST.EchoServiceController;

@RunWith(SpringJUnit4ClassRunner.class)
@WebAppConfiguration
@ContextConfiguration(loader = AnnotationConfigWebContextLoader.class)
// Need clean context with no probes/targets registered.
@DirtiesContext(classMode = ClassMode.BEFORE_EACH_TEST_METHOD)
public class EchoServiceTest extends AbstractEchoServiceTest {

    /**
     * Nested configuration for Spring context.
     */
    @Configuration
    @EnableWebMvc
    static class ContextConfiguration extends WebMvcConfigurerAdapter {

        @Override
        public void configureMessageConverters(List<HttpMessageConverter<?>> converters) {
            GsonHttpMessageConverter msgConverter = new GsonHttpMessageConverter();
            msgConverter.setGson(ComponentGsonFactory.createGson());
            converters.add(msgConverter);
        }

        @Bean
        public NormalEchoService testService() {
            return Mockito.spy(new NormalEchoService());
        }

        @Bean
        public EchoServiceController restController() {
            return new EchoServiceController(testService());
        }
    }

    @Test
    public void testEcho() throws Exception {
        EchoREST.EchoServiceController.EchoServiceResponse<EchoREST.EchoResponse> response =
                parseEchoResponse(postAndExpect("/EchoService/echo", gson.toJson(inputRequest1), HttpStatus.OK));

        Mockito.verify(testService).echo(Mockito.any(), Mockito.any());

        Assert.assertNotNull(response.response);
        Assert.assertEquals(inputRequest1.getEchoThis(), response.response.getEcho());
    }

    @Test
    public void testMissingRequired() throws Exception {
        String serializedRequest = "{ \"extraOptional\":\"test\" }";
        EchoREST.EchoServiceController.EchoServiceResponse<EchoREST.EchoResponse> response =
                parseEchoResponse(postAndExpect("/EchoService/echo", serializedRequest, HttpStatus.INTERNAL_SERVER_ERROR));
        Assert.assertNotNull(response.error);
        Assert.assertNull(response.response);
    }

    @Test
    public void testMissingOptional() throws Exception {

        final EchoREST.EchoRequest noOptional = EchoREST.EchoRequest
                .fromProto(Echo.EchoRequest.newBuilder()
                        .setEchoThis("echoThis")
                        .build());

        EchoREST.EchoServiceController.EchoServiceResponse<EchoREST.EchoResponse> response =
                parseEchoResponse(postAndExpect("/EchoService/echo", gson.toJson(noOptional), HttpStatus.OK));
        Assert.assertNull(response.error);
        Assert.assertNotNull(response.response);
        Assert.assertEquals("echoThis", response.response.getEcho());
    }

    @Test
    public void testEchoServerStream() throws Exception {
        EchoREST.EchoServiceController.EchoServiceResponse<List<EchoREST.EchoResponse>> responses =
                parseListEchoResponse(postAndExpect("/EchoService/serverStreamEcho", gson.toJson(inputRequest1), HttpStatus.OK));

        Mockito.verify(testService).serverStreamEcho(Mockito.any(), Mockito.any());

        Assert.assertNotNull(responses.response);
        Assert.assertEquals(2, responses.response.size());
        Assert.assertEquals(inputRequest1.getEchoThis(), responses.response.get(0).getEcho());
        Assert.assertEquals(inputRequest1.getEchoThis(), responses.response.get(1).getEcho());
    }

    @Test
    public void testEchoClientStream() throws Exception {
        List<EchoREST.EchoRequest> requests = ImmutableList.of(inputRequest1, inputRequest2);
        EchoREST.EchoServiceController.EchoServiceResponse<EchoREST.EchoResponse> response =
                parseEchoResponse(postAndExpect("/EchoService/clientStreamEcho", gson.toJson(requests), HttpStatus.OK));

        Mockito.verify(testService).clientStreamEcho(Mockito.any());
        Assert.assertNotNull(response.response);
        Assert.assertEquals(inputRequest2.getEchoThis(), response.response.getEcho());
    }

    @Test
    public void testEchoBidirectionalStream() throws Exception {
        List<EchoREST.EchoRequest> requests = ImmutableList.of(inputRequest1, inputRequest2);
        EchoREST.EchoServiceController.EchoServiceResponse<List<EchoREST.EchoResponse>> responses =
                parseListEchoResponse(postAndExpect("/EchoService/biStreamEcho", gson.toJson(requests), HttpStatus.OK));

        Mockito.verify(testService).biStreamEcho(Mockito.any());

        Assert.assertNotNull(responses.response);
        Assert.assertEquals(2, responses.response.size());
        Assert.assertEquals(inputRequest1.getEchoThis(), responses.response.get(0).getEcho());
        Assert.assertEquals(inputRequest2.getEchoThis(), responses.response.get(1).getEcho());

    }
}
