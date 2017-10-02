package com.vmturbo.api.component;

import java.nio.charset.Charset;
import java.util.List;

import org.apache.catalina.connector.Connector;
import org.apache.coyote.http11.Http11NioProtocol;
import org.apache.tomcat.util.net.Constants;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.embedded.EmbeddedServletContainerCustomizer;
import org.springframework.boot.context.embedded.tomcat.TomcatEmbeddedServletContainerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.GsonHttpMessageConverter;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

import com.vmturbo.api.handler.GlobalExceptionHandler;
import com.vmturbo.commons.idgen.IdentityInitializer;
import com.vmturbo.components.api.ComponentGsonFactory;

/**
 * Configuration of things that affect the entire API component and don't fit into
 * any specific package.
 */
@Configuration
@EnableWebMvc
public class ApiComponentGlobalConfig extends WebMvcConfigurerAdapter {

    @Value("${identityGeneratorPrefix}")
    private long identityGeneratorPrefix;

    @Value("${keystoreFile}")
    private String keystoreFile_;

    @Value("${keystorePass:jumpy-crazy-experience}")
    private String keystorePass_;

    @Value("${keystoreType}")
    private String keystoreType_;

    @Value("${keystoreAlias}")
    private String keystoreAlias_;

    /**
     * Add a new instance of the {@link GsonHttpMessageConverter} to the list of available {@link HttpMessageConverter}s in use.
     *
     * @param converters is the list of {@link HttpMessageConverter}s to which the new converter instance is added.
     */
    @Override
    public void extendMessageConverters(List<HttpMessageConverter<?>> converters) {
        // Handle text-plain.
        final StringHttpMessageConverter stringMessageConverter =
                new StringHttpMessageConverter(Charset.forName("UTF-8"));
        converters.add(stringMessageConverter);

        // GSON for application-json serialization.
        final GsonHttpMessageConverter msgConverter = new GsonHttpMessageConverter();
        msgConverter.setGson(ComponentGsonFactory.createGson());

        converters.add(msgConverter);
    }

    /**
     * Redirect HTTP to HTTPS.
     *
     * @return The TomcatEmbeddedServletContainerFactory.
     */
    @Bean
    public EmbeddedServletContainerCustomizer containerCustomizer() {
        return container -> {
            TomcatEmbeddedServletContainerFactory tomcat =
                    (TomcatEmbeddedServletContainerFactory)container;
            Connector connector = new Connector(TomcatEmbeddedServletContainerFactory.DEFAULT_PROTOCOL);
            connector.setPort(9443);
            connector.setSecure(true);
            connector.setScheme("https");

            Http11NioProtocol proto = (Http11NioProtocol)connector.getProtocolHandler();
            proto.setSSLEnabled(true);

            proto.setSSLProtocol(Constants.SSL_PROTO_TLSv1_2);
            proto.setSSLCipherSuite("TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256, " +
                                    "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256, " +
                                    "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384, " +
                                    "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384");
            proto.setUseServerCipherSuitesOrder("true");
            proto.setSSLHonorCipherOrder("true");

            proto.setKeystoreFile(keystoreFile_);
            proto.setKeystorePass(keystorePass_);
            proto.setKeystoreType(keystoreType_);
            proto.setKeyAlias(keystoreAlias_);
            tomcat.addAdditionalTomcatConnectors(connector);
        };
    }

    @Bean
    public IdentityInitializer identityInitializer() {
        return new IdentityInitializer(identityGeneratorPrefix);
    }

    @Bean
    public GlobalExceptionHandler exceptionHandler() {
        return new GlobalExceptionHandler();
    }

}

