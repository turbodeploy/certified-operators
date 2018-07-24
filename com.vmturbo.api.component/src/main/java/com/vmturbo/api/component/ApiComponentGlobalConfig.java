package com.vmturbo.api.component;

import java.nio.charset.Charset;
import java.util.List;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.GsonHttpMessageConverter;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

import com.vmturbo.api.handler.GlobalExceptionHandler;
import com.vmturbo.api.interceptors.TelemetryInterceptor;
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
     * Register a {@link TelemetryInterceptor} to collect metrics about API usage.
     *
     * @param registry The registry to which we will add interceptors.
     */
    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(telemetryInterceptor()).addPathPatterns("/**");
    }

    @Bean
    public IdentityInitializer identityInitializer() {
        return new IdentityInitializer(identityGeneratorPrefix);
    }

    @Bean
    public GlobalExceptionHandler exceptionHandler() {
        return new GlobalExceptionHandler();
    }

    /**
     * Create a telemetry interceptor to collect REST API usage and latency metrics.
     *
     * @return A {@link TelemetryInterceptor} instance.
     */
    @Bean
    public TelemetryInterceptor telemetryInterceptor() {
        return new TelemetryInterceptor();
    }
}

