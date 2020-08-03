package com.vmturbo.components.api;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nonnull;
import javax.xml.transform.Source;

import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.http.converter.ByteArrayHttpMessageConverter;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.ResourceHttpMessageConverter;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.GsonHttpMessageConverter;
import org.springframework.http.converter.support.AllEncompassingFormHttpMessageConverter;
import org.springframework.http.converter.xml.SourceHttpMessageConverter;
import org.springframework.web.client.RestTemplate;

import com.google.common.collect.Lists;
import com.google.gson.Gson;

import com.vmturbo.components.api.tracing.RestTemplateTracingInterceptor;

/**
 * A utility class to create uniform {@link RestTemplate} instances for components to
 * use to make REST requests to each other.serialize and deserialize their responses.
 */
public class ComponentRestTemplate {

    private static final Gson GSON = ComponentGsonFactory.createGson();

    /**
     * Create a {@link RestTemplate} to use to communicate with controllers that have a
     * REST interface.
     *
     * @return The properly configured {@link RestTemplate}.
     */
    @Nonnull
    public static RestTemplate create() {
        final GsonHttpMessageConverter gsonConverter = new GsonHttpMessageConverter();
        gsonConverter.setGson(GSON);

        final List<HttpMessageConverter<?>> converters = new ArrayList<>();
        converters.add(new ByteArrayHttpMessageConverter());
        converters.add(new StringHttpMessageConverter());
        converters.add(new ResourceHttpMessageConverter());
        converters.add(new SourceHttpMessageConverter<Source>());
        converters.add(new AllEncompassingFormHttpMessageConverter());

        // Add GSON first, so that if both JSON and XML encoding is available and no explicit
        // content type is set we will use GSON.
        converters.add(gsonConverter);

        final RestTemplate template = new RestTemplate(converters);
        // The new interceptors come first.
        final List<ClientHttpRequestInterceptor> interceptors =
            Lists.newArrayList(new RestTemplateTracingInterceptor());
        interceptors.addAll(template.getInterceptors());
        template.setInterceptors(interceptors);
        return template;
    }

}
