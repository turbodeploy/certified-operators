package com.vmturbo.components.api;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;
import javax.xml.transform.Source;

import org.springframework.http.converter.ByteArrayHttpMessageConverter;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.ResourceHttpMessageConverter;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.GsonHttpMessageConverter;
import org.springframework.http.converter.support.AllEncompassingFormHttpMessageConverter;
import org.springframework.http.converter.xml.Jaxb2RootElementHttpMessageConverter;
import org.springframework.http.converter.xml.MappingJackson2XmlHttpMessageConverter;
import org.springframework.http.converter.xml.SourceHttpMessageConverter;
import org.springframework.util.ClassUtils;
import org.springframework.web.client.RestTemplate;

import com.google.gson.Gson;

/**
 * A utility class to create uniform {@link RestTemplate} instances for components to
 * use to make REST requests to each other.serialize and deserialize their responses.
 */
public class ComponentRestTemplate {

    private static final boolean JAX_B_2_PRESENT =
            ClassUtils.isPresent("javax.xml.bind.Binder", ComponentRestTemplate.class.getClassLoader());

    private static final boolean jackson2XmlPresent =
            ClassUtils.isPresent("com.fasterxml.jackson.dataformat.xml.XmlMapper", ComponentRestTemplate.class.getClassLoader());

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

        // The regular RestTemplate also adds the Jackson converter by default, but we will
        // use GSON. It's probably fine.

        if (jackson2XmlPresent) {
            converters.add(new MappingJackson2XmlHttpMessageConverter());
        } else if (JAX_B_2_PRESENT) {
            converters.add(new Jaxb2RootElementHttpMessageConverter());
        }

        return new RestTemplate(converters);
    }

}
