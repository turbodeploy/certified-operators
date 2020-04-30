package com.vmturbo.api.component;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.GsonHttpMessageConverter;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

import com.vmturbo.api.component.external.api.interceptor.DevFreemiumInterceptor;
import com.vmturbo.api.component.external.api.interceptor.LicenseInterceptor;
import com.vmturbo.api.component.external.api.interceptor.TracingInterceptor;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.handler.GlobalExceptionHandler;
import com.vmturbo.api.interceptors.TelemetryInterceptor;
import com.vmturbo.api.serviceinterfaces.IAppVersionInfo;
import com.vmturbo.auth.api.licensing.LicenseCheckClientConfig;
import com.vmturbo.commons.idgen.IdentityInitializer;
import com.vmturbo.components.api.ComponentGsonFactory;

/**
 * Configuration of things that affect the entire API component and don't fit into
 * any specific package.
 */
@Configuration
@EnableWebMvc
@Import({LicenseCheckClientConfig.class})
public class ApiComponentGlobalConfig extends WebMvcConfigurerAdapter {

    @Value("${identityGeneratorPrefix}")
    private long identityGeneratorPrefix;

    @Value("${publicVersionString}")
    private String publicVersionString;

    @Value("${build-number.build}")
    private String buildNumber;

    @Autowired
    private LicenseCheckClientConfig licenseCheckClientConfig;

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
        // license interceptor which validates current license for every request except for some
        // endpoints which should be allowed even license is invalid
        registry.addInterceptor(licenseInterceptor())
            .addPathPatterns("/**")
            .excludePathPatterns("/login")
            .excludePathPatterns("/logout")
            .excludePathPatterns("/licenses/**")
            .excludePathPatterns("/license/**")
            .excludePathPatterns("/checkInit")
            .excludePathPatterns("/initAdmin")
            .excludePathPatterns("/notifications/**")
            .excludePathPatterns("/users/administrator/reset")
            .excludePathPatterns("/cluster/proactive/initialized")
            .excludePathPatterns("/cluster/proactive/enabled")
            .excludePathPatterns("/cluster/isXLEnabled")
            .excludePathPatterns("/admin/versions")
            .excludePathPatterns("/admin/productcapabilities")
            .excludePathPatterns("/users/me")
            .excludePathPatterns("/users/saml")
            .excludePathPatterns("/health")
            // exclude "/" so that redirection from "/" to "/app/index.html" works
            // as defined in ExternalApiConfig
            .excludePathPatterns("/")
            // Fix after upgrading to Spring 5.x, initialization and license installation pages failed to
            // load due to they are blocked by license interceptor.
            .excludePathPatterns("/assets/**")
            .excludePathPatterns("/doc/**")
            .excludePathPatterns("/widgetsets/**")
            .excludePathPatterns("/app/**");
        registry.addInterceptor(devFreemiumInterceptor())
            .addPathPatterns("/actions/**")
            .addPathPatterns("/businessunits/**")
            .addPathPatterns("/deploymentprofiles/**")
            .addPathPatterns("/entities/*/actions/**")
            .addPathPatterns("/groups/*/actions/**")
            .addPathPatterns("/markets/**")
            .addPathPatterns("/plandestinations/**")
            .addPathPatterns("/policies/**")
            .addPathPatterns("/pricelists/**")
            .addPathPatterns("/reports/**")
            .addPathPatterns("/reservations/**")
            .addPathPatterns("/search/" + UuidMapper.UI_REAL_TIME_MARKET_STR + "/**")
            .addPathPatterns("/settings/**")
            .addPathPatterns("/settingspolicies/**")
            .addPathPatterns("/stats/**")
            .addPathPatterns("/templates/**");
        registry.addInterceptor(restTracingInterceptor());
    }

    @Bean
    public IdentityInitializer identityInitializer() {
        return new IdentityInitializer(identityGeneratorPrefix);
    }

    @Bean
    public GlobalExceptionHandler exceptionHandler() {
        return new GlobalExceptionHandler();
    }

    @Bean
    public IAppVersionInfo versionInfo() {
        // Build Number includes quotation marks around it. Strip those if they are present.
        final String strippedBuildNumber = buildNumber.replaceAll("^\"|\"$", "");
        return new AppVersionInfo(publicVersionString, strippedBuildNumber);
    }

    /**
     * Create a telemetry interceptor to collect REST API usage and latency metrics.
     *
     * @return A {@link TelemetryInterceptor} instance.
     */
    @Bean
    public TelemetryInterceptor telemetryInterceptor() {
        return new TelemetryInterceptor(versionInfo());
    }

    @Bean
    public TracingInterceptor restTracingInterceptor() {
        return new TracingInterceptor();
    }

    /**
     * Create a license interceptor to check if current license is valid or not.
     *
     * @return A {@link LicenseInterceptor} instance.
     */
    @Bean
    public LicenseInterceptor licenseInterceptor() {
        return new LicenseInterceptor(licenseCheckClientConfig.licenseCheckClient());
    }

    /**
     * Create an interceptor to check if the current request is allowed based on the license used.
     *
     * @return A {@link DevFreemiumInterceptor} instance.
     */
    @Bean
    public DevFreemiumInterceptor devFreemiumInterceptor() {
        return new DevFreemiumInterceptor(licenseCheckClientConfig.licenseCheckClient());
    }
    /**
     * Implement the {@link IAppVersionInfo} to provide version info to the {@link TelemetryInterceptor}.
     */
    private static class AppVersionInfo implements IAppVersionInfo {
        private final String publicVersionString;
        private final String buildNumber;

        AppVersionInfo(@Nonnull final String publicVersionString,
                       @Nonnull final String buildNumber) {
            this.publicVersionString = Objects.requireNonNull(publicVersionString);
            this.buildNumber = Objects.requireNonNull(buildNumber);
        }

        @Override
        public String getVersion() {
            return publicVersionString;
        }

        @Override
        public String getBuildTime() {
            return buildNumber;
        }

        @Override
        public String getBuildUser() {
            return "";
        }

        @Override
        public String getGitCommit() {
            return "";
        }

        @Override
        public String getGitBranch() {
            return "";
        }

        @Override
        public String getGitDescription() {
            return "";
        }

        @Override
        public boolean hasCodeChanges() {
            return false;
        }

        @Override
        public String getProperty(String s) {
            return "";
        }

        @Override
        public String getProperty(String s, String s1) {
            return "";
        }
    }
}

