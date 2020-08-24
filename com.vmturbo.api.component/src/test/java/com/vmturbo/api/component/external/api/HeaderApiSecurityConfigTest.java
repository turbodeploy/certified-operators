package com.vmturbo.api.component.external.api;

import static org.junit.Assert.assertEquals;

import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.AnnotationConfigWebContextLoader;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

import com.vmturbo.auth.api.authorization.jwt.SecurityConstant.PredefinedRole;

/**
 * Test {@link HeaderApiSecurityConfig}.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@WebAppConfiguration
@ContextConfiguration(loader = AnnotationConfigWebContextLoader.class)
public class HeaderApiSecurityConfigTest {

    @Autowired
    private HeaderApiSecurityConfig headerApiSecurityConfig;

    /**
     * Test the role map is built correctly with default spring injected values.
     */
    @Test
    public void testDefaultRoleMap() {
        final Map<PredefinedRole, String> roleMap = headerApiSecurityConfig.getRoleMap();
        assertEquals(6, roleMap.size());
        assertEquals("Workload Optimizer Administrator, Account Administrator",
                roleMap.get(PredefinedRole.ADMINISTRATOR));
        assertEquals("Workload Optimizer Site admin", roleMap.get(PredefinedRole.SITE_ADMIN));
        assertEquals("Workload Optimizer Automator", roleMap.get(PredefinedRole.AUTOMATOR));
        assertEquals("Workload Optimizer Deployer", roleMap.get(PredefinedRole.DEPLOYER));
        assertEquals("Workload Optimizer Advisor", roleMap.get(PredefinedRole.ADVISOR));
        assertEquals("Workload Optimizer Observer, Read-Only", roleMap.get(PredefinedRole.OBSERVER));
    }

    /**
     * Nested configuration for Spring context.
     */
    @Configuration
    @EnableWebMvc
    @EnableGlobalMethodSecurity(prePostEnabled = true)
    static class ContextConfiguration extends WebSecurityConfigurerAdapter {

        /**
         * Bean to be tested.
         * @return test bean
         */
        @Bean
        public HeaderApiSecurityConfig headerApiSecurityConfig() {
            return new HeaderApiSecurityConfig();
        }
    }
}