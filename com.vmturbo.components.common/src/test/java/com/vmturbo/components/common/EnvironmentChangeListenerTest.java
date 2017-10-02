package com.vmturbo.components.common;

import com.google.common.collect.Sets;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.context.environment.EnvironmentChangeEvent;
import org.springframework.context.ApplicationContext;
import org.springframework.core.env.Environment;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.web.context.WebApplicationContext;

import java.util.Set;

import static org.hamcrest.CoreMatchers.is;
import static org.mockito.Mockito.verify;

@RunWith(SpringJUnit4ClassRunner.class)
@WebAppConfiguration
@ContextConfiguration("classpath:environment-change-listener-test.xml")
public class EnvironmentChangeListenerTest {

    @Autowired
    IVmtComponent componentMock;

    Environment mockEnv = Mockito.mock(Environment.class);

    @Autowired
    ApplicationContext context;

    @Test
    public void onApplicationEvent() throws Exception {
        // Arrange
        Set<String> changedPropertykeys = Sets.newHashSet("a","b","c");
        EnvironmentChangeEvent evtChanged = new EnvironmentChangeEvent(changedPropertykeys);
        // Act
        context.publishEvent(evtChanged);
        // Assert
        verify(componentMock).configurationPropertiesChanged(context.getEnvironment(), changedPropertykeys);
    }

}