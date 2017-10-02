package com.vmturbo.clustermgr;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@RunWith(SpringJUnit4ClassRunner.class)
@WebAppConfiguration
@ContextConfiguration("classpath:factory-installed-components-test.xml")
public class FactoryInstalledComponentsServiceTest {
    @Test
    public void getFactoryInstalledComponents() throws Exception {
        // Arrange
        FactoryInstalledComponentsService service = new FactoryInstalledComponentsService();
        // Act
        ComponentPropertiesMap components = service.getFactoryInstalledComponents();
        // Assert
        assertThat(components.getComponents().size(), is(3));
    }

}