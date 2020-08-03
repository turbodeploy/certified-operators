package com.vmturbo.clustermgr;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;

import com.orbitz.consul.model.catalog.CatalogService;

import org.junit.Test;

import com.vmturbo.clustermgr.ConsulService.ComponentInstance;
import com.vmturbo.components.common.health.ConsulHealthcheckRegistration;

/**
 * Unit tests for {@link ConsulService}.
 */
public class ConsulServiceTest {
    private static final String ID = "my-component-1";
    private static final String ADDRESS = "128.182.17.1";
    private static final int PORT = 192;

    /**
     * Test {@link ComponentInstance#getUri(String)}.
     */
    @Test
    public void testComponentInstanceUrl() {
        final CatalogService catalogService = mock(CatalogService.class);
        when(catalogService.getServiceAddress()).thenReturn(ADDRESS);
        when(catalogService.getServicePort()).thenReturn(PORT);
        when(catalogService.getServiceId()).thenReturn(ID);

        final ComponentInstance instance = new ComponentInstance(catalogService);
        assertThat(instance.getId(), is(ID));
        assertThat(instance.getUri("/foo").toString(), is("http://" + ADDRESS + ":" + PORT + "/foo"));
    }

    /**
     * Test {@link ComponentInstance#getUri(String)} when there is a prefix encoded in the service
     * tags.
     */
    @Test
    public void testComponentInstanceWithPrefix() {
        final CatalogService catalogService = mock(CatalogService.class);
        when(catalogService.getServiceAddress()).thenReturn(ADDRESS);
        when(catalogService.getServicePort()).thenReturn(PORT);
        when(catalogService.getServiceId()).thenReturn(ID);
        when(catalogService.getServiceTags()).thenReturn(Collections.singletonList(
            ConsulHealthcheckRegistration.encodeInstanceRoute("/prefix")));

        final ComponentInstance instance = new ComponentInstance(catalogService);
        assertThat(instance.getId(), is(ID));
        assertThat(instance.getUri("/foo").toString(), is("http://" + ADDRESS + ":" + PORT + "/prefix/foo"));
    }

}