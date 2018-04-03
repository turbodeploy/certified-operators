package com.vmturbo.clustermgr;

import java.util.Collections;
import java.util.Map;

import com.google.common.collect.Sets;
import com.pszymczyk.consul.junit.ConsulResource;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Integration tests for {@link ClusterMgrService}. Run against live consul (started
 * automatically embedded).
 */
public class ClusterMgrServiceIT {

    private static final String PROP_1 = "race";
    private static final String PROP_2 = "degree";
    private static final String PROP_1_DEF_VAL = "human";
    private static final String PROP_2_DEF_VAL = "Hogwarts, 5 OWL";
    private static final String COMP_TYPE_1 = "staff";

    @Rule
    public final ConsulResource consul = new ConsulResource();

    private ClusterMgrService svc;
    private ComponentPropertiesMap factoryInstalledComponents;

    @Before
    public void startup() {
        factoryInstalledComponents = new ComponentPropertiesMap();
        final FactoryInstalledComponentsService fic =
                Mockito.mock(FactoryInstalledComponentsService.class);
        Mockito.when(fic.getFactoryInstalledComponents()).thenReturn(factoryInstalledComponents);
        final ConsulService consulService = new ConsulService("localhost", consul.getHttpPort());
        svc = new ClusterMgrService(consulService, fic);

        final ComponentProperties componentProperties = new ComponentProperties();
        componentProperties.put(PROP_1, PROP_1_DEF_VAL);
        componentProperties.put(PROP_2, PROP_2_DEF_VAL);
        factoryInstalledComponents.addComponentConfiguration(COMP_TYPE_1, componentProperties);
    }

    /**
     * Tests initialization of default values on empty KV store.
     */
    @Test
    public void testInitDefaultValues() {
        svc.initializeClusterKVStore();

        Assert.assertEquals(PROP_1_DEF_VAL,
                svc.getDefaultPropertiesForComponentType(COMP_TYPE_1).get(PROP_1));
        Assert.assertEquals(PROP_2_DEF_VAL,
                svc.getDefaultPropertiesForComponentType(COMP_TYPE_1).get(PROP_2));

        Assert.assertEquals(Collections.singleton(COMP_TYPE_1), svc.getKnownComponents());
        final String instanceId = svc.getComponentInstanceIds(COMP_TYPE_1).iterator().next();
        Assert.assertEquals(Collections.singleton(instanceId),
                svc.getComponentInstanceIds(COMP_TYPE_1));
        Assert.assertNotNull(svc.getComponentInstanceProperties(COMP_TYPE_1, instanceId));

        Assert.assertNull(svc.getComponentInstanceProperty(COMP_TYPE_1, instanceId, PROP_1));
        Assert.assertNull(svc.getComponentInstanceProperty(COMP_TYPE_1, instanceId, PROP_2));
    }

    /**
     * Tests reinitialization of default values (mimics restart migration to a new version of
     * appliance). It is expected, that new components will receive new set of properties,
     * defined in new factoryInstalledComponents.yml file
     */
    @Test
    public void testsChangeOfPropertiesSet() {
        svc.initializeClusterKVStore();
        final String instanceId = svc.getComponentInstanceIds(COMP_TYPE_1).iterator().next();
        Assert.assertEquals(Sets.newHashSet(PROP_1, PROP_2),
                svc.getComponentInstanceProperties(COMP_TYPE_1, instanceId).keySet());

        final String val1 = "Devil pixies";
        final String val2 = "Durmstrang";
        final String prop3 = "favourite spell";
        final String prop3val = "Expelliarmus";
        final ComponentProperties componentProperties = new ComponentProperties();
        componentProperties.put(PROP_1, val1);
        componentProperties.put(PROP_2, PROP_2_DEF_VAL);
        componentProperties.put(prop3, prop3val);
        factoryInstalledComponents.addComponentConfiguration(COMP_TYPE_1, componentProperties);
        svc.initializeClusterKVStore();
        Assert.assertEquals(componentProperties,
                svc.getComponentInstanceProperties(COMP_TYPE_1, instanceId));

    }

    /**
     * Tests, that method {@link ClusterMgrService#getComponentInstanceProperties} returns the
     * values where defaults are ovewritten by instance-specific values.
     */
    @Test
    public void testGetInstanceProperties() {
        svc.initializeClusterKVStore();
        final String instanceId = svc.getComponentInstanceIds(COMP_TYPE_1).iterator().next();
        svc.setPropertyForComponentInstance(COMP_TYPE_1, instanceId, PROP_1, "new value");
        final Map<String, String> props =
                svc.getComponentInstanceProperties(COMP_TYPE_1, instanceId);
        Assert.assertEquals("new value", props.get(PROP_1));
        Assert.assertEquals(PROP_2_DEF_VAL, props.get(PROP_2));
    }
}
