package com.vmturbo.clustermgr;

import java.util.Collections;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.Sets;
import com.pszymczyk.consul.junit.ConsulResource;

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

    @Before
    public void startup() {
        final ComponentPropertiesMap componentPropertiesMap = new ComponentPropertiesMap();
        final FactoryInstalledComponentsService fic =
                Mockito.mock(FactoryInstalledComponentsService.class);
        Mockito.when(fic.getFactoryInstalledComponents()).thenReturn(componentPropertiesMap);
        final ConsulService consulService = new ConsulService("localhost", consul.getHttpPort());
        svc = new ClusterMgrService(consulService, fic);

        final ComponentProperties componentProperties = new ComponentProperties();
        componentProperties.put(PROP_1, PROP_1_DEF_VAL);
        componentProperties.put(PROP_2, PROP_2_DEF_VAL);
        componentPropertiesMap.addComponentConfiguration(COMP_TYPE_1, componentProperties);
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
     * Tests reinitialization of default values (mimics restart of cluster manager). It is
     * expected, that instance-specific values are not rewritten, while default values are fully
     * rewritten.
     */
    @Test
    public void testInitializeRewritesDefaults() {
        svc.initializeClusterKVStore();

        final String instanceId = svc.getComponentInstanceIds(COMP_TYPE_1).iterator().next();
        final String val1 = "Devil pixies";
        final String val2 = "Durmstrang";
        final String prop3 = "favourite spell";
        final String prop3val = "Expelliarmus";
        svc.setPropertyForComponentType(COMP_TYPE_1, PROP_1, val1);
        svc.setPropertyForComponentType(COMP_TYPE_1, prop3, prop3val);
        svc.setPropertyForComponentInstance(COMP_TYPE_1, instanceId, PROP_2, val2);
        Assert.assertEquals(val1,
                svc.getDefaultPropertiesForComponentType(COMP_TYPE_1).get(PROP_1));
        Assert.assertEquals(prop3val,
                svc.getDefaultPropertiesForComponentType(COMP_TYPE_1).get(prop3));
        Assert.assertEquals(val2,
                svc.getComponentInstanceProperty(COMP_TYPE_1, instanceId, PROP_2));

        svc.initializeClusterKVStore();
        Assert.assertEquals(PROP_1_DEF_VAL,
                svc.getDefaultPropertiesForComponentType(COMP_TYPE_1).get(PROP_1));
        Assert.assertEquals(val2,
                svc.getComponentInstanceProperty(COMP_TYPE_1, instanceId, PROP_2));
        Assert.assertEquals(Sets.newHashSet(PROP_1, PROP_2),
                svc.getDefaultPropertiesForComponentType(COMP_TYPE_1).keySet());
    }
}
