package com.vmturbo.topology.processor.probeproperties;

import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.topology.processor.probeproperties.ProbePropertyStore.ProbePropertyKey;
import com.vmturbo.topology.processor.probes.ProbeException;
import com.vmturbo.topology.processor.targets.TargetNotFoundException;
import com.vmturbo.topology.processor.targets.TargetStoreException;

/**
 * Tests for the functionality of {@link KVBackedProbePropertyStore}.
 */
public class KVBackedProbePropertyStoreTest extends ProbePropertiesTestBase {
    /**
     * Set up mock probes and targets.
     */
    @Override
    @Before
    public void setUp() {
        super.setUp();
    }

    /**
     * Cannot get probe-specific probe properties from a non-existent probe.
     *
     * @throws Exception expected.
     */
    @Test(expected = ProbeException.class)
    public void testGetProbeSpecificPropertiesFromNonExistentProbe() throws Exception {
        probePropertyStore.getProbeSpecificProbeProperties(NON_EXISTENT_PROBE_ID);
    }

    /**
     * Cannot get target-specific probe properties from a non-existent probe.
     *
     * @throws Exception expected.
     */
    @Test(expected = ProbeException.class)
    public void testGetTargetSpecificPropertiesFromNonExistentProbe() throws Exception {
        probePropertyStore.getTargetSpecificProbeProperties(NON_EXISTENT_PROBE_ID, TARGET_ID_11);
    }

    /**
     * Cannot get target-specific probe properties from a non-existent target.
     *
     * @throws Exception expected.
     */
    @Test(expected = TargetNotFoundException.class)
    public void testGetTargetSpecificPropertiesFromNonExistentTarget() throws Exception {
        probePropertyStore.getTargetSpecificProbeProperties(PROBE_ID_1, NON_EXISTENT_TARGET_ID);
    }

    /**
     * Cannot get target-specific probe properties if the target is not discovered by the given probe.
     *
     * @throws Exception expected.
     */
    @Test(expected = TargetStoreException.class)
    public void testGetTargetSpecificPropertiesFromNonMatching() throws Exception {
        probePropertyStore.getTargetSpecificProbeProperties(PROBE_ID_1, TARGET_ID_2);
    }

    /**
     * Calling {@link KVBackedProbePropertyStore#putProbeProperty(ProbePropertyKey, String)} should
     * insert the appropriate properties in the table of the specified probe, replacing all other
     * previously existing properties.
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testPutProbeSpecificProperties() throws Exception {
        probePropertyStore.putAllProbeSpecificProperties(PROBE_ID_1, PROBE_PROPERTY_MAP_1);
        checkMapInProbe1(PROBE_PROPERTY_MAP_1, "C");

        probePropertyStore.putAllProbeSpecificProperties(PROBE_ID_1, PROBE_PROPERTY_MAP_2);
        checkMapInProbe1(PROBE_PROPERTY_MAP_2, "B");
    }

    /**
     * Cannot update the probe property table of a non-existent probe.
     *
     * @throws Exception expected.
     */
    @Test(expected = ProbeException.class)
    public void testPutInNonExistentProbe() throws Exception {
        probePropertyStore.putAllProbeSpecificProperties(NON_EXISTENT_PROBE_ID, PROBE_PROPERTY_MAP_1);
    }

    /**
     * This checks the methods that modify a specific probe property
     * {@link KVBackedProbePropertyStore#putProbeProperty(ProbePropertyKey, String)} and
     * {@link KVBackedProbePropertyStore#deleteProbeProperty(ProbePropertyKey)}.
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testSpecificProbePropertyMethods() throws Exception {
        // create and check property map 1
        probePropertyStore.putProbeProperty(new ProbePropertyKey(PROBE_ID_1, "A"), "Avalue");
        probePropertyStore.putProbeProperty(new ProbePropertyKey(PROBE_ID_1, "B"), "Bvalue");
        checkMapInProbe1(PROBE_PROPERTY_MAP_1, "C");

        // modify to create property map 2 and check again
        probePropertyStore.deleteProbeProperty(new ProbePropertyKey(PROBE_ID_1, "B"));
        probePropertyStore.putProbeProperty(new ProbePropertyKey(PROBE_ID_1, "C"), "Cvalue");
        probePropertyStore.putProbeProperty(new ProbePropertyKey(PROBE_ID_1, "A"), "Avalue1");
        checkMapInProbe1(PROBE_PROPERTY_MAP_2, "B");
    }

    /**
     * Cannot delete a non-existent probe property.
     *
     * @throws Exception expected.
     */
    @Test(expected = ProbeException.class)
    public void testDeleteNonExistentProbeProperty() throws Exception {
        probePropertyStore.deleteProbeProperty(new ProbePropertyKey(PROBE_ID_1, "A"));
    }

    /**
     * Checks that all probe properties of a given map are in a probe-specific probe property table
     * of probe1 and that no other table has any probe properties.  All possible getter methods of
     * {@link KVBackedProbePropertyStore} are excercised.
     *
     * @param propertiesExpected the probe property names and values that are expected.
     * @param notInMap a specific string that is a key in {@code probePropertiesExpected} map.
     * @throws Exception should not happen.
     */
    private void checkMapInProbe1(Map<String, String> propertiesExpected, String notInMap) throws Exception {
        // properties can be retrieved by looking at probe1
        Assert.assertEquals(
            propertiesExpected.entrySet(),
            probePropertyStore
                .getProbeSpecificProbeProperties(PROBE_ID_1)
                .collect(Collectors.toSet()));

        // properties can be retrieved individually
        for (Entry<String, String> e : propertiesExpected.entrySet()) {
            Assert.assertEquals(
                e.getValue(),
                probePropertyStore.getProbeProperty(new ProbePropertyKey(PROBE_ID_1, e.getKey())).get());
        }

        // if a property name is not in the map, then inquiring for that property
        // should return an empty string
        Assert.assertFalse(
            probePropertyStore
                .getProbeProperty(new ProbePropertyKey(PROBE_ID_1, notInMap))
                .isPresent()
        );

        // other probe property tables should be empty
        Assert.assertEquals(0, probePropertyStore.getProbeSpecificProbeProperties(PROBE_ID_2).count());
        Assert.assertEquals(
            0,
            probePropertyStore.getTargetSpecificProbeProperties(PROBE_ID_1, TARGET_ID_11).count());
        Assert.assertEquals(
            0,
            probePropertyStore.getTargetSpecificProbeProperties(PROBE_ID_2, TARGET_ID_2).count());
        Assert.assertFalse(
            probePropertyStore.getProbeProperty(new ProbePropertyKey(PROBE_ID_2, "A")).isPresent());
        Assert.assertFalse(
            probePropertyStore.getProbeProperty(
                new ProbePropertyKey(PROBE_ID_1, TARGET_ID_11, "A")).isPresent());
        Assert.assertFalse(
            probePropertyStore.getProbeProperty(
                new ProbePropertyKey(PROBE_ID_2, TARGET_ID_2, "A")).isPresent());

        // fetching all properties at once reflects the same situation
        probePropertyStore.getAllProbeProperties().forEach(
            e -> {
                Assert.assertEquals(PROBE_ID_1, e.getKey().getProbeId());
                Assert.assertFalse(e.getKey().isTargetSpecific());
                Assert.assertEquals(propertiesExpected.get(e.getKey().getName()), e.getValue());
            });
    }
}
