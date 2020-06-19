package com.vmturbo.components.test.utilities.topology.conversion;

import static org.junit.Assert.assertEquals;

import java.util.stream.Collectors;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.stream.StreamSource;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.components.api.test.ResourcePath;

public class XmiElementTest {
    private XmiElement xmiElement;

    @Before
    public void setup() throws Exception {
        final String filename = "topologies/classic/test.customer.markets.topology";
        final StreamSource streamSource = new StreamSource(ResourcePath.getTestResource(getClass(), filename).toFile());
        final JAXBContext jc = JAXBContext.newInstance(XmiElement.class);
        final Unmarshaller unmarshaller = jc.createUnmarshaller();
        xmiElement = (XmiElement)unmarshaller.unmarshal(streamSource);
    }

    @Test
    public void testGroupParsing() {
        // Count the number of groups.
        assertEquals(121, xmiElement.getMainMarketGroups().count());

        // Count the number of dynamic groups.
        assertEquals(54, xmiElement.getMainMarketGroups()
            .filter(GroupElement::isDynamic)
            .count());

        // Count the number of groups of virtual machines.
        assertEquals(36L, (long)xmiElement.getMainMarketGroups()
            .collect(Collectors.groupingBy(GroupElement::getEntityType, Collectors.counting()))
            .get("VirtualMachine")
        );
    }
}