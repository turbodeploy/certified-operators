package com.vmturbo.matrix.component;

import static com.vmturbo.matrix.component.CommunicationMatrixTest.CENTER_OID;
import static com.vmturbo.matrix.component.CommunicationMatrixTest.constructEdges;
import static com.vmturbo.matrix.component.CommunicationMatrixTest.ipFromIndex;

import java.util.Optional;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.platform.common.dto.CommonDTO.FlowDTO.Protocol;

/**
 * The {@link SEMapperTest} implements ServiceEntityOIDMapper related tests.
 */
public class SEMapperTest {
    /**
     * Setup.
     */
    @Before
    public void setup() {
        System.setProperty("vmt.matrix.updatecycle", "0");
        IdentityGenerator.initPrefix(100L);
    }

    /**
     * Tests basics.
     */
    @Test
    public void testBasics() {
        ServiceEntityOIDMapper mapper = new ServiceEntityOIDMapper();
        // Nothing there, fail.
        mapper.remap("abc", 123L);
        Assert.assertFalse(mapper.uuidToOid("abc").isPresent());
        // Map.
        mapper.map("abc");
        Assert.assertEquals(1, mapper.seToOid_.size());
        Assert.assertEquals(1, mapper.oidToSe_.size());
        Assert.assertTrue(mapper.uuidToOid("abc").isPresent());
        // Remap.
        mapper.remap("abc", 123L);
        Assert.assertEquals(1, mapper.seToOid_.size());
        Assert.assertEquals(1, mapper.oidToSe_.size());
        Assert.assertTrue(mapper.uuidToOid("abc").isPresent());
        // Test
        Long oid = mapper.seToOid_.get("abc");
        Optional<String> uuid = mapper.oidToUuid(oid);
        Assert.assertTrue(uuid.isPresent());
        Assert.assertEquals("abc", uuid.get());
        // No such OID
        Assert.assertFalse(mapper.oidToUuid(oid + 1).isPresent());
        // Test clear.
        Assert.assertFalse(mapper.oidToSe_.isEmpty());
        Assert.assertFalse(mapper.seToOid_.isEmpty());
        mapper.clear();
        Assert.assertTrue(mapper.seToOid_.isEmpty());
        Assert.assertTrue(mapper.oidToSe_.isEmpty());
    }

    /**
     * Tests population of not available endpoints.
     */
    @Test
    public void testPopulateNotAvailable() {
        int count = 10;
        CommunicationMatrix matrix = new CommunicationMatrix();
        matrix.update(constructEdges(count, -1, 8080, 1, 1, 1,
                                     Protocol.TCP));
        // See if we have an accurate reading. The default is SITE.
        final long underlayStart = 1000000L;
        for (int i = 0; i < count; i++) {
            long key = underlayStart + i + 1;
            matrix.populateUnderlay(key, 999L);
        }

        for (int i = 0; i < count; i++) {
            String ip = ipFromIndex("2", 1 + i);
            matrix.setEndpointOID(i + 1L, ip);
        }
        for (int i = 0; i < count; i++) {
            matrix.place((i + 1), underlayStart + i + 1);
        }

        matrix.setEndpointOID(CENTER_OID, "1.1.1.1");
        matrix.place(CENTER_OID, 999L);
    }
}
