package com.vmturbo.repository.topology.protobufs;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;

/**
 * Test converting ArangoDB stored strings to raw topology DTOs.
 *
 */
public class TopologyProtobufReaderParsingTest {

    @Test
    public void testNotParsable() {
        TopologyEntityDTO empty = TopologyProtobufReader.parseJson("NOT-PARSABLE");
        assertEquals(-1, empty.getOid());
        assertEquals(0, empty.getEntityType());
    }

    @Test
    public void testJsonParser() {
        String str = "{\"entityType\":55,\"oid\":\"7\",\"displayName\":\"foo\"}";

        TopologyEntityDTO dto = TopologyProtobufReader.parseJson(str);
        assertEquals(55, dto.getEntityType());
        assertEquals(7L, dto.getOid());
        assertEquals("foo", dto.getDisplayName());
    }

}
