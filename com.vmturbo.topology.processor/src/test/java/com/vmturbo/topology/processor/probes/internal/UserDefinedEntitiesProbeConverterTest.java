package com.vmturbo.topology.processor.probes.internal;

import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.APPLICATION;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.DATABASE_SERVER;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.SERVICE;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.VIRTUAL_MACHINE;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.common.protobuf.group.GroupDTO.EntityDefinitionData;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PerTargetEntityInformation;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.DiscoveryOrigin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Origin;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;

/**
 * Test class for {@link UserDefinedEntitiesProbeConverter}.
 */
public class UserDefinedEntitiesProbeConverterTest {

    /**
     * Group should not be converted if it has no members.
     */
    @Test
    public void testEmptyMembers() {
        Grouping group = createGroup(SERVICE);
        Collection<TopologyEntityDTO> emptyMembers = Collections.emptySet();
        Map<Grouping, Collection<TopologyEntityDTO>> groups = Collections.singletonMap(group, emptyMembers);
        UserDefinedEntitiesProbeConverter converter = new UserDefinedEntitiesProbeConverter();
        DiscoveryResponse response = converter.convertToResponse(groups);
        Assert.assertEquals(0, response.getEntityDTOCount());
    }

    /**
     * Group should not be converted if it has unsupported type.
     */
    @Test
    public void testNotSupportedGroupType() {
        EntityType notSupportedType = EntityType.VIRTUAL_VOLUME;
        Grouping group = createGroup(notSupportedType);
        TopologyEntityDTO member = createMember(1010L, VIRTUAL_MACHINE, "");
        Collection<TopologyEntityDTO> members = Collections.singleton(member);
        Map<Grouping, Collection<TopologyEntityDTO>> groups = Collections.singletonMap(group, members);
        UserDefinedEntitiesProbeConverter converter = new UserDefinedEntitiesProbeConverter();
        DiscoveryResponse response = converter.convertToResponse(groups);
        Assert.assertEquals(0, response.getEntityDTOCount());
    }

    /**
     * Group should not be converted if all members have unsupported type.
     */
    @Test
    public void testNotSupportedMemberType() {
        Grouping group = createGroup(SERVICE);
        EntityType notSupportedType = EntityType.VIRTUAL_VOLUME;
        TopologyEntityDTO member = createMember(1010L, notSupportedType, "");
        Collection<TopologyEntityDTO> members = Collections.singleton(member);
        Map<Grouping, Collection<TopologyEntityDTO>> groups = Collections.singletonMap(group, members);
        UserDefinedEntitiesProbeConverter converter = new UserDefinedEntitiesProbeConverter();
        DiscoveryResponse response = converter.convertToResponse(groups);
        Assert.assertEquals(0, response.getEntityDTOCount());
    }

    /**
     * If a members has no 'vendorId' value, EntityDTO gets ID from TopologyEntityDTO`s OID.
     */
    @Test
    public void emptyVendorIdTest() {
        long memberUid = 1010L;
        String expectedEntityDtoId = String.valueOf(memberUid);
        TopologyEntityDTO member = createMember(memberUid, DATABASE_SERVER, "");
        Map<Grouping, Collection<TopologyEntityDTO>> groups
                = Collections.singletonMap(createGroup(SERVICE), Collections.singleton(member));
        DiscoveryResponse response = new UserDefinedEntitiesProbeConverter().convertToResponse(groups);
        String dtoId = response.getEntityDTOList().stream()
                .map(EntityDTO::getId)
                .filter(id -> id.equals(expectedEntityDtoId))
                .findFirst().orElse(null);
        Assert.assertNotNull(dtoId);
        Assert.assertEquals(expectedEntityDtoId, dtoId);
    }

    /**
     * Group members should be converted to DTOs with origin property 'PROXY'.
     */
    @Test
    public void testIsProxy() {
        long id = 1010L;
        TopologyEntityDTO member = createMember(id, VIRTUAL_MACHINE, "");
        String memberId = String.valueOf(id);
        Map<Grouping, Collection<TopologyEntityDTO>> groups
                = Collections.singletonMap(createGroup(SERVICE), Collections.singleton(member));
        DiscoveryResponse response = new UserDefinedEntitiesProbeConverter().convertToResponse(groups);
        EntityDTO memberDto = response.getEntityDTOList().stream().filter(e -> e.getId().equals(memberId)).findFirst().orElse(null);
        Assert.assertNotNull(memberDto);
        Assert.assertEquals(EntityDTO.EntityOrigin.PROXY, memberDto.getOrigin());
    }

    /**
     * Checks an APPLICATION commodity key which is links group DTO with member DTO.
     */
    @Test
    public void testApplicationCommodityKey() {
        TopologyEntityDTO member = createMember(1010L, VIRTUAL_MACHINE, "");
        Map<Grouping, Collection<TopologyEntityDTO>> groups
                = Collections.singletonMap(createGroup(SERVICE), Collections.singleton(member));
        DiscoveryResponse response = new UserDefinedEntitiesProbeConverter().convertToResponse(groups);
        EntityDTO memberDto = response.getEntityDTOList().stream()
                .filter(e -> e.getEntityType() == VIRTUAL_MACHINE).findFirst().orElse(null);
        EntityDTO groupDto = response.getEntityDTOList().stream()
                .filter(e -> e.getEntityType() == SERVICE).findFirst().orElse(null);
        Assert.assertNotNull(memberDto);
        Assert.assertNotNull(groupDto);
        CommodityDTO memberCommodity = memberDto.getCommoditiesSoldList().stream()
                .filter(c -> APPLICATION == c.getCommodityType()).findFirst().orElse(null);
        CommodityDTO groupCommodity = memberDto.getCommoditiesSoldList().stream()
                .filter(c -> APPLICATION == c.getCommodityType()).findFirst().orElse(null);
        Assert.assertNotNull(memberCommodity);
        Assert.assertNotNull(groupCommodity);
        Assert.assertEquals(memberCommodity.getKey(), groupCommodity.getKey());
    }

    private Grouping createGroup(EntityType entityType) {
        return Grouping.newBuilder()
                .setDefinition(GroupDefinition
                        .newBuilder()
                        .setEntityDefinitionData(EntityDefinitionData
                                .newBuilder()
                                .setDefinedEntityType(entityType)
                                .build())
                        .build())
                .build();
    }

    private TopologyEntityDTO createMember(long oid, EntityType entityType, String vendorId) {
        return TopologyEntityDTO.newBuilder()
                .setEntityType(entityType.getNumber())
                .setOid(oid)
                .setOrigin(Origin
                        .newBuilder()
                        .setDiscoveryOrigin(DiscoveryOrigin
                                .newBuilder()
                                .putDiscoveredTargetData(1L, PerTargetEntityInformation
                                        .newBuilder()
                                        .setVendorId(vendorId)
                                        .buildPartial())
                                .build())
                        .build())
                .build();
    }
}
