package com.vmturbo.topology.processor.template;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.io.Files;
import com.google.protobuf.Internal.EnumLiteMap;
import com.google.protobuf.util.JsonFormat;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.StorageType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.identity.IdentityProvider;

/**
 * Tests for class HCIPhysicalMachineEntityConstructor.
 */
public class HCIPhysicalMachineEntityConstructorTest {

    private static final long[] OIDS = {11111111111111L, 22222222222222L};
    private static int oidCount = 0;

    private final IdentityProvider identityProvider = mock(IdentityProvider.class);

    /**
     * Common setup before every test.
     */
    @Before
    public void setup() {
        when(identityProvider.generateTopologyId()).thenAnswer(invocationOnMock -> OIDS[oidCount++]);
    }

    /**
     * Test constructing HCI entities from the HCI template.
     *
     * @throws Exception any test exception
     */
    @Test
    public void testConstructor() throws Exception {
        Template template = loadTemplate("HCITemplate.json");

        TopologyEntity.Builder originalHostBuilder = TopologyEntity
                .newBuilder(loadTopologyEntityDTO("HCIHost.json").toBuilder());
        TopologyEntity.Builder originalStorageBuilder = TopologyEntity
                .newBuilder(loadTopologyEntityDTO("HCIStorage.json").toBuilder());
        originalHostBuilder.addConsumer(originalStorageBuilder);
        TopologyEntity.Builder vmVMFS = TopologyEntity
                .newBuilder(loadTopologyEntityDTO("VmVMFS.json").toBuilder());
        originalHostBuilder.addConsumer(vmVMFS);
        TopologyEntity.Builder vmVsan = TopologyEntity
                .newBuilder(loadTopologyEntityDTO("VmVsan.json").toBuilder());
        originalHostBuilder.addConsumer(vmVsan);

        Map<Long, TopologyEntity.Builder> topology = new HashMap<>();
        topology.put(originalHostBuilder.getOid(), originalHostBuilder);
        topology.put(originalStorageBuilder.getOid(), originalStorageBuilder);

        Set<Long> hostProviderOids = new HashSet<>();

        for (int i = 1; i <= 5; i++) {
            TopologyEntity.Builder hostProvider = TopologyEntity
                    .newBuilder(loadTopologyEntityDTO("HostProvider" + i + ".json").toBuilder());
            topology.put(hostProvider.getOid(), hostProvider);
            hostProviderOids.add(hostProvider.getOid());
        }

        // Run test
        Collection<TopologyEntityDTO.Builder> result = new HCIPhysicalMachineEntityConstructor(
                template, topology, Collections.singletonList(originalHostBuilder), true,
                identityProvider).createTopologyEntitiesFromTemplate();

        Assert.assertEquals(2, result.size());
        TopologyEntityDTO.Builder newHost = result.stream()
                .filter(o -> o.getEntityType() == EntityType.PHYSICAL_MACHINE_VALUE).findFirst()
                .get();
        TopologyEntityDTO.Builder newStorage = result.stream()
                .filter(o -> o.getEntityType() == EntityType.STORAGE_VALUE).findFirst().get();

        TopologyEntityDTO.Builder originalHost = originalHostBuilder.getEntityBuilder();
        TopologyEntityDTO.Builder originalStorage = originalStorageBuilder.getEntityBuilder();

        // Check the original entities for modifications
        Assert.assertEquals(newHost.getOid(),
                originalHost.getEdit().getReplaced().getReplacementId());
        Assert.assertEquals(newStorage.getOid(),
                originalStorage.getEdit().getReplaced().getReplacementId());

        // New entities should not have any references to the old oids.
        Assert.assertFalse(hasCommodityByAccessOid(newHost, originalStorage.getOid()));
        Assert.assertFalse(hasCommodityByAccessOid(newStorage, originalHost.getOid()));

        // New entities should have references to the new oids.
        Assert.assertTrue(hasCommodityByAccessOid(newHost, newStorage.getOid()));
        Assert.assertTrue(hasCommodityByAccessOid(newStorage, newHost.getOid()));

        // Check vSAN info
        Assert.assertEquals(StorageType.VSAN,
                newStorage.getTypeSpecificInfo().getStorage().getStorageType());

        // Compare sold commodities
        checkMissingSoldCommodity(originalHost, newHost);
        checkMissingSoldCommodity(originalStorage, newStorage);

        // Check the Storage bought commodities
        List<CommoditiesBoughtFromProvider> boughts = newStorage
                .getCommoditiesBoughtFromProvidersList().stream()
                .filter(b -> b.getProviderId() == newHost.getOid()).collect(Collectors.toList());
        Assert.assertEquals(1, boughts.size());
        boughts.get(0).getCommodityBoughtList().forEach(comm -> {
            Assert.assertTrue(printCommBought(comm), comm.getUsed() > 0);
        });

        // Check if the original host storages are marked for replacement
        for (Long oid : hostProviderOids) {
            TopologyEntity.Builder provider = topology.get(oid);

            if (provider.getEntityType() == EntityType.STORAGE_VALUE) {
                Assert.assertEquals(newStorage.getOid(),
                        provider.getEntityBuilder().getEdit().getReplaced().getReplacementId());
            }
        }
    }

    private static void checkMissingSoldCommodity(@Nonnull TopologyEntityDTO.Builder originalEntity,
            @Nonnull TopologyEntityDTO.Builder newEntity) {
        for (CommoditySoldDTO commOrig : originalEntity.getCommoditySoldListList()) {
            List<CommoditySoldDTO> comms = newEntity.getCommoditySoldListList().stream()
                    .filter(commNew -> commOrig.getCommodityType().getType() == commNew
                            .getCommodityType().getType()
                            && commOrig.getCommodityType().getKey() == commNew.getCommodityType()
                                    .getKey())
                    .collect(Collectors.toList());

            Assert.assertTrue(printComm(commOrig), comms.size() == 1);
        }
    }

    private static String printComm(CommoditySoldDTO comm) {
        return "[" + comm.getCommodityType().getType() + "-"
                + getCommodityById(comm.getCommodityType()) + ", key: '"
                + comm.getCommodityType().getKey() + "']";
    }

    private static String printCommBought(CommodityBoughtDTO comm) {
        return "[" + comm.getCommodityType().getType() + "-"
                + getCommodityById(comm.getCommodityType()) + ", key: '"
                + comm.getCommodityType().getKey() + "']";
    }

    private static CommodityDTO.CommodityType getCommodityById(
            TopologyDTO.CommodityType commodityType) {
        EnumLiteMap<CommodityType> s = CommodityType.internalGetValueMap();
        return s.findValueByNumber(commodityType.getType());
    }

    private static boolean hasCommodityByAccessOid(TopologyEntityDTO.Builder entity, long oid) {
        return entity.getCommoditySoldListBuilderList().stream()
                .filter(comm -> comm.getAccesses() == oid).findAny().isPresent();
    }

    @Nonnull
    private Template loadTemplate(@Nonnull String jsonFileName) throws IOException {
        String str = readResourceFileAsString(jsonFileName);
        Template.Builder builder = Template.newBuilder();
        JsonFormat.parser().merge(str, builder);

        return builder.build();
    }

    @Nonnull
    private TopologyEntityDTO loadTopologyEntityDTO(@Nonnull String jsonFileName)
            throws IOException {
        String str = readResourceFileAsString(jsonFileName);
        TopologyEntityDTO.Builder builder = TopologyEntityDTO.newBuilder();
        JsonFormat.parser().merge(str, builder);

        return builder.build();
    }

    @Nonnull
    private String readResourceFileAsString(@Nonnull String fileName) throws IOException {
        String path = getClass().getClassLoader().getResource("template/" + fileName).getFile();
        return Files.asCharSource(new File(path), Charset.defaultCharset()).read();
    }
}
