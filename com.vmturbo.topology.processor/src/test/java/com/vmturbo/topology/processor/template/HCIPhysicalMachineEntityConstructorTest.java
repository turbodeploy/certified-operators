package com.vmturbo.topology.processor.template;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.charset.Charset;
import java.util.ArrayList;
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
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.protobuf.Internal.EnumLiteMap;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTOOrBuilder;
import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.StorageType;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TargetSpec;
import com.vmturbo.topology.processor.identity.IdentityMetadataMissingException;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.identity.IdentityProviderException;
import com.vmturbo.topology.processor.identity.IdentityUninitializedException;

/**
 * Tests for class HCIPhysicalMachineEntityConstructor.
 */
public class HCIPhysicalMachineEntityConstructorTest {

    private static final long[] OIDS = {11111111111111L, 22222222222222L};
    private static int oidCount = 0;

    private static final IdentityProvider IDENTITY_PROVIDER = new IdentityProvider() {
        @Override
        public long generateTopologyId() {
            return OIDS[oidCount++];
        }

        @Override
        public void restoreDiags(List<String> arg0) throws DiagnosticsException {
        }

        @Override
        public void collectDiags(DiagnosticsAppender arg0) throws DiagnosticsException {
        }

        @Override
        public String getFileName() {
            return null;
        }

        @Override
        public long getTargetId(TargetSpec targetSpec) {
            return 0;
        }

        @Override
        public long getProbeId(ProbeInfo probeInfo) throws IdentityProviderException {
            return 0;
        }

        @Override
        public Map<Long, EntityDTO> getIdsForEntities(long probeId, List<EntityDTO> entityDTOs)
                throws IdentityUninitializedException, IdentityMetadataMissingException,
                IdentityProviderException {
            return null;
        }

        @Override
        public long getCloneId(TopologyEntityDTOOrBuilder inputEntity) {
            return 0;
        }

        @Override
        public long generateOperationId() {
            return 0;
        }

        @Override
        public void updateProbeInfo(ProbeInfo probeInfo) {
        }

        @Override
        public void initialize() throws InitializationException {
        }
    };

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
        Collection<Builder> result = new HCIPhysicalMachineEntityConstructor(template, topology,
                Collections.singletonList(originalHostBuilder), true, IDENTITY_PROVIDER)
                        .createTopologyEntitiesFromTemplate();

        Assert.assertEquals(2, result.size());
        Builder newHost = result.stream()
                .filter(o -> o.getEntityType() == EntityType.PHYSICAL_MACHINE_VALUE).findFirst()
                .get();
        Builder newStorage = result.stream()
                .filter(o -> o.getEntityType() == EntityType.STORAGE_VALUE).findFirst().get();

        Builder originalHost = originalHostBuilder.getEntityBuilder();
        Builder originalStorage = originalStorageBuilder.getEntityBuilder();

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

    private List<TopologyEntityDTO> loadEntities()
            throws IOException, InvalidProtocolBufferException {
        List<TopologyEntityDTO> result = new ArrayList<>();

        String entitiesStr = readResourceFileAsString("HCIPlanEntities.json");
        Type listType = new TypeToken<List<Object>>() {
        }.getType();
        List<Object> entitiesObj = new Gson().fromJson(entitiesStr, listType);

        for (Object o : entitiesObj) {
            String str = new Gson().toJson(o);
            TopologyEntityDTO.Builder builder = TopologyEntityDTO.newBuilder();
            JsonFormat.parser().merge(str, builder);
            result.add(builder.build());
        }

        return result;
    }

    private static void checkMissingSoldCommodity(@Nonnull Builder originalEntity,
            @Nonnull Builder newEntity) {
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
