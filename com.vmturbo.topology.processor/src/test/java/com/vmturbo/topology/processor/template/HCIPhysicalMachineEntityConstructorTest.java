package com.vmturbo.topology.processor.template;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.google.common.io.Files;
import com.google.protobuf.util.JsonFormat;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTOOrBuilder;
import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
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

    private static long oid = 1;

    private static final IdentityProvider IDENTITY_PROVIDER = new IdentityProvider() {
        @Override
        public long generateTopologyId() {
            return oid++;
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
    };

    /**
     * Test constructing HCI entities from the HCI template.
     *
     * @throws Exception any test exception
     */
    @Test
    public void testConstructor() throws Exception {
        Template template = loadTemplate("HCITemplate.json");
        TopologyEntityDTO hostDto = loadTopologyEntityDTO("HCIHost.json");
        TopologyEntityDTO storageDto = loadTopologyEntityDTO("HCIStorage.json");

        TopologyEntity.Builder originalHost = TopologyEntity.newBuilder(hostDto.toBuilder());
        TopologyEntity.Builder originalStorage = TopologyEntity.newBuilder(storageDto.toBuilder());
        originalHost.addConsumer(originalStorage);

        Map<Long, TopologyEntity.Builder> topology = new HashMap<>();
        topology.put(originalHost.getOid(), originalHost);
        topology.put(originalStorage.getOid(), originalStorage);

        // Run test
        Collection<Builder> result = new HCIPhysicalMachineEntityConstructor()
                .createTopologyEntityFromTemplate(template, topology, Optional.of(originalHost),
                        true, IDENTITY_PROVIDER);

        Assert.assertEquals(2, result.size());
        Builder newHost = result.stream()
                .filter(o -> o.getEntityType() == EntityType.PHYSICAL_MACHINE_VALUE).findFirst()
                .get();
        Builder newStorage = result.stream()
                .filter(o -> o.getEntityType() == EntityType.STORAGE_VALUE).findFirst().get();

        // Check the original entities for modifications
        Assert.assertEquals(newHost.getOid(),
                originalHost.getEntityBuilder().getEdit().getReplaced().getReplacementId());
        Assert.assertEquals(newStorage.getOid(),
                originalStorage.getEntityBuilder().getEdit().getReplaced().getReplacementId());

        // Check the commodities
        // TODO OM-56990 implement the checks

        Assert.assertNotNull(result);
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
        String path = getClass().getClassLoader().getResource(fileName).getFile();
        return Files.asCharSource(new File(path), Charset.defaultCharset()).read();
    }

}
