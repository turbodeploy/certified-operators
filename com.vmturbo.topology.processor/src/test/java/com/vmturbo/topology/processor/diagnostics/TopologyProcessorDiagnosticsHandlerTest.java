package com.vmturbo.topology.processor.diagnostics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.gson.Gson;

import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.common.DiagnosticsWriter;
import com.vmturbo.kvstore.MapKeyValueStore;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.Discovery.AccountDefEntry;
import com.vmturbo.platform.common.dto.Discovery.CustomAccountDefEntry;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.AccountValue;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TargetInfo;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TargetSpec;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.entity.IdentifiedEntityDTO;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.scheduling.Scheduler;
import com.vmturbo.topology.processor.scheduling.TargetDiscoverySchedule;
import com.vmturbo.topology.processor.targets.InvalidTargetException;
import com.vmturbo.topology.processor.targets.KVBackedTargetStore;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetDeserializationException;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * Tests for {@link TopologyProcessorDiagnosticsHandler}.
 *
 */
public class TopologyProcessorDiagnosticsHandlerTest {

    private static final Gson GSON = ComponentGsonFactory.createGsonNoPrettyPrint();

    private final List<Target> targets = Lists.newArrayList();
    private final TargetStore targetStore = Mockito.mock(TargetStore.class);
    private final Scheduler scheduler = Mockito.mock(Scheduler.class);
    private final EntityStore entityStore = Mockito.mock(EntityStore.class);
    private final IdentityProvider identityProvider = Mockito.mock(IdentityProvider.class);
    private final EntityDTO nwDto =
            EntityDTO.newBuilder().setId("NW-1").setEntityType(EntityType.NETWORK).build();

    private final TargetSpec.Builder targetSpecBuilder = TargetSpec.newBuilder()
            .addAccountValue(AccountValue.newBuilder()
                .setKey("targetIdentifier")
                .setStringValue("fieldValue"));

    // With a mock schedule, the Gson response will always be with zero fields
    private static final String SCHEDULE_JSON =
            GSON.toJson(Mockito.mock(TargetDiscoverySchedule.class), TargetDiscoverySchedule.class);

    private static final String IDENTIFIED_ENTITY =
            "{\"oid\":\"199\",\"entity\":"
            + "{\"entityType\":\"NETWORK\",\"id\":\"NW-1\"}}";

    @Before
    public void setup() {
        Mockito.when(targetStore.getAll()).thenReturn(targets);
        Map<Long, EntityDTO> map = ImmutableMap.of(199L, nwDto);
        Mockito.when(entityStore.discoveredByTarget(100001)).thenReturn(map);
        Mockito.when(entityStore.getTargetLastUpdatedTime(100001)).thenReturn(Optional.of(12345L));
    }

    private ZipInputStream dumpDiags() throws IOException {
        ByteArrayOutputStream zipBytes = new ByteArrayOutputStream();
        ZipOutputStream zos = new ZipOutputStream(zipBytes);
        TopologyProcessorDiagnosticsHandler handler =
                new TopologyProcessorDiagnosticsHandler(targetStore, scheduler, entityStore,
                        identityProvider, new DiagnosticsWriter());
        handler.dumpDiags(zos);
        zos.close();
        return new ZipInputStream(new ByteArrayInputStream(zipBytes.toByteArray()));
    }

    @Test
    /**
     * Test case with no targets (and no schedules).
     * The generated diags should be empty.
     *
     * @throws IOException
     */
    public void testNoTargets() throws IOException {
        ZipInputStream zis = dumpDiags();

        ZipEntry ze = zis.getNextEntry();
        assertTrue(ze.getName().equals("Targets.diags"));
        byte[] bytes = new byte[20];
        zis.read(bytes);
        assertEquals(0, bytes[0]); // the entry is empty

        ze = zis.getNextEntry();
        assertTrue(ze.getName().equals("Schedules.diags"));
        bytes = new byte[20];
        zis.read(bytes);
        assertEquals(0, bytes[0]); // the entry is empty

        ze = zis.getNextEntry();
        assertEquals("Identity.diags", ze.getName());

        ze = zis.getNextEntry();
        assertNull(ze);
        zis.close();
    }

    @Test
    public void testTargetSecretFields()
            throws InvalidTargetException, IOException {
        final long targetId = 1;
        final long probeId = 2;
        Mockito.when(entityStore.getTargetLastUpdatedTime(targetId)).thenReturn(Optional.of(12345L));

        // A probe with a secret field.
        final ProbeInfo probeInfo = ProbeInfo.newBuilder()
                .setProbeCategory("cat")
                .setProbeType("type")
                .addTargetIdentifierField("field")
                .addAccountDefinition(AccountDefEntry.newBuilder()
                    .setMandatory(true)
                    .setCustomDefinition(CustomAccountDefEntry.newBuilder()
                        .setName("name")
                        .setDisplayName("displayName")
                        .setDescription("description")
                        .setIsSecret(true)))
                .build();

        // The target spec with an account value for the secret field
        final TargetSpec targetSpec = TargetSpec.newBuilder()
                .setProbeId(probeId)
                .addAccountValue(AccountValue.newBuilder()
                    .setKey("name")
                    .setStringValue("value"))
                .build();

        final ProbeStore probeStore = Mockito.mock(ProbeStore.class);
        Mockito.when(probeStore.getProbe(Mockito.eq(probeId)))
               .thenReturn(Optional.of(probeInfo));
        Mockito.when(scheduler.getDiscoverySchedule(targetId))
                .thenReturn(Optional.empty());

        targets.add(new Target(targetId, probeStore, targetSpec, true));

        final ZipInputStream zis = dumpDiags();

        final ZipEntry ze = zis.getNextEntry();
        assertEquals("Targets.diags", ze.getName());
        byte[] bytes = new byte[1024];
        zis.read(bytes);
        final String targetJson = new String(bytes, 0, 1024).split("\n")[0];
        final TargetInfo savedTargetInfo = GSON.fromJson(targetJson, TargetInfo.class);
        Assert.assertEquals(0, savedTargetInfo.getSpec().getAccountValueCount());
    }

    @Test
    public void testRestoreTargetsInvalidJson()
            throws IOException, TargetDeserializationException, InvalidTargetException {
        final long targetId = 1;
        final TargetInfo validTarget = TargetInfo.newBuilder()
                .setId(targetId)
                .setSpec(targetSpecBuilder.setProbeId(2))
                .build();
        final TopologyProcessorDiagnosticsHandler handler =
                new TopologyProcessorDiagnosticsHandler(targetStore, scheduler, entityStore,
                        identityProvider, new DiagnosticsWriter());
        // Valid json, but not a target info
        final String invalidJsonTarget = GSON.toJson(targetSpecBuilder.setProbeId(3));
        // Invalid json
        final String invalidJson = "{osaeintr";
        final String validJsonTarget = GSON.toJson(validTarget);
        // Put the valid JSON target at the end of the target list, to assure
        // that preceding invalid targets don't halt processing.
        handler.restoreTargets(ImmutableList.of(invalidJsonTarget, invalidJson, validJsonTarget));

        // Verify that we only restored one target.
        Mockito.verify(targetStore, Mockito.times(1)).createTarget(
                Mockito.anyLong(),
                any());
        // Verify that the restored target had the right information.
        Mockito.verify(targetStore).createTarget(
                Mockito.eq(targetId),
                Mockito.eq(validTarget.getSpec()));
    }

    /**
     * Test case with some targets/schedules.
     *
     * @throws IOException
     */
    @Test
    public void testSomeTargets() throws IOException {
        long[] targetIds = new long[]{100001,200001};
        long[] probeIds = new long[]{101,201};
        TargetInfo[] targetInfos = new TargetInfo[]{
                TargetInfo.newBuilder()
                        .setId(targetIds[0])
                        .setSpec(targetSpecBuilder.setProbeId(probeIds[0]))
                        .build(),
                TargetInfo.newBuilder()
                        .setId(targetIds[1])
                        .setSpec(targetSpecBuilder.setProbeId(probeIds[1]))
                        .build(),
        };
        TargetDiscoverySchedule[] schedules = new TargetDiscoverySchedule[]{
                null,
                Mockito.mock(TargetDiscoverySchedule.class)};

        for (int i = 0; i < 2; ++i) {
            targets.add(mockTarget(targetIds[i], targetInfos[i], schedules[i]));
        }

        Mockito.when(entityStore.getTargetLastUpdatedTime(200001)).thenReturn(Optional.of(23456L));
        final ZipInputStream zis = dumpDiags();

        ZipEntry ze = zis.getNextEntry();
        assertEquals("Targets.diags", ze.getName());
        byte[] bytes = new byte[1024];
        zis.read(bytes);
        String firstTargetJson = new String(bytes, 0, 1024).split("\n")[0];
        assertEquals(targetInfos[0], GSON.fromJson(firstTargetJson, TargetInfo.class));

        ze = zis.getNextEntry();
        assertEquals("Schedules.diags", ze.getName());
        bytes = new byte[1024];
        zis.read(bytes);
        assertEquals(SCHEDULE_JSON, new String(bytes, 0, SCHEDULE_JSON.length()));

        ze = zis.getNextEntry();
        assertEquals("Entities." + targetIds[0] + "-12345.diags", ze.getName());
        bytes = new byte[100];
        zis.read(bytes);
        String json = new String(bytes, 0, IDENTIFIED_ENTITY.length());
        assertEquals(IDENTIFIED_ENTITY, json);

        IdentifiedEntityDTO dto = IdentifiedEntityDTO.fromJson(json);
        assertEquals(199, dto.getOid());
        assertEquals(nwDto, dto.getEntity());

        ze = zis.getNextEntry();
        assertEquals("Entities." + targetIds[1] + "-23456.diags", ze.getName());
        bytes = new byte[100];
        zis.read(bytes);
        assertEquals(0, bytes[0]);

        ze = zis.getNextEntry();
        assertEquals("Identity.diags", ze.getName());

        ze = zis.getNextEntry();
        assertNull(ze);
        zis.close();
    }

    @Test
    public void testRestore() throws Exception {
        TargetStore simpleTargetStore =
                new KVBackedTargetStore(
                        new MapKeyValueStore(),
                        Mockito.mock(IdentityProvider.class),
                        Mockito.mock(ProbeStore.class));
        TopologyProcessorDiagnosticsHandler handler =
                new TopologyProcessorDiagnosticsHandler(simpleTargetStore, scheduler, entityStore,
                        identityProvider, new DiagnosticsWriter());

        handler.restore(new FileInputStream(new File(fullPath("diags/compressed/diags0.zip"))));
        List<Target> targets = simpleTargetStore.getAll();
        assertTrue(!targets.isEmpty());

        for (Target target : simpleTargetStore.getAll()) {
            Mockito.verify(scheduler, Mockito.times(1))
                .setDiscoverySchedule(target.getId(), 600000, TimeUnit.MILLISECONDS);
            Mockito.verify(entityStore).entitiesRestored(
                    Mockito.eq(target.getId()),
                    Mockito.anyLong(),
                    Mockito.anyMapOf(Long.class, EntityDTO.class));
        }
        Mockito.verify(identityProvider).restoreDiags(any());
    }

    private Target mockTarget(long targetId,
                              TargetInfo targetInfo,
                              TargetDiscoverySchedule schedule) {
        Target target = Mockito.mock(Target.class);
        Mockito.when(target.getId()).thenReturn(targetId);
        Mockito.when(target.getNoSecretDto()).thenReturn(targetInfo);
        Mockito.when(scheduler.getDiscoverySchedule(targetId))
               .thenReturn(Optional.ofNullable(schedule));
        return target;
    }

    /**
     * Converts a relative (to src/test/resources) path to absolute path.
     * @param fileName file name as relative path
     * @return file name with absolute path
     * @throws URISyntaxException if the path name cannot be parsed properly
     * @throws IOException is I/O error occurs
     */
    private String fullPath(String fileName) throws URISyntaxException, IOException {
        URL fileUrl = this.getClass().getClassLoader().getResources(fileName).nextElement();
        return Paths.get(fileUrl.toURI()).toString();
    }
}
