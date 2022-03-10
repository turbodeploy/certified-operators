package com.vmturbo.topology.processor.template;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
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
import com.google.protobuf.util.JsonFormat;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingPolicyServiceMole;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.api.test.ResourcePath;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.StorageType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.identity.IdentityProvider;

/**
 * Tests for class HCIPhysicalMachineEntityConstructor.
 */
public class HCIPhysicalMachineEntityConstructorTest {

    private static final long[] OIDS = {11111111111111L, 22222222222222L, 33333333333333L};
    private static int oidCount = 0;

    private final IdentityProvider identityProvider = Mockito.mock(IdentityProvider.class);
    private final SettingPolicyServiceMole testSettingPolicyService = spy(
            new SettingPolicyServiceMole());
    private SettingPolicyServiceBlockingStub settingPolicyServiceClient;

    /**
     * GRPC test server.
     */
    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(testSettingPolicyService);

    /**
     * Common setup before every test.
     *
     * @throws IOException error loading settings
     */
    @Before
    public void setup() throws IOException {
        when(identityProvider.generateTopologyId())
                .thenAnswer(invocationOnMock -> OIDS[oidCount++]);
        settingPolicyServiceClient = SettingPolicyServiceGrpc
                .newBlockingStub(grpcServer.getChannel());
        List<SettingPolicy> settings = loadStorageSettingPolicies();
        when(testSettingPolicyService.listSettingPolicies(any())).thenReturn(settings);
    }

    /**
     * Test constructing HCI entities from the HCI template.
     *
     * @throws Exception any test exception
     */
    @Test
    public void testConstructor() throws Exception {
        Template template = loadTemplate("HCITemplate.json");

        TopologyEntity.Builder host1 = TopologyEntity
                .newBuilder(loadTopologyEntityImpl("hp-esx78.eng.vmturbo.com.json"));
        TopologyEntity.Builder host2 = TopologyEntity
                .newBuilder(loadTopologyEntityImpl("hp-esx79.eng.vmturbo.com.json"));
        TopologyEntity.Builder storage = TopologyEntity
                .newBuilder(loadTopologyEntityImpl("HCIStorage.json"));
        host1.addConsumer(storage);
        TopologyEntity.Builder vmVMFS = TopologyEntity
                .newBuilder(loadTopologyEntityImpl("astra-vmfs.json"));
        host1.addConsumer(vmVMFS);
        TopologyEntity.Builder vmVsan1 = TopologyEntity
                .newBuilder(loadTopologyEntityImpl("astra-vsan-esx78.json"));
        host1.addConsumer(vmVsan1);
        TopologyEntity.Builder vmVsan2 = TopologyEntity
                .newBuilder(loadTopologyEntityImpl("astra-vsan-esx79.json"));
        host2.addConsumer(vmVsan2);
        host2.addConsumer(storage);

        List<TopologyEntity.Builder> hosts = Arrays.asList(host1, host1);

        Map<Long, TopologyEntity.Builder> topology = new HashMap<>();
        topology.put(host1.getOid(), host1);
        topology.put(host2.getOid(), host2);
        topology.put(storage.getOid(), storage);

        Set<Long> hostProviderOids = new HashSet<>();

        for (int i = 1; i <= 5; i++) {
            TopologyEntity.Builder hostProvider = TopologyEntity
                    .newBuilder(loadTopologyEntityImpl("HostProvider" + i + ".json"));
            topology.put(hostProvider.getOid(), hostProvider);
            hostProviderOids.add(hostProvider.getOid());
        }

        // Run test
        Collection<TopologyEntityImpl> result = new HCIPhysicalMachineEntityConstructor(
                template, topology, identityProvider, settingPolicyServiceClient)
                        .replaceEntitiesFromTemplate(Collections.singletonList(host1));

        Assert.assertEquals(3, result.size());
        List<TopologyEntityImpl> newHosts = result.stream()
                .filter(o -> o.getEntityType() == EntityType.PHYSICAL_MACHINE_VALUE)
                .collect(Collectors.toList());
        TopologyEntityImpl newStorage = result.stream()
                .filter(o -> o.getEntityType() == EntityType.STORAGE_VALUE).findFirst().get();

        TopologyEntityImpl originalHost = host1.getTopologyEntityImpl();
        TopologyEntityImpl originalStorage = storage.getTopologyEntityImpl();

        List<Long> hostOids = newHosts.stream().map(h -> h.getOid()).collect(Collectors.toList());

        // Check the original entities for modifications
        Assert.assertTrue(
                hostOids.contains(originalHost.getEdit().getReplaced().getReplacementId()));
        Assert.assertEquals(newStorage.getOid(),
                originalStorage.getEdit().getReplaced().getReplacementId());

        // Check vSAN info
        Assert.assertEquals(StorageType.VSAN,
                newStorage.getTypeSpecificInfo().getStorage().getStorageType());

        // Check if the provider storages are marked for replacement
        for (TopologyEntity.Builder host : hosts) {
            for (Long oid : host.getProviderIds()) {
                TopologyEntity.Builder provider = topology.get(oid);

                if (provider.getEntityType() == EntityType.STORAGE_VALUE) {
                    Assert.assertEquals(newStorage.getOid(),
                            provider.getTopologyEntityImpl().getEdit().getReplaced().getReplacementId());
                }
            }
        }
    }

    @Nonnull
    private Template loadTemplate(@Nonnull String jsonFileName) throws IOException {
        String str = readResourceFileAsString(jsonFileName);
        Template.Builder builder = Template.newBuilder();
        JsonFormat.parser().merge(str, builder);

        return builder.build();
    }

    @Nonnull
    private TopologyEntityImpl loadTopologyEntityImpl(@Nonnull String jsonFileName)
            throws IOException {
        return TopologyEntityImpl.fromProto(loadTopologyEntityDTO(jsonFileName));
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
    private List<SettingPolicy> loadStorageSettingPolicies()
            throws IOException {
        String str = readResourceFileAsString("StorageSettingPolicy.json");
        SettingPolicy.Builder builder = SettingPolicy.newBuilder();
        JsonFormat.parser().merge(str, builder);

        return Collections.singletonList(builder.build());
    }

    @Nonnull
    private String readResourceFileAsString(@Nonnull String fileName) throws IOException {
        File file = ResourcePath.getTestResource(getClass(), "template/" + fileName).toFile();
        return Files.asCharSource(file, Charset.defaultCharset()).read();
    }
}
