package com.vmturbo.topology.processor.conversions;

import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.RESPONSE_TIME;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.APPLICATION;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.APPLICATION_COMPONENT;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.APPLICATION_SERVER;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.VIRTUAL_MACHINE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import com.google.protobuf.TextFormat;
import com.google.protobuf.util.JsonFormat;

import org.junit.Test;

import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;

/**
 * Tests AppComponentConverter class.
 */
public class AppComponentConverterTest {

    private static final String APP_ID = "Iremel-Cluster-App [Iremel-Cluster-App, appinsights-cluster-b]";

    /**
     * Tests converting response from old format to new.
     * @throws IOException error reading file
     */
    @Test
    public void convertResponseTest() throws IOException {
        DiscoveryResponse response = null;
        try (InputStream inputStream = AppComponentConverterTest.class
                .getClassLoader().getResource("protobuf/messages/apm_old_format_data.txt").openStream()) {
            final DiscoveryResponse.Builder builder = DiscoveryResponse.newBuilder();
            InputStreamReader reader = new InputStreamReader(inputStream, UTF_8);
            TextFormat.getParser().merge(reader, builder);
            response = builder.build();
            DiscoveryResponse newResponse = new AppComponentConverter().convertResponse(response);
            assertEquals(1, response.getEntityDTOList().stream().filter(e -> e.getEntityType().equals(APPLICATION)).count());
            assertEquals(4, response.getEntityDTOList().stream().filter(e -> e.getEntityType().equals(APPLICATION_SERVER)).count());
            assertEquals(0, newResponse.getEntityDTOList().stream().filter(e -> e.getEntityType().equals(APPLICATION)).count());
            assertEquals(0, newResponse.getEntityDTOList().stream().filter(e -> e.getEntityType().equals(APPLICATION_SERVER)).count());
            assertEquals(5, newResponse.getEntityDTOList().stream().filter(e -> e.getEntityType().equals(APPLICATION_COMPONENT)).count());
            assertEquals(5, newResponse.getEntityDTOList().stream().filter(e -> e.getEntityType().equals(VIRTUAL_MACHINE)).count());
            assertEquals(1, newResponse.getDiscoveredGroupList().size());
            CommonDTO.EntityDTO appComp = newResponse.getEntityDTOList()
                    .stream()
                    .filter(e -> e.getId().equals(APP_ID))
                    .findFirst()
                    .get();
            assertEquals(2, appComp.getCommoditiesSoldList().size());
            assertEquals(1, appComp.getCommoditiesBoughtList().size());
            assertEquals(2, appComp.getCommoditiesBought(0).getBoughtList().size());
            assertEquals(4, appComp.getEntityPropertiesCount());
            CommonDTO.CommodityDTO comm = appComp.getCommoditiesSoldList()
                    .stream()
                    .filter(c -> c.getCommodityType().equals(RESPONSE_TIME))
                    .findFirst()
                    .get();
            assertEquals(1.75, comm.getUsed(), 0.01);
            assertEquals(4.0, comm.getPeak(), 0.01);
        }
    }

    /**
     * Tests converting response without entities (should return original response).
     * @throws IOException error reading file
     */
    @Test
    public void convertProbeInfo() throws IOException {
        try (InputStream inputStream = AppComponentConverterTest.class
                .getClassLoader().getResource("protobuf/messages/probe_info_conv_test.json").openStream()) {
            final ProbeInfo.Builder builder = ProbeInfo.newBuilder();
            InputStreamReader reader = new InputStreamReader(inputStream, UTF_8);
            JsonFormat.parser().merge(reader, builder);
            ProbeInfo probeInfo = new AppComponentConverter().convertProbeInfo(builder.build());
            assertTrue(probeInfo.getEntityMetadataList().stream()
                    .noneMatch(e -> e.getEntityType() == APPLICATION));
            assertTrue(probeInfo.getEntityMetadataList().stream()
                    .noneMatch(e -> e.getEntityType() == APPLICATION_SERVER));
            assertEquals(1, probeInfo.getEntityMetadataList().stream()
                    .filter(e -> e.getEntityType() == APPLICATION_COMPONENT)
                    .count());
            assertTrue(probeInfo.getSupplyChainDefinitionSetList()
                    .stream()
                    .noneMatch(e -> e.getTemplateClass() == APPLICATION));
            assertTrue(probeInfo.getSupplyChainDefinitionSetList()
                    .stream()
                    .noneMatch(e -> e.getTemplateClass() == APPLICATION_SERVER));
            assertEquals(1, probeInfo.getSupplyChainDefinitionSetList()
                    .stream()
                    .filter(e -> e.getTemplateClass() == APPLICATION_COMPONENT)
                    .count());
            assertTrue(probeInfo.getSupplyChainDefinitionSetList()
                    .stream()
                    .noneMatch(e -> e.getExternalLinkList().stream().anyMatch(c -> c.hasValue()
                            && c.getValue().getBuyerRef() == APPLICATION)));
            assertTrue(probeInfo.getSupplyChainDefinitionSetList()
                    .stream()
                    .noneMatch(e -> e.getExternalLinkList().stream().anyMatch(c -> c.hasValue()
                            && c.getValue().getBuyerRef() == APPLICATION_SERVER)));
            assertTrue(probeInfo.getSupplyChainDefinitionSetList()
                    .stream()
                    .anyMatch(e -> e.getExternalLinkList().stream().anyMatch(c -> c.hasValue()
                            && c.getValue().getBuyerRef() == APPLICATION_COMPONENT)));
            assertTrue(probeInfo.getSupplyChainDefinitionSetList()
                    .stream()
                    .noneMatch(e -> e.getCommodityBoughtList().stream().anyMatch(c -> c.hasKey()
                            && c.getKey().getTemplateClass() == APPLICATION)));
            assertTrue(probeInfo.getSupplyChainDefinitionSetList()
                    .stream()
                    .noneMatch(e -> e.getCommodityBoughtList().stream().anyMatch(c -> c.hasKey()
                            && c.getKey().getTemplateClass() == APPLICATION_SERVER)));
            assertTrue(probeInfo.getSupplyChainDefinitionSetList()
                    .stream()
                    .anyMatch(e -> e.getCommodityBoughtList().stream().anyMatch(c -> c.hasKey()
                            && c.getKey().getTemplateClass() == APPLICATION_COMPONENT)));
        }
    }

    /**
     * Tests converting response without entities (should return original response).
     * @throws IOException error reading file
     */
    @Test
    public void convertEntityFreeTest() throws IOException {
        DiscoveryResponse response = null;
        try (InputStream inputStream = AppComponentConverterTest.class
                .getClassLoader().getResource("protobuf/messages/aws_cost_shrink_data.txt").openStream()) {
            final DiscoveryResponse.Builder builder = DiscoveryResponse.newBuilder();
            InputStreamReader reader = new InputStreamReader(inputStream, UTF_8);
            TextFormat.getParser().merge(reader, builder);
            response = builder.build();
            DiscoveryResponse newResponse = new AppComponentConverter().convertResponse(response);
            assertSame(response, newResponse);
        }
    }

    /**
     * Tests converting response without apps/app servers (should return original response).
     * @throws IOException error reading file
     */
    @Test
    public void convertAppFreeTest() throws IOException {
        DiscoveryResponse response = null;
        try (InputStream inputStream = AppComponentConverterTest.class
                .getClassLoader().getResource("protobuf/messages/app_free_data.txt").openStream()) {
            final DiscoveryResponse.Builder builder = DiscoveryResponse.newBuilder();
            InputStreamReader reader = new InputStreamReader(inputStream, UTF_8);
            TextFormat.getParser().merge(reader, builder);
            response = builder.build();
            DiscoveryResponse newResponse = new AppComponentConverter().convertResponse(response);
            assertSame(response, newResponse);
        }
    }
}