package com.vmturbo.voltron;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import com.vmturbo.action.orchestrator.api.impl.ActionOrchestratorNotificationReceiver;
import com.vmturbo.action.orchestrator.dto.ActionMessages.ActionOrchestratorNotification;
import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.search.Search.SearchEntitiesRequest;
import com.vmturbo.common.protobuf.search.Search.SearchQuery;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.Type;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.external.api.SupplyChainsApi;
import com.vmturbo.external.api.model.SupplychainApiDTO;
import com.vmturbo.voltron.VoltronConfiguration.MediationComponent;

/**
 * This test is here for illustration purposes, to show how to use the Voltron configuration
 * rule to create your own tests.
 */
public class SampleVoltronTest {

    private static final Logger logger = LogManager.getLogger();

    /**
     * Rule to set up Voltron before the class runs.
     */
    @ClassRule
    public static VoltronRule voltron = new VoltronRule(VoltronConfiguration.newBuilder()
            .addPlatformComponents()
            .addMediationComponent(MediationComponent.MEDIATION_VC)
            .setLicensePath("absolute path to some license XML file")
            .build());

    /**
     * Example test.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    @Ignore
    public void testVoltronSample() throws Exception {
        VoltronsContainer container = voltron.getVoltronsContainer();
        // Load some topology processor zip file.
        container.loadTopology("absolute path to some topology processor zip file");

        // Set up a listener to wait for actions to become available.
        final CompletableFuture<Void> actionsAvailable = new CompletableFuture<>();
        final IMessageReceiver<ActionOrchestratorNotification>
                aoListener = container.getMessageReceiver(ActionOrchestratorNotificationReceiver.ACTIONS_TOPIC);
        aoListener.addListener(((actionOrchestratorNotification, runnable, spanContext) -> {
            if (actionOrchestratorNotification.hasActionsUpdated()) {
                actionsAvailable.complete(null);
            }
        }));

        // Broadcast topology.
        container.broadcastTopology();

        // Wait for actions to be available.
        actionsAvailable.get(10, TimeUnit.MINUTES);

        // Example of using gRPC to call a component's service.
        final SearchServiceBlockingStub search = SearchServiceGrpc.newBlockingStub(container.getGrpcChannel());
        final List<TopologyEntityDTO> pms = new ArrayList<>();
        RepositoryDTOUtil.topologyEntityStream(search.searchEntitiesStream(SearchEntitiesRequest.newBuilder()
                .setReturnType(Type.FULL)
                .setSearch(SearchQuery.newBuilder()
                        .addSearchParameters(SearchProtoUtil.makeSearchParameters(SearchProtoUtil.entityTypeFilter(
                                ApiEntityType.PHYSICAL_MACHINE)))
                        .build())
                .build())).map(p -> p.getFullEntity())
                .forEach(pms::add);
        logger.info("These are the PMs: " + pms.stream()
                .map(TopologyEntityDTO::getDisplayName)
                .collect(Collectors.joining(",")));

        // Example of using external API stubs.
        SupplyChainsApi spApi = container.getApiClient().getStub(SupplyChainsApi.class);
        SupplychainApiDTO activeEntities = spApi.getSupplyChainByUuids(Collections.singletonList("Market"),
                false,
                null,
                Collections.singletonList("ACTIVE"), null, null, null);
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectWriter writer = objectMapper.writerWithDefaultPrettyPrinter();
        logger.info("This is the supply chain: \n" + writer.writeValueAsString(activeEntities));
    }

}
