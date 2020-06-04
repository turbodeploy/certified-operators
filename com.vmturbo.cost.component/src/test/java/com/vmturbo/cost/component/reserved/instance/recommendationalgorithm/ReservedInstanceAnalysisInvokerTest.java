package com.vmturbo.cost.component.reserved.instance.recommendationalgorithm;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.context.annotation.Bean;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import reactor.core.publisher.Flux;

import com.vmturbo.common.protobuf.cost.Cost.StartBuyRIAnalysisRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTOMoles.RepositoryServiceMole;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingServiceMole;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.cost.component.pricing.BusinessAccountPriceTableKeyStore;
import com.vmturbo.cost.component.pricing.SQLPriceTableStore;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceBoughtStore;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceBoughtStore.ReservedInstanceBoughtChangeType;

/**
 * Class to unit test ReservedInstanceAnalysisInvoker.
 */
public class ReservedInstanceAnalysisInvokerTest {
    private final Logger logger = LogManager.getLogger();

    BusinessAccountPriceTableKeyStore store = Mockito.mock(BusinessAccountPriceTableKeyStore.class);
    SQLPriceTableStore prTabStore = Mockito.mock(SQLPriceTableStore.class);

    /**
     * Unit test method for testing ReservedInstanceAnalysisInvoker::getNewBusinessAccountsWithCost.
     */
    @Test
    public void testGetNewBusinessAccountsWithCost() {
        Set<ImmutablePair<Long, String>> allBusinessAccounts = Sets.newHashSet(
                ImmutablePair.of(1L, "Acc A"),
                ImmutablePair.of(2L, "Acc B"),
                ImmutablePair.of(3L, "Acc C"));

        // Map of BA OID to Price Table OID.
        final Map<Long, Long> allBusinessAccountsWithCost = new HashMap<>();
        allBusinessAccountsWithCost.put(1L, 10001L);
        allBusinessAccountsWithCost.put(2L, 20002L);
        allBusinessAccountsWithCost.put(3L, 30003L);
        Mockito.when(store.fetchPriceTableKeyOidsByBusinessAccount(
                allBusinessAccounts.stream().map(p -> p.left).collect(Collectors.toSet())))
                        .thenReturn(allBusinessAccountsWithCost);

        ReservedInstanceAnalysisInvoker invoker = getReservedInstanceAnalysisInvoker();

        logger.info("Add BAs with Cost {}", allBusinessAccounts);
        boolean result = invoker.addNewBAsWithCost(allBusinessAccounts);
        assertEquals(true, result);

        // A new business account is added but has no cost.
        allBusinessAccounts.add(ImmutablePair.of(4L, "Acc D"));

        HashSet<Long> newBa = new HashSet<>();
        newBa.add(4L);
        Mockito.when(store.fetchPriceTableKeyOidsByBusinessAccount(newBa))
                        .thenReturn(new HashMap<>());

        logger.info("Add new BA *w/o* Cost {}", allBusinessAccounts);
        result = invoker.addNewBAsWithCost(allBusinessAccounts);
        assertEquals(false, result);

        // Now costs are also available for the new business account.
        allBusinessAccountsWithCost.put(4L, 40004L);

        final Map<Long, Long> newBaWithCost = new HashMap<>();
        newBaWithCost.put(4L, 40004L);
        Mockito.when(store.fetchPriceTableKeyOidsByBusinessAccount(newBa))
                .thenReturn(newBaWithCost);

        logger.info("Add new BA *with* Cost {}", allBusinessAccounts);
        result = invoker.addNewBAsWithCost(allBusinessAccounts);
        assertEquals(true, result);

        // Now test removal of BA:
        // Use case: target deleted or account doesn't exist anymore in the target and is not discovered going forward.
        logger.info("Delete stale BA *with* Cost from 'All Business Accounts' - {}", allBusinessAccounts);
        assertTrue("Failed to remove BA with cost from 'allBusinessAccounts' collection.",
                allBusinessAccounts.remove(ImmutablePair.of(2L, "Acc B")));
        result = invoker.rmObsoleteBAs(allBusinessAccounts);
        assertEquals(true, result);

        logger.info("Call 'Remove Obsolete BAs' again on same BAs - {}", allBusinessAccounts);
        result = invoker.rmObsoleteBAs(allBusinessAccounts);
        assertEquals(false, result);
    }

    @Test
    public void testOnRIInventoryUpdated() {
        // Scenario where the StartBuyRIAnalysisRequest doesn't contain any entities (to simulate the
        // case where the repository doesn't have any entities). In this case the RI Buy Analysis will
        // not be invoked.
        ReservedInstanceAnalysisInvoker invoker = spy(getReservedInstanceAnalysisInvoker());
        StartBuyRIAnalysisRequest startBuyRIAnalysisRequest = StartBuyRIAnalysisRequest.newBuilder().build();
        doReturn(startBuyRIAnalysisRequest).when(invoker).getStartBuyRIAnalysisRequest();
        invoker.onRIInventoryUpdated(ReservedInstanceBoughtChangeType.UPDATED);
        verify(invoker, never()).invokeBuyRIAnalysis(startBuyRIAnalysisRequest);

        // Scenario where the BA's are present in the repository. In this case the RI Buy Analysis
        // will be invoked.
        startBuyRIAnalysisRequest = StartBuyRIAnalysisRequest.newBuilder()
                .addAllAccounts(Lists.newArrayList(1L, 2L, 3L)).build();
        doReturn(startBuyRIAnalysisRequest).when(invoker).getStartBuyRIAnalysisRequest();
        Mockito.doNothing().when(invoker).invokeBuyRIAnalysis(startBuyRIAnalysisRequest);
        invoker.onRIInventoryUpdated(ReservedInstanceBoughtChangeType.UPDATED);
        verify(invoker).invokeBuyRIAnalysis(startBuyRIAnalysisRequest);
    }

    private ReservedInstanceAnalysisInvoker getReservedInstanceAnalysisInvoker() {
        final ReservedInstanceBoughtStore riBoughtStore = Mockito.mock(ReservedInstanceBoughtStore.class);
        final Flux<ReservedInstanceBoughtChangeType> updateEventStream = Flux.empty();
        Mockito.when(riBoughtStore.getUpdateEventStream()).thenReturn(updateEventStream);

        ReservedInstanceAnalysisInvoker invoker = new
                ReservedInstanceAnalysisInvoker(Mockito.mock(ReservedInstanceAnalyzer.class),
                repositoryRpcService(),
                settingsRpcService(),
                riBoughtStore,
                store,
                prTabStore,
                1);

        return invoker;
    }

    @Bean
    public GrpcTestServer grpcTestServer() {
        try {
            final GrpcTestServer testServer = GrpcTestServer.newServer(settingServiceMole(),
                    repositoryService());
            testServer.start();
            return testServer;
        } catch (IOException e) {
            throw new BeanCreationException("Failed to create test channel", e);
        }
    }

    @Bean
    public RepositoryServiceBlockingStub repositoryRpcService() {
        return RepositoryServiceGrpc.newBlockingStub(grpcTestServer().getChannel());
    }

    @Bean
    public SettingServiceBlockingStub settingsRpcService() {
        return SettingServiceGrpc.newBlockingStub(grpcTestServer().getChannel());
    }

    @Bean
    public SettingServiceMole settingServiceMole() {
        return spy(new SettingServiceMole());
    }

    @Bean
    public RepositoryServiceMole repositoryService() {
        return spy(new RepositoryServiceMole());
    }
}
