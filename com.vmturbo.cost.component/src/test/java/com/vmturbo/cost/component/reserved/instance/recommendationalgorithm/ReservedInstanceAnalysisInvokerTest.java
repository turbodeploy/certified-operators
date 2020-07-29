package com.vmturbo.cost.component.reserved.instance.recommendationalgorithm;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.Collections;
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
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;

import reactor.core.publisher.Flux;

import com.vmturbo.common.protobuf.cost.Cost.StartBuyRIAnalysisRequest;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTableKey;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTableKey.Builder;
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
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.ReservedInstanceAnalysisInvoker.BizAccPriceRecord;

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
    public void testGetNewBusinessAccountsWithCost() throws InvalidProtocolBufferException {
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

        Map<Long, PriceTableKey> prTabOidToTabKey = Maps.newHashMap();
        Builder priceTableKeyBuilder = PriceTableKey.newBuilder();
        final String probeKeyMaterialString = "{" +
                "  \"probeKeyMaterial\": {" +
                "    \"ENROLLMENT_NUMBER\": \"\"," +
                "    \"OFFER_ID\": \"MS-AZR-0003P\"" +
                "  }," +
                "  \"serviceProviderId\": \"73494941922466\"" +
                "}";
        JsonFormat.parser().merge(probeKeyMaterialString, priceTableKeyBuilder);
        PriceTableKey prKey = priceTableKeyBuilder.build();
        prTabOidToTabKey.put(10001L, prKey);
        prTabOidToTabKey.put(20002L, prKey);
        prTabOidToTabKey.put(30003L, prKey);
        Mockito.when(prTabStore.getPriceTableKeys(any())).thenReturn(prTabOidToTabKey);

        Map<PriceTableKey, Long> prTabkeyToChkSum = Maps.newHashMap();
        prTabkeyToChkSum.put(prKey, 5501L);
        Mockito.when(prTabStore.getChecksumByPriceTableKeys(any())).thenReturn(prTabkeyToChkSum);

        ReservedInstanceAnalysisInvoker invoker = getReservedInstanceAnalysisInvoker();

        logger.info("Add BAs with Cost {}", allBusinessAccounts);
        int result = invoker.addNewBAsWithCost(allBusinessAccounts);
        assertEquals(3, result);

        // A new business account is added but has no cost.
        allBusinessAccounts.add(ImmutablePair.of(4L, "Acc D"));

        HashSet<Long> newBa = new HashSet<>();
        newBa.add(4L);
        Mockito.when(store.fetchPriceTableKeyOidsByBusinessAccount(newBa))
                .thenReturn(new HashMap<>());

        logger.info("Add new BA __w/o__ Cost {}", allBusinessAccounts);
        result = invoker.addNewBAsWithCost(allBusinessAccounts);
        assertEquals(0, result);

        // Now costs are also available for the new business account.
        allBusinessAccountsWithCost.put(4L, 40004L);

        final Map<Long, Long> newBaWithCost = new HashMap<>();
        newBaWithCost.put(4L, 40004L);
        Mockito.when(store.fetchPriceTableKeyOidsByBusinessAccount(newBa))
                .thenReturn(newBaWithCost);
        prTabOidToTabKey.put(40004L, prKey);
        Mockito.when(prTabStore.getPriceTableKeys(any())).thenReturn(prTabOidToTabKey);

        logger.info("------Add new BA __with__ Cost------\nallBusinessAccounts: {}",
                allBusinessAccounts);
        result = invoker.addNewBAsWithCost(allBusinessAccounts);
        assertEquals(1, result);

        // Now test removal of BA:
        // Use case: target deleted or account doesn't exist in the target and is not discovered anymore.
        logger.info("Delete stale BA *with* Cost from 'All Business Accounts' - {}", allBusinessAccounts);
        assertTrue("Failed to remove BA with cost from 'allBusinessAccounts' collection.",
                allBusinessAccounts.remove(ImmutablePair.of(2L, "Acc B")));
        boolean res = invoker.rmObsoleteBAs(allBusinessAccounts);
        assertEquals(true, res);

        logger.info("Call 'Remove Obsolete BAs' again on same BAs - {}", allBusinessAccounts);
        res = invoker.rmObsoleteBAs(allBusinessAccounts);
        assertEquals(false, res);
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
