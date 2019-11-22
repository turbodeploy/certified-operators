package com.vmturbo.cost.component.reserved.instance.recommendationalgorithm;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.context.annotation.Bean;

import com.google.common.collect.Sets;

import reactor.core.publisher.Flux;

import com.vmturbo.common.protobuf.repository.RepositoryDTOMoles.RepositoryServiceMole;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingServiceMole;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.cost.component.pricing.BusinessAccountPriceTableKeyStore;
import com.vmturbo.cost.component.reserved.instance.ActionContextRIBuyStore;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceBoughtStore;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceBoughtStore.ReservedInstanceBoughtChangeType;

/**
 * Class to unit test ReservedInstanceAnalysisInvoker.
 */
public class ReservedInstanceAnalysisInvokerTest {

    /**
     * Unit test method for testing ReservedInstanceAnalysisInvoker::getNewBusinessAccountsWithCost.
     */
    @Test
    public void testGetNewBusinessAccountsWithCost() {
        Set<Long> allBusinessAccounts = Sets.newHashSet(1L, 2L, 3L);
        BusinessAccountPriceTableKeyStore store = Mockito.mock(BusinessAccountPriceTableKeyStore.class);

        final Map<Long, Long> allBusinessAccountsWithCost = new HashMap<>();
        allBusinessAccountsWithCost.put(1L, 1L);
        allBusinessAccountsWithCost.put(2L, 2L);
        allBusinessAccountsWithCost.put(3L, 3L);
        Mockito.when(store.fetchPriceTableKeyOidsByBusinessAccount(any()))
                .thenReturn(allBusinessAccountsWithCost);

        final ReservedInstanceBoughtStore riBoughtStore = Mockito.mock(ReservedInstanceBoughtStore.class);
        final Flux<ReservedInstanceBoughtChangeType> updateEventStream = Flux.empty();
        Mockito.when(riBoughtStore.getUpdateEventStream()).thenReturn(updateEventStream);

        ReservedInstanceAnalysisInvoker invoker = new
                ReservedInstanceAnalysisInvoker(Mockito.mock(ReservedInstanceAnalyzer.class),
                                                repositoryRpcService(),
                                                settingsRpcService(),
                                                riBoughtStore,
                                                Mockito.mock(ActionContextRIBuyStore.class),
                                                store,
                          1);

        boolean result = invoker.isNewBusinessAccountWithCostFound(allBusinessAccounts);
        assertEquals(true, result);

        // A new business account is added but has no cost.
        allBusinessAccounts.add(4L);
        result = invoker.isNewBusinessAccountWithCostFound(allBusinessAccounts);
        assertEquals(false, result);

        // Now costs are also available for the new business account.
        allBusinessAccountsWithCost.put(4L, 4L);
        result = invoker.isNewBusinessAccountWithCostFound(allBusinessAccounts);
        assertEquals(true, result);
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
