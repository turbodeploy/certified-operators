package com.vmturbo.group.service;

import javax.annotation.Nonnull;

import io.grpc.Status;

import org.jooq.exception.DataAccessException;
import org.mockito.Mockito;

import com.vmturbo.group.group.GroupUpdateListener;
import com.vmturbo.group.policy.IPlacementPolicyStore;
import com.vmturbo.group.service.TransactionProviderImpl.StoresImpl;

/**
 * Mock transaction provider, holding the stores, that are really mocks.
 */
public class MockTransactionProvider implements TransactionProvider {
    private final Stores stores;
    private final IPlacementPolicyStore placementPolicyStore;
    private final MockSettingPolicyStore settingPolicyStore;
    private final MockGroupStore groupStore;

    /**
     * Constructs mock transaction provider with all the stores created as mocks.
     */
    public MockTransactionProvider() {
        this.placementPolicyStore = Mockito.mock(IPlacementPolicyStore.class);
        this.settingPolicyStore = Mockito.spy(new MockSettingPolicyStore());
        this.groupStore = Mockito.spy(new MockGroupStore());
        this.stores = new StoresImpl(settingPolicyStore, placementPolicyStore, groupStore);
    }

    @Nonnull
    @Override
    public <T> T transaction(@Nonnull TransactionalOperation<T> operation)
            throws StoreOperationException, InterruptedException {
        try {
            return operation.execute(stores);
        } catch (DataAccessException e) {
            if (e.getCause() instanceof StoreOperationException) {
                throw (StoreOperationException)e.getCause();
            } else {
                throw new StoreOperationException(Status.INTERNAL,
                        "Stores operation failed: " + e.getMessage(), e);
            }
        }
    }

    @Override
    public void addGroupUpdateListener(GroupUpdateListener updateListener) {

    }

    @Nonnull
    public IPlacementPolicyStore getPlacementPolicyStore() {
        return placementPolicyStore;
    }

    @Nonnull
    public MockSettingPolicyStore getSettingPolicyStore() {
        return settingPolicyStore;
    }

    @Nonnull
    public MockGroupStore getGroupStore() {
        return groupStore;
    }
}
