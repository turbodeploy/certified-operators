package com.vmturbo.group.service;

import javax.annotation.Nonnull;

import io.grpc.Status;

import org.jooq.exception.DataAccessException;
import org.mockito.Mockito;

import com.vmturbo.group.policy.IPlacementPolicyStore;
import com.vmturbo.group.service.TransactionProviderImpl.StoresImpl;
import com.vmturbo.group.setting.ISettingPolicyStore;

/**
 * Mock transaction provider, holding the stores, that are really mocks.
 */
public class MockTransactionProvider implements TransactionProvider {
    private final Stores stores;
    private final IPlacementPolicyStore placementPolicyStore;
    private final ISettingPolicyStore settingPolicyStore;
    private final MockGroupStore groupStore;

    /**
     * Constructs mock transaction provider with all the stores created as mocks.
     */
    public MockTransactionProvider() {
        this.placementPolicyStore = Mockito.mock(IPlacementPolicyStore.class);
        this.settingPolicyStore = Mockito.mock(ISettingPolicyStore.class);
        this.groupStore = Mockito.spy(new MockGroupStore());
        this.stores = new StoresImpl(settingPolicyStore, placementPolicyStore, groupStore);
    }

    @Nonnull
    @Override
    public <T> T transaction(@Nonnull TransactionalOperation<T> operation)
            throws StoreOperationException {
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

    @Nonnull
    public IPlacementPolicyStore getPlacementPolicyStore() {
        return placementPolicyStore;
    }

    @Nonnull
    public ISettingPolicyStore getSettingPolicyStore() {
        return settingPolicyStore;
    }

    @Nonnull
    public MockGroupStore getGroupStore() {
        return groupStore;
    }
}
