package com.vmturbo.group.service;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;

import io.grpc.Status;

import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredPolicyInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredSettingPolicyInfo;
import com.vmturbo.group.group.GroupDAO;
import com.vmturbo.group.group.IGroupStore;
import com.vmturbo.group.identity.IdentityProvider;
import com.vmturbo.group.policy.IPlacementPolicyStore;
import com.vmturbo.group.policy.PolicyStore;
import com.vmturbo.group.setting.ISettingPolicyStore;
import com.vmturbo.group.setting.SettingStore;

/**
 * Transaction provider based on Jooq connection.
 */
public class TransactionProviderImpl implements TransactionProvider {

    private final PolicyStore policyStore;
    private final SettingStore settingStore;
    private final DSLContext dslContext;
    private final IdentityProvider identityProvider;

    /**
     * Constructs transaction provider for RPC services.
     *
     * @param policyStore placement policy store to use
     * @param settingStore setting policy store to use
     * @param dslContext Jooq connection
     * @param identityProvider identity provider
     */
    public TransactionProviderImpl(@Nonnull PolicyStore policyStore,
            @Nonnull SettingStore settingStore, @Nonnull DSLContext dslContext,
            @Nonnull IdentityProvider identityProvider) {
        this.policyStore = Objects.requireNonNull(policyStore);
        this.settingStore = Objects.requireNonNull(settingStore);
        this.dslContext = Objects.requireNonNull(dslContext);
        this.identityProvider = Objects.requireNonNull(identityProvider);
    }

    @Nonnull
    @Override
    public <T> T transaction(@Nonnull TransactionalOperation<T> operation)
            throws StoreOperationException {
        try {
            return dslContext.transactionResult(config -> {
                final DSLContext transactionContext = DSL.using(config);
                final Stores stores =
                        new StoresImpl(new SettingPolicyStoreImpl(settingStore, transactionContext),
                                new PlacementPolicyStoreImpl(policyStore, transactionContext),
                                new GroupDAO(transactionContext, identityProvider));
                return operation.execute(stores);
            });
        } catch (DataAccessException e) {
            if (e.getCause() instanceof StoreOperationException) {
                throw (StoreOperationException)e.getCause();
            } else {
                throw new StoreOperationException(Status.INTERNAL,
                        "Stores operation failed: " + e.getMessage(), e);
            }
        }
    }

    /**
     * Class to hold various stores available within transaction.
     */
    protected static class StoresImpl implements Stores {
        private final ISettingPolicyStore settingPolicyStore;
        private final IPlacementPolicyStore placementPolicyStore;
        private final IGroupStore groupStore;

        StoresImpl(@Nonnull ISettingPolicyStore settingPolicyStore,
                @Nonnull IPlacementPolicyStore placementPolicyStore,
                @Nonnull IGroupStore groupStore) {
            this.settingPolicyStore = settingPolicyStore;
            this.placementPolicyStore = placementPolicyStore;
            this.groupStore = groupStore;
        }

        @Override
        @Nonnull
        public IGroupStore getGroupStore() {
            return groupStore;
        }

        @Override
        @Nonnull
        public ISettingPolicyStore getSettingPolicyStore() {
            return settingPolicyStore;
        }

        @Override
        @Nonnull
        public IPlacementPolicyStore getPlacementPolicyStore() {
            return placementPolicyStore;
        }
    }

    /**
     * Setting policy store implementation substituting the DB connection.
     */
    private static class SettingPolicyStoreImpl implements ISettingPolicyStore {
        private final SettingStore settingStore;
        private final DSLContext dslContext;

        SettingPolicyStoreImpl(@Nonnull SettingStore settingStore, @Nonnull DSLContext dslContext) {
            this.settingStore = settingStore;
            this.dslContext = dslContext;
        }

        @Override
        public void updateTargetSettingPolicies(long targetId,
                @Nonnull List<DiscoveredSettingPolicyInfo> settingPolicyInfos,
                @Nonnull Map<String, Long> groupOids) throws DataAccessException {
            settingStore.updateTargetSettingPolicies(dslContext, targetId, settingPolicyInfos,
                    groupOids);
        }
    }

    /**
     * Placement policy store substituting the DB connection.
     */
    private static class PlacementPolicyStoreImpl implements IPlacementPolicyStore {
        private final PolicyStore policyStore;
        private final DSLContext dslContext;

        PlacementPolicyStoreImpl(@Nonnull PolicyStore policyStore, @Nonnull DSLContext dslContext) {
            this.policyStore = policyStore;
            this.dslContext = dslContext;
        }

        @Override
        public void updateTargetPolicies(long targetId,
                @Nonnull List<DiscoveredPolicyInfo> policyInfos,
                @Nonnull Map<String, Long> groupOids) throws DataAccessException {
            policyStore.updateTargetPolicies(dslContext, targetId, policyInfos, groupOids);
        }
    }
}
