package com.vmturbo.group.service;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import io.grpc.Status;

import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredPolicyInfo;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy.Type;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.group.group.GroupDAO;
import com.vmturbo.group.group.IGroupStore;
import com.vmturbo.group.policy.IPlacementPolicyStore;
import com.vmturbo.group.policy.PolicyStore;
import com.vmturbo.group.setting.ISettingPolicyStore;
import com.vmturbo.group.setting.SettingPolicyFilter;
import com.vmturbo.group.setting.SettingStore;
import com.vmturbo.platform.sdk.common.util.Pair;

/**
 * Transaction provider based on Jooq connection.
 */
public class TransactionProviderImpl implements TransactionProvider {

    private final PolicyStore policyStore;
    private final SettingStore settingStore;
    private final DSLContext dslContext;

    /**
     * Constructs transaction provider for RPC services.
     *
     * @param policyStore placement policy store to use
     * @param settingStore setting policy store to use
     * @param dslContext Jooq connection
     */
    public TransactionProviderImpl(@Nonnull PolicyStore policyStore,
            @Nonnull SettingStore settingStore, @Nonnull DSLContext dslContext) {
        this.policyStore = Objects.requireNonNull(policyStore);
        this.settingStore = Objects.requireNonNull(settingStore);
        this.dslContext = Objects.requireNonNull(dslContext);
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
                                new GroupDAO(transactionContext));
                return operation.execute(stores);
            });
        } catch (DataAccessException | org.springframework.dao.DataAccessException e) {
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

        @Nonnull
        @Override
        public Map<Long, Map<String, Long>> getDiscoveredPolicies() {
            return settingStore.getDiscoveredPolicies(dslContext);
        }

        @Override
        public void deletePolicies(@Nonnull Collection<Long> oids, @Nonnull Type allowedType)
                throws StoreOperationException {
            settingStore.deleteSettingPolcies(dslContext, oids, allowedType);
        }

        @Override
        public void createSettingPolicies(@Nonnull Collection<SettingPolicy> settingPolicies)
                throws StoreOperationException {
            settingStore.createSettingPolicies(dslContext, settingPolicies);
        }

        @Nonnull
        @Override
        public Optional<SettingPolicy> getPolicy(long id) throws StoreOperationException {
            return settingStore.getSettingPolicy(dslContext, id);
        }

        @Nonnull
        @Override
        public Collection<SettingPolicy> getPolicies(
                @Nonnull SettingPolicyFilter filter) throws StoreOperationException {
            return settingStore.getSettingPolicies(dslContext, filter);
        }

        @Nonnull
        @Override
        public Pair<SettingPolicy, Boolean> updateSettingPolicy(long id,
                @Nonnull SettingPolicyInfo newPolicyInfo) throws StoreOperationException {
            return settingStore.updateSettingPolicy(id, newPolicyInfo);
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

        @Override
        public void deletePoliciesForGroupBeingRemoved(@Nonnull Collection<Long> groupIds) {
            policyStore.deletePoliciesForGroupBeingRemoved(dslContext, groupIds);
        }
    }

}
