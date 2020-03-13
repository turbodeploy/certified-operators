package com.vmturbo.group.service;

import javax.annotation.Nonnull;

import com.vmturbo.group.group.IGroupStore;
import com.vmturbo.group.policy.IPlacementPolicyStore;
import com.vmturbo.group.setting.ISettingPolicyStore;

/**
 * Abstraction providing the transaction mechanism. It opens a transaction and allows operating with
 * different stores (DAOs) within this transaction.
 */
public interface TransactionProvider {

    /**
     * Execute operation in transaction.
     *
     * @param operation operation to be executed
     * @param <T> type of a return value
     * @return result of an operation
     * @throws StoreOperationException operation (if any) while operating with stores
     *         object.
     * @throws InterruptedException if current thread has been interrupted
     */
    @Nonnull
    <T> T transaction(@Nonnull TransactionalOperation<T> operation)
            throws StoreOperationException, InterruptedException;

    /**
     * Stores is a set of different stores (DAOs) available to be operated within the transaction.
     */
    interface Stores {
        /**
         * Returns group store.
         *
         * @return group store
         */
        @Nonnull
        IGroupStore getGroupStore();

        /**
         * Returns setting policy store.
         *
         * @return setting policy store
         */
        @Nonnull
        ISettingPolicyStore getSettingPolicyStore();

        /**
         * Returns placement policy store.
         *
         * @return placement policy store
         */
        @Nonnull
        IPlacementPolicyStore getPlacementPolicyStore();
    }

    /**
     * Operation that could be executed within a transaction.
     *
     * @param <T> type of return value.
     */
    interface TransactionalOperation<T> {
        /**
         * Executes the operation within a transaction.
         *
         * @param stores stores available within the transaction
         * @return operation result
         * @throws StoreOperationException if any store thrown an exception.
         * @throws InterruptedException if current thread has been interrupted
         */
        @Nonnull
        T execute(@Nonnull Stores stores) throws StoreOperationException, InterruptedException;
    }
}


