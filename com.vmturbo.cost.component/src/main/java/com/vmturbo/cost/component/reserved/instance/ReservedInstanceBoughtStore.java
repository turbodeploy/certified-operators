package com.vmturbo.cost.component.reserved.instance;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.jooq.DSLContext;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.cost.component.TableDiagsRestorable;
import com.vmturbo.cost.component.db.tables.records.ReservedInstanceBoughtRecord;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceBoughtFilter;
import com.vmturbo.platform.sdk.common.PricingDTO;
import com.vmturbo.sql.utils.DbException;

/**
 * An interface for a SQL-based stores for reserved isntances.
 *
 */
public interface ReservedInstanceBoughtStore extends ReservedInstanceCostStore,
        TableDiagsRestorable<Void, ReservedInstanceBoughtRecord> {

    /**
     * Get all {@link ReservedInstanceBought} from reserved instance table.
     *
     * @param filter {@link ReservedInstanceBoughtFilter} which contains all filter condition.
     * @return a list of {@link ReservedInstanceBought}.
     */
    @Nonnull
    List<ReservedInstanceBought> getReservedInstanceBoughtByFilter(
            @Nonnull ReservedInstanceBoughtFilter filter);

    /**
     * Get all {@link ReservedInstanceBought} from reserved instance table.
     *
     * @param filter {@link ReservedInstanceBoughtFilter} which contains all filter condition.
     * @param context The context to use as a transaction for querying available RIs.
     * @return a list of {@link ReservedInstanceBought}.
     */
    @Nonnull
    List<ReservedInstanceBought> getReservedInstanceBoughtByFilterWithContext(
            @Nonnull final DSLContext context,
            @Nonnull ReservedInstanceBoughtFilter filter);

    /**
     * Get the sum count of reserved instance bought by different compute tier type.
     *
     * @param filter a {@link ReservedInstanceBoughtFilter} contains all filter condition of the request.
     * @return a Map which key is compute tier id and value is the sum count of reserved instance bought
     * which belong to this type computer tier.
     */
    @Nonnull
    Map<Long, Long> getReservedInstanceCountMap(@Nonnull ReservedInstanceBoughtFilter filter);

    /**
     * Get the sum count of reserved instance bought by RI spec ID.
     *
     * @param filter {@link ReservedInstanceBoughtFilter} which contains all filter condition.
     * @return a Map which key is reservedInstance spec ID and value is the sum count of reserved
     * instance bought which belong to this spec.
     */
    @Nonnull
    Map<Long, Long> getReservedInstanceCountByRISpecIdMap(ReservedInstanceBoughtFilter filter);

    /**
     * Input a list of latest {@link ReservedInstanceBoughtInfo}, it will update reserved instance table
     * based on "probeReservedInstanceId" field to tell if two reserved instance bought are same or
     * not. And for new added reserved instance data which is not exist in current table, those data will
     * be insert to table as new records. For those table records which also appeared in latest reserved instance
     * bought data, they will be updated based on the latest data. For those table records which is not
     * appeared in latest reserved instance bought data, they will be deleted from table.
     *
     * @param context {@link DSLContext} transactional context.
     * @param newReservedInstances a list of {@link ReservedInstanceBoughtInfo}.
     */
    void updateReservedInstanceBought(
            @Nonnull final DSLContext context,
            @Nonnull List<ReservedInstanceBoughtInfo> newReservedInstances);

    /**
     * Update RIBought iff the recurringPrices and fixedCost both are 0.
     *
     * @param reservedInstanceSpecPrices PriceList for RI indexed by OID.
     */
    void updateRIBoughtFromRIPriceList(
            @Nonnull Map<Long, PricingDTO.ReservedInstancePrice> reservedInstanceSpecPrices);


    /**
     * Registers a callback method, invoked on RI inventory updates.
     * @param callback The callback method to be invoked on RI inventory updates.
     */
    void onInventoryChange(@Nonnull Runnable callback);


    /**
     * Retrieve the reserved instances per the passed filter and then update the capacities for a
     * partial cloud environment. If an RI is undiscovered, cap the available number of coupons to
     * the number of coupons used by discovered accounts. If it is discovered, exclude the usage
     * from undiscovered accounts.
     *
     * @param filter {@link ReservedInstanceBoughtFilter} which contains all filter condition.
     * @return a list of {@link ReservedInstanceBought}.
     */
    List<ReservedInstanceBought>  getReservedInstanceBoughtForAnalysis(
            @Nonnull ReservedInstanceBoughtFilter filter);

    /**
     * Gets the number of used coupons for the reserved instances.
     *
     * @param context {@link DSLContext} transactional context
     * @param filterByReservedInstanceIds filter by the reserved instances
     * @return the number of used coupons for the reserved instances
     */
    @Nonnull
    Map<Long, Double> getNumberOfUsedCouponsForReservedInstances(@Nonnull DSLContext context,
            @Nonnull Collection<Long> filterByReservedInstanceIds);

    /**
     * Gets the number of used coupons for the reserved instances.
     *
     * @param filterByReservedInstanceIds filter by the reserved instances
     * @return the number of used coupons for the reserved instances
     */
    @Nonnull
    Map<Long, Double> getNumberOfUsedCouponsForReservedInstances(
            @Nonnull Collection<Long> filterByReservedInstanceIds);


    /**
     * Gets the undiscovered RIs. An undiscovered RI is an RI that is purchased by an account
     * that has not been added as a target.
     *
     * @return List of undiscovered reserved instances.
     */
    @Nonnull
    List<ReservedInstanceBought> getUndiscoveredReservedInstances();

    /**
     * Gets the undiscovered RIs not being used by any of the managed accounts.
     * An undiscovered RI is an RI that is purchased by an account
     * that has not been added as a target.
     *
     * @param filter Filter rEmptyReservedInstanceBoughtStore.javaepresenting the scope for the requested RI list.
     * @return List of reserved instances not used by managed accounts.
     */
    @Nonnull
    List<ReservedInstanceBought> getUndiscoveredUnusedReservedInstancesInScope(
            ReservedInstanceBoughtFilter filter);

    /**
     * Retrieves the timestamp when the RI was first discovered and stored in
     * Tubo.
     * @param riOid the reserved instance oid
     * @return dicovery timestamp
     * @throws DbException throws DB exception when unable to retrieve data.
     */
    @Nonnull
    Map<Long, Long> getCreationTime(Set<Long> riOid);
}
