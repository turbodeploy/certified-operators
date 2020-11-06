package com.vmturbo.platform.analysis.ledger;

import static com.google.common.base.Preconditions.checkArgument;

import org.checkerframework.checker.nullness.qual.NonNull;

import com.vmturbo.platform.analysis.economy.Market;

/**
 * Purchasing tendency of an entity that trades/is traded in a {@link Market}.
 *
 * <p>
 *  Both the trader and the commodities sold have expenses and revenues.
 * </p>
 *
 * @author shravan
 */
public class IncomeStatement {
    // Fields
    private double expenses_;
    private double revenues_;
    private double minDesiredExpenses_;
    private double maxDesiredExpenses_;
    private double minDesiredRevenues_;
    private double maxDesiredRevenues_;
    private double desiredRevenues_;

    // Constructors

    /**
     * Constructs a new IncomeStatement instance.
     *
     */
    public IncomeStatement() {
        expenses_ = 0;
        revenues_ = 0;
        minDesiredExpenses_ = 0;
        maxDesiredExpenses_ = 0;
        minDesiredRevenues_ = 0;
        maxDesiredRevenues_ = 0;
        desiredRevenues_ = 0;
    }


    // Methods

    /**
     *
     * <p>
     *  For a commodity, it is the amount the commodity spends on all the commodities it is chargedBy and
     *  for a trader, it is the expense of the trader on its suppliers.
     * </p>
     *
     * @return expense of this entity.
     * @see #setExpenses(double)
     */
    public double getExpenses() {
        return expenses_;
    }

    /**
     * Sets the value of the <b>expenses_</b> field.
     *
     * <p>
     *  Has no observable side-effects except setting the above field.
     * </p>
     *
     * @param expenses the new value for the field. Must be non-negative.
     * @return {@code this}
     *
     * @see #getExpenses()
     */
    protected @NonNull IncomeStatement setExpenses(double expenses) {
        checkArgument(expenses >= 0, "expenses = %s", expenses);
        expenses_ = expenses;
        return this;
    }

    /**
     *
     * <p>For a commodity, it is the amount the commodity earns from all the rawMaterials it made up
     *  of, and for a trader, it is the revenue of the trader from its customers.
     * </p>
     *
     * @return revenue of this entity.
     * @see #setRevenues(double)
     */
    public double getRevenues() {
        return revenues_;
    }

    /**
     * Sets the value of the <b>revenues_</b> field.
     *
     * <p>
     *  Has no observable side-effects except setting the above field.
     * </p>
     *
     * @param revenues the new value for the field. Must be non-negative.
     * @return {@code this}
     *
     * @see #getRevenues()
     */
    protected @NonNull IncomeStatement setRevenues(double revenues) {
        checkArgument(revenues >= 0, "revenues = %s", revenues);
        revenues_ = revenues;
        return this;
    }

    /**
     *
     * <p>
     *  For a trader T: the min desired expenses of T, while all the commodities it buys from its suppliers are within the Desired utilization range.
     *  For a commodity C sold by trader T: the total min cost of all the commodities bought by T that contribute to C's expenses, while all
     *  commodities sold by the corresponding suppliers of T are within the Desired utilization range.
     *  For a commodity C bought by a trader: N/A
     *  Traders and commodities: For non-decreasing price functions, min is achieved when all commodities sold by all suppliers are at their min
     *  Desired utilization.
     * </p>
     *
     * @return minDesiredExpense for this entity
     * @see #setMinDesiredExpenses(double)
     */
    public double getMinDesiredExpenses() {
        return minDesiredExpenses_;
    }

    /**
     * Sets the value of the <b>minDesiredExpenses_</b> field.
     *
     * <p>
     *  Has no observable side-effects except setting the above field.
     * </p>
     *
     * @param minDesiredExpenses the new value for the field. Must be non-negative.
     * @return {@code this}
     *
     * @see #getMinDesiredExpenses()
     */
    protected @NonNull IncomeStatement setMinDesiredExpenses(double minDesiredExpenses) {
        checkArgument(minDesiredExpenses >= 0, "minDesiredExpenses = %s", minDesiredExpenses);
        minDesiredExpenses_ = minDesiredExpenses;
        return this;
    }

    /**
    *
    * <p>
    *   For a trader T: the max expenses of T, while all the commodities it buys from its suppliers are within the Desired utilization range.
    *   For a commodity C sold by trader T: the total max cost of all the commodities bought by T that contribute to C's expenses, while all
    *   commodities sold by the corresponding suppliers of T are within the Desired utilization range.
    *   For a commodity C bought by a trader: N/A
    *   Traders and commodities: For non-decreasing price functions, max is achieved when all commodities sold by all suppliers are at their max
    *   Desired utilization.
    * </p>
    *
    * @return maxDesiredExpense for this entity
    * @see #setMaxDesiredExpenses(double)
    */
    public double getMaxDesiredExpenses() {
        return maxDesiredExpenses_;
    }


    /**
     * Sets the value of the <b>maxDesiredExpenses_</b> field.
     *
     * <p>
     *  Has no observable side-effects except setting the above field.
     * </p>
     *
     * @param maxDesiredExpenses the new value for the field. Must be non-negative.
     * @return {@code this}
     *
     * @see #getMaxDesiredExpenses()
     */
    protected @NonNull IncomeStatement setMaxDesiredExpenses(double maxDesiredExpenses) {
        checkArgument(maxDesiredExpenses >= 0, "maxDesiredExpenses = %s", maxDesiredExpenses);
        maxDesiredExpenses_ = maxDesiredExpenses;
        return this;
    }


    /**
    *
    * <p>
    *  For a trader T: the min revenues of T, while all the commodities it sells to its customers are within the Desired utilization range.
    *  For a commodity C sold by a trader: the min revenues of C, while C remains within the Desired utilization range.
    *  For a commodity C bought by a trader: N/A
    *  Traders and commodities: For non-decreasing price functions, min is achieved when all commodities sold are at their min Desired utilization.
    * </p>
    *
    * @return minDesiredRevenue for this entity
    * @see #setMinDesiredRevenues(double)
    */
    public double getMinDesiredRevenues() {
        return minDesiredRevenues_;
    }

    /**
     * Sets the value of the <b>minDesiredRevenues_</b> field.
     *
     * <p>
     *  Has no observable side-effects except setting the above field.
     * </p>
     *
     * @param minDesiredRevenues the new value for the field. Must be non-negative.
     * @return {@code this}
     *
     * @see #getMinDesiredRevenues()
     */
    protected @NonNull IncomeStatement setMinDesiredRevenues(double minDesiredRevenues) {
        checkArgument(minDesiredRevenues >= 0, "minDesiredRevenues = %s", minDesiredRevenues);
        minDesiredRevenues_ = minDesiredRevenues;
        return this;
    }


    /**
     *
     * <p>
     *  For a trader T: the max revenues of T, while all the commodities it sells to its customers are within the Desired utilization range.
     *  For a commodity C sold by a trader: the max revenues of C, while C remains within the Desired utilization range.
     *  For a commodity C bought by a trader: N/A
     *  Traders and commodities: For non-decreasing price functions, max is achieved when all commodities sold are at their max Desired utilization.
     * </p>
     *
     * @return maxDesiredRevenue for this entity
     * @see #setMaxDesiredRevenues(double)
     */
    public double getMaxDesiredRevenues() {
        return maxDesiredRevenues_;
    }

    /**
     * Sets the value of the <b>maxDesiredRevenues_</b> field.
     *
     * <p>
     *  Has no observable side-effects except setting the above field.
     * </p>
     *
     * @param maxDesiredRevenues the new value for the field. Must be non-negative.
     * @return {@code this}
     *
     * @see #getMaxDesiredRevenues()
     */
    protected @NonNull IncomeStatement setMaxDesiredRevenues(double maxDesiredRevenues) {
        checkArgument(maxDesiredRevenues >= 0, "maxDesiredRevenues = %s", maxDesiredRevenues);
        maxDesiredRevenues_ = maxDesiredRevenues;
        return this;
    }

    /**
    *
    * <p>
    *  For a trader T: the desired revenues of T, while all the commodities it sells to its customers are within the desired utilization range.
    *  For a commodity C sold by a trader: the desired revenues of C, while C remains within the desired utilization range.
    *  For a commodity C bought by a trader: N/A
    * </p>
    *
    * @return desired revenue for this entity
    * @see #setDesiredRevenues(double)
    */
    public double getDesiredRevenues() {
        return desiredRevenues_;
    }

    /**
     * Sets the value of the <b>desiredRevenues_</b> field.
     *
     * <p>
     *  Has no observable side-effects except setting the above field.
     * </p>
     *
     * @param desiredRevenues the new value for the field. Must be non-negative.
     * @return {@code this}
     *
     * @see #getDesiredRevenues()
     */
    protected @NonNull IncomeStatement setDesiredRevenues(final double desiredRevenues) {
        this.desiredRevenues_ = desiredRevenues;
        return this;
    }

    /**
     * Returns the "Return On Investment" of a Trader or a Commodity sold, using the corresponding
     * revenues and expenses of the Trader or Commodity.
     *
     * @see #setRevenues(double)
     * @see #setExpenses(double)
     */
    public double getROI() {
        if (expenses_ == 0) {
            return revenues_;
        } else {
            return revenues_ / expenses_;
        }
    }

    /**
     * Returns the min Return On Investment of a Trader or a Commodity sold, while all commodities contributing to the expenses and revenues remain
     * within the Desired utilization range.
     *
     * @see #setMinDesiredRevenues(double)
     * @see #setMaxDesiredExpenses(double)
     */
    public double getMinDesiredROI() {
        if (maxDesiredExpenses_ == 0) {
            return minDesiredRevenues_;
        } else {
            return minDesiredRevenues_ / maxDesiredExpenses_;
        }
    }

    /**
     * Returns the max Return On Investment of a Trader or a Commodity sold, while all commodities contributing to the expenses and revenues remain
     * within the Desired utilization range.
     *
     * @see #setMaxDesiredRevenues(double)
     * @see #setMinDesiredExpenses(double)
     */
    public double getMaxDesiredROI() {
        if (minDesiredExpenses_ == 0) {
            return maxDesiredRevenues_;
        } else {
            return maxDesiredRevenues_ / minDesiredExpenses_;
        }
    }

    public @NonNull IncomeStatement resetIncomeStatement() {

        expenses_ = 0;
        revenues_ = 0;
        minDesiredExpenses_ = 0;
        maxDesiredExpenses_ = 0;
        minDesiredRevenues_ = 0;
        maxDesiredRevenues_ = 0;
        desiredRevenues_ = 0;

        return this;
    }
} // end class IncomeStatement
