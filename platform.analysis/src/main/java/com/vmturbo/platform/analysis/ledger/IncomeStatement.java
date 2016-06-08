package com.vmturbo.platform.analysis.ledger;

import static com.google.common.base.Preconditions.checkArgument;

import java.lang.reflect.Field;

import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * An purchasing tendency of an entity that trades/is traded in a {@link Market}.
 *
 * <p>
 *  Both the trader and the commodities sold have expenses and revenues.
 * </p>
 */
public class IncomeStatement {
    // Fields
    private double expenses_;
    private double revenues_;
    private double minDesiredExpenses_;
    private double maxDesiredExpenses_;
    private double minDesiredRevenues_;
    private double maxDesiredRevenues_;

    // Constructors

    /**
     * Constructs a new IncomeStatement instance
     *
     */
    public IncomeStatement() {
        expenses_ = 0;
        revenues_ = 0;
        minDesiredExpenses_ = 0;
        maxDesiredExpenses_ = 0;
        minDesiredRevenues_ = 0;
        maxDesiredRevenues_ = 0;
    }


    // Methods

    /**
     *
     * <p>
     *  If this is a commodity its the amount the commodity spends on all the commodities it is chargedBy and
     *  if the entity is a trader, its the expense of the trader on its suppliers
     * </p>
     *
     * @return expense of this entity.
     * @see #setExpenses()
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
        checkArgument(expenses >= 0, "expenses = " + expenses);
        expenses_ = expenses;
        return this;
    }

    /**
     *
     * <p>
     *  If this is a commodity, its the amount the commodity earns from all the rawMaterials it made up of and
     *  if the entity is a trader, its the revenue of the trader from its customers
     * </p>
     *
     * @return revenue of this entity.
     * @see #setRevenues()
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
        checkArgument(revenues >= 0, "revenues = " + revenues);
        revenues_ = revenues;
        return this;
    }

    /**
     *
     * <p>
     *  For a trader T: the min expenses of T, while all the commodities it buys from its suppliers are within the Desired utilization range.
     *  For a commodity C sold by trader T: the total min cost of all the commodities bought by T that contribute to C's expenses, while all
     *  commodities sold by the corresponding suppliers of T are within the Desired utilization range.
     *  For a commodity C bought by a trader: N/A
     *  Traders and commodities: For non-decreasing price functions, min is achieved when all commodities sold by all suppliers are at their min
     *  Desired utilization.
     * </p>
     *
     * @return minDesiredExpense for this entity
     * @see #setMinDesiredExpenses()
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
        checkArgument(minDesiredExpenses >= 0, "minDesiredExpenses = " + minDesiredExpenses);
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
    * @see #setMaxDesiredExpenses()
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
        checkArgument(maxDesiredExpenses >= 0, "maxDesiredExpenses = " + maxDesiredExpenses);
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
    * @see #setMinDesiredRevenues()
    */
    public double getMinDesiredRevenues() {
        return minDesiredRevenues_;
    }

    /**
     * Sets the value of the <b>minDesiredRevenues_</b> field of that of a trader or a commodity
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
        checkArgument(minDesiredRevenues >= 0, "minDesiredRevenues = "+minDesiredRevenues);
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
     * @see #setMaxDesiredRevenues()
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
        checkArgument(maxDesiredRevenues >= 0, "maxDesiredRevenues + " + maxDesiredRevenues);
        maxDesiredRevenues_ = maxDesiredRevenues;
        return this;
    }

    /**
     * Returns the "Return On Investment" of a Trader or a Commodity sold, using the corresponding revenues and expenses of the Trader or Commodity
     *
     * @see #setRevenues()
     * @see #setExpenses()
     */
    public double getROI() {
        return revenues_/expenses_;
    }

    /**
     * Returns the min Return On Investment of a Trader or a Commodity sold, while all commodities contributing to the expenses and revenues remain
     * within the Desired utilization range.
     *
     * @see #setMinDesiredRevenues()
     * @see #setMaxDesiredExpenses()
     */
    public double getMinDesiredROI() {
        return minDesiredRevenues_/maxDesiredExpenses_;
    }

    /**
     * Returns the max Return On Investment of a Trader or a Commodity sold, while all commodities contributing to the expenses and revenues remain
     * within the Desired utilization range.
     *
     * @see #setMaxDesiredRevenues()
     * @see #setMinDesiredExpenses()
     */
    public double getMaxDesiredROI() {
        return maxDesiredRevenues_/minDesiredExpenses_;
    }

    public @NonNull IncomeStatement resetIncomeStatement() {

        for (Field f : this.getClass().getDeclaredFields()) {
            try {
                f.setDouble(this, 0);
            } catch (IllegalArgumentException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        return this;
    }

} // end class IncomeStatement
