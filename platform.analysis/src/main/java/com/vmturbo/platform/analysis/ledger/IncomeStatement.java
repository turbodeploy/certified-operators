package com.vmturbo.platform.analysis.ledger;

import static com.google.common.base.Preconditions.checkArgument;

import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * An purchasing tendency of an entity that trades/is traded in a {@link Market}.
 *
 * <p>
 *  Both the trader and the goods being traded have expenses and revenues.
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
     * Returns the expense of this entity.
     * If this is a commodity its the amount the commodity spends on all the commodities it is chargedBy and
     * if the entity is a trader, its the expense of the trader on its suppliers
     *
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
     * Returns the revenue of this entity.
     * If this is a commodity, its the amount the commodity earns from all the rawMaterials it made up of and
     * if the entity is a trader, its the revenue of the trader from its customers
     *
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
     * min expense accumulated from the provider(s) of an entity while remaining in desired state
     *
     * @return {@code this}
     *
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
        checkArgument(minDesiredExpenses >= 0, "minDesiredExpenses : " + minDesiredExpenses);
        minDesiredExpenses_ = minDesiredExpenses;
        return this;
    }

    /**
    *
    * max expense accumulated from the provider(s) of an entity while remaining in desired state
    *
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
        checkArgument(maxDesiredExpenses >= 0, "maxDesiredExpenses : " + maxDesiredExpenses);
        maxDesiredExpenses_ = maxDesiredExpenses;
        return this;
    }


    /**
    *
    * returns the minDesiredRevenue for this entity
    * <p>
    *  min revenue obtained from all the consumers of the entity while remaining in desired state
    * </p>
    *
    * @see #setMinDesiredRevenues()
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
        checkArgument(minDesiredRevenues >= 0, "minDesiredRevenues : "+minDesiredRevenues);
        minDesiredRevenues_ = minDesiredRevenues;
        return this;
    }


    /**
     *
     *  returns the max revenues returned while remaining in desired state
     *
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
        checkArgument(maxDesiredRevenues >= 0, "maxDesiredRevenues : " + maxDesiredRevenues);
        maxDesiredRevenues_ = maxDesiredRevenues;
        return this;
    }

    /**
     * returns the "Return On Investment" using the revenues and expenses accumulated by an entity
     *
     * @see #setRevenues()
     * @see #setExpenses()
     */
    public double getROI() {
        return revenues_/expenses_;
    }

    /**
     * returns the "min Return On Investment" using the minRevenues and maxExpenses accumulated by an entity while remaining in the desired state
     *
     * @see #setMinDesiredRevenues()
     * @see #setMaxDesiredExpenses()
     */
    public double getMinDesiredROI() {
        return minDesiredRevenues_/maxDesiredExpenses_;
    }

    /**
     * returns the "max Return On Investment" using the maxRevenues and minExpenses accumulated by an entity while remaining in the desired state
     *
     * @see #setMaxDesiredRevenues()
     * @see #setMinDesiredExpenses()
     */
    public double getMaxDesiredROI() {
        return maxDesiredRevenues_/minDesiredExpenses_;
    }

} // end class IncomeStatement
