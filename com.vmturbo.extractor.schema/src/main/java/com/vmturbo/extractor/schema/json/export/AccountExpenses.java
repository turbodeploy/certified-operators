package com.vmturbo.extractor.schema.json.export;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * Top-down expenses associated with an account.
 */
@JsonInclude(Include.NON_EMPTY)
@JsonPropertyOrder(alphabetic = true)
public class AccountExpenses {

    private String expenseDate;

    /**
     * Map from service display name (e.g. "ec2") to the current expenses for that service.
     */
    private Map<String, CostAmount> serviceExpenses;

    private Set<String> serviceList;

    private List<CostAmount> newServiceExpenses;

    @Nullable
    public String getExpenseDate() {
        return expenseDate;
    }

    public void setExpenseDate(String expenseDate) {
        this.expenseDate = expenseDate;
    }

    @Nullable
    public Map<String, CostAmount> getServiceExpenses() {
        return serviceExpenses;
    }

    /**
     * Set the expenses by service.
     *
     * @param serviceExpenses The expenses by service name.
     */
    public void setServiceExpenses(Map<String, CostAmount> serviceExpenses) {
        this.serviceExpenses = serviceExpenses;
        this.serviceList = serviceExpenses.keySet();
    }

    @Nullable
    public List<CostAmount> getNewServiceExpenses() {
        return newServiceExpenses;
    }

    /**
     * Set the expenses as list.
     *
     * @param serviceExpenses The expenses.
     */
    public void setNewServiceExpenses(Map<String, CostAmount> serviceExpenses) {
        this.serviceList = serviceExpenses.keySet();
        this.newServiceExpenses = serviceExpenses.values().stream().collect(Collectors.toList());
    }

    @Nullable
    public Set<String> getServiceList() {
        return serviceList;
    }
}
