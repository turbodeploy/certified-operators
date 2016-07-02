package com.vmturbo.platform.analysis.ede;

import java.util.ArrayList;
import java.util.List;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.ProvisionBySupply;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.ledger.Ledger;

public abstract class Supply {

    /**
     * Return a list of recommendations to optimize the change in supply of the economy.
     *
     * <p>
     *  As a result of invoking this method, both the economy and the state that are passed as
     *  parameters to it, may be changed.
     * </p>
     *
     * @param economy - the {@link Economy} whose traders we may clone or suspend to bring in
     *                  the desired state
     * @param ledger - the {@link Ledger}} with the expenses and revenues of all the traders
     *        and commodities in the economy
     *
     * @return a list of actions with regard to supply change and the move actions after applying
     *         the supply change actions
     */
    public @NonNull List<@NonNull Action> supplyDecisions(@NonNull Economy economy,
                    @NonNull Ledger ledger, boolean isProvision) {

        List<@NonNull Action> allActions = new ArrayList<>();
        List<@NonNull Action> actions = new ArrayList<>();
        for (Market market : economy.getMarkets()) {
            for (;;) {
                // if there are no sellers in the market, the buyer is misconfigured
                actions.clear();
                if (market.getActiveSellers().isEmpty()) {
                    break;
                }

                ledger.calculateExpAndRevForSellersInMarket(economy, market);

                Trader bestTraderToEngage = findTheBestTraderToEngage(market, ledger);
                // break if there is no seller that satisfies the engagement criteria in the market
                if (bestTraderToEngage == null) {
                    break;
                }

                takeActionAndUpdateLedger(economy, market, ledger, bestTraderToEngage,
                                actions);
                Trader provisionedSeller = (actions.get(actions.size() -1) instanceof ProvisionBySupply) ?
                                                ((ProvisionBySupply)actions.get(actions.size() - 1))
                                                                .getProvisionedSeller()
                                                : null;
                // run placement after cloning or suspending a trader to the economy
                actions.addAll(Placement.placementDecisions(economy));
                ledger.calculateExpAndRevForSellersInMarket(economy, market);

                if (!evalAcceptanceCriteriaForMarket(market, ledger)) {
                    rollBackActionAndUpdateLedger(ledger, provisionedSeller, actions);
                    break;
                }
                allActions.addAll(actions);
            }
        }

        return allActions;
    }

    /**
     * Return the best trader to clone or suspend after checking the engagement criteria for all
     * traders of a particular market
     *
     * @param market - the {@link Market} whose sellers are considered to verify profitability that
     *                 implies eligibility to clone or suspend
     * @param ledger - the {@link Ledger} that holds the incomeStatement of the sellers considered
     *
     * @return the best trader satisfy the engagement criteria if there is any, otherwise NULL
     */
    public abstract Trader findTheBestTraderToEngage(Market market, Ledger ledger);

    /**
     * Return true/false after checking the acceptance criteria for a particular market
     *
     * @param market - the {@link Market} whose sellers are considered to verify profitability that
     *                 implies eligibility to clone or suspend
     * @param ledger - the {@link Ledger} that holds the incomeStatement of the sellers considered
     *
     * @return true - if the acceptance criteria is met by every trader in market
     */
    public abstract boolean evalAcceptanceCriteriaForMarket(Market market, Ledger ledger);

    /**
     * Return a list of actions which contains the clone or suspend action. The particular supply change
     * action would be created, executed and added to the list returned. If it is a provision action,
     * the newly provisioned trader would be added to ledger and relevant income statements would be
     * generated.
     * @param economy - the {@link Economy} in which the clone or suspend action is taken place
     * @param market - the {@link Market} in which the clone or suspend action takes place
     * @param ledger -  the {@link Ledger} that holds the incomeStatement of the economy
     * @param bestTraderToEngage - the trader that satisfies the engagement criteria best
     * @param actions - a list that the clone or suspend action would be added to
     *
     */
    public abstract void takeActionAndUpdateLedger(Economy economy, Market market, Ledger ledger,
                    Trader bestTraderToEngage, List<@NonNull Action> actions);

    /**
     * Rolling back the actions in a reverse order. Remove the newly added trader's income statement
     * from ledger if the actions are derived from provision. In such a case, the provisionedTrader is
     * not null, otherwise it is NULL.
     * <p>
     *  Rolling back the actions must be in reverse order, otherwise we may encounter issues.
     *  e.g: action1 is trader1 move from A to B, action2 is trader1 move from B to C
     *  rolling back should start from action2 as trader1 is at C now
     * </p>
     * @param ledger - the {@link Ledger} that holds the incomeStatement of the economy
     * @param provisionedTrader - the newly added trader, it is NULL if actions contain a provision
     * @param actions - a list of actions to be rolled back
     *
     * @return a list of actions that has being rolled back
     */
    public abstract void rollBackActionAndUpdateLedger(Ledger ledger,
                    @Nullable Trader provisionedTrader, List<@NonNull Action> actions);
}
