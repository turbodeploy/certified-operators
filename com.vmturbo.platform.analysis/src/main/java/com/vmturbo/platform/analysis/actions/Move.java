package com.vmturbo.platform.analysis.actions;

import java.util.ArrayList;
import java.util.List;
import java.util.function.DoubleBinaryOperator;
import java.util.function.Function;
import java.util.function.IntFunction;

import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;

import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.BuyerParticipation;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Trader;

import static com.vmturbo.platform.analysis.actions.Utility.appendTrader;

/**
 * An action to move a {@link BuyerParticipation buyer participation} from one supplier to another.
 */
public class Move extends MoveBase implements Action { // inheritance for code reuse
    // Fields
    private final @Nullable Trader destination_;

    // Constructors

    /**
     * Constructs a new move action with the specified target and economy.
     *
     * @param economy The economy containing target and destination.
     * @param target The buyer participation that will move.
     * @param destination The trader, target is going to move to.
     */
    public Move(@NonNull Economy economy, @NonNull BuyerParticipation target, @Nullable Trader destination) {
        super(economy,target);
        destination_ = destination;
    }

    // Methods

    /**
     * Returns the destination of {@code this} move. i.e. the trader, target will move to.
     */
    @Pure
    public @Nullable Trader getDestination(@ReadOnly Move this) {
        return destination_;
    }

    @Override
    public @NonNull String serialize(@NonNull Function<@NonNull Trader, @NonNull String> oid) {
        return new StringBuilder()
            // TODO: is it enough to send the buyer or is the basket needed as well?
            .append("<action type=\"move\" target=\"").append(oid.apply(getTarget().getBuyer()))
            .append("\" source=\"").append(oid.apply(getSource()))
            .append("\" destination=\"").append(oid.apply(destination_))
            .append("\" />")
            .toString();
    }

    @Override
    public void take() {
        getTarget().move(destination_);
        updateQuantities(getEconomy(), getTarget(), getSource(), (sold, bought) -> Math.max(0, sold - bought));
        updateQuantities(getEconomy(), getTarget(), destination_, (sold, bought) -> sold + bought);
    }

    @Override
    public void rollback() {
        getTarget().move(getSource());
        updateQuantities(getEconomy(), getTarget(), destination_, (sold, bought) -> Math.max(0, sold - bought));
        updateQuantities(getEconomy(), getTarget(), getSource(), (sold, bought) -> sold + bought);
    }

    @Override
    public @NonNull String debugDescription(@NonNull Function<@NonNull Trader, @NonNull String> uuid,
                                            @NonNull Function<@NonNull Trader, @NonNull String> name,
                                            @NonNull IntFunction<@NonNull String> commodityType,
                                            @NonNull IntFunction<@NonNull String> traderType) {
        final @NonNull StringBuilder sb = new StringBuilder();

        if (getSource() != null) {
            if (destination_ != null) { // Move
                sb.append("Move ");
                appendTrader(sb, getTarget().getBuyer(), uuid, name);
                sb.append(" from ");
                appendTrader(sb, getSource(), uuid, name);
                sb.append(" to ");
                appendTrader(sb, getDestination(), uuid, name);
                sb.append(".");
            } else {// Unplace
                sb.append("Unplace ");
                appendTrader(sb, getTarget().getBuyer(), uuid, name);
                sb.append(" from ");
                appendTrader(sb, getSource(), uuid, name);
                sb.append(".");
            }
        } else {
            if (destination_ != null) { // Start
                sb.append("Start ");
                appendTrader(sb, getTarget().getBuyer(), uuid, name);
                sb.append(" on ");
                appendTrader(sb, getDestination(), uuid, name);
                sb.append(".");
            } else {
                return "Error in post-processing actions! This action should have been removed!";
            }
        }

        return sb.toString();
    }

    @Override
    public @NonNull String debugReason(@NonNull Function<@NonNull Trader, @NonNull String> uuid,
                                       @NonNull Function<@NonNull Trader, @NonNull String> name,
                                       @NonNull IntFunction<@NonNull String> commodityType,
                                       @NonNull IntFunction<@NonNull String> traderType) {
        final @NonNull StringBuilder sb = new StringBuilder();

        if (getSource() != null) {
            if (destination_ != null) {
                return "To provide a better placement."; // TODO: cover move conditions here!
            } else { // Unplace
                sb.append("To suspend ");
                appendTrader(sb, getSource(), uuid, name);
                sb.append(".");
            }
        } else {
            if (destination_ != null) { // Start
                sb.append("Because ");
                appendTrader(sb, getTarget().getBuyer(), uuid, name);
                sb.append(" was previously unplaced");
            } else {
                return "Error in post-processing actions! This action should have been removed!";
            }
        }

        return sb.toString();
    }

    /**
     * Updates the quantities and peak quantities sold by a specified seller as a result of a buyer
     * becoming or seizing to be a customer of the seller.
     *
     * <p>
     *  A binary operator is used to predict the change in quantities and peak quantities sold
     *  caused by the move.
     * </p>
     *
     * <p>
     *  Quantities are updated on a "best guess" basis: there is no guarantee that when the move is
     *  actually executed in the real environment, the quantities will change in that way.
     * </p>
     *
     * @param basketBought The basket bought by the buyer. It must match participation.
     * @param participation The buyer participation that will be moved. It must match basketBought.
     * @param traderToUpdate The seller whose commodities sold will be updated.
     * @param binaryOperator A binary operator (old quantity sold, quantity bought) -> new quantity sold.
     */
    // TODO: should we cover moves of inactive traders?
    static void updateQuantities(@NonNull Economy economy, @NonNull BuyerParticipation participation,
                                 @Nullable Trader traderToUpdate, @NonNull DoubleBinaryOperator binaryOperator) {
        @NonNull Basket basketBought = economy.getMarket(participation).getBasket();

        if (traderToUpdate != null) {
            final @NonNull @ReadOnly Basket basketSold = traderToUpdate.getBasketSold();
            List<@NonNull BuyerParticipation> participations = null;
            for (int boughtIndex = 0, soldIndex = 0 ; boughtIndex < basketBought.size() ; ++boughtIndex) {
                final int soldIndexBackUp = soldIndex;
                while (soldIndex < basketSold.size()
                                && !basketBought.get(boughtIndex).isSatisfiedBy(basketSold.get(soldIndex))) {
                    ++soldIndex;
                }

                if (soldIndex == basketSold.size()) { // if not found
                    soldIndex = soldIndexBackUp;
                } else {
                    final @NonNull CommoditySold commodity = traderToUpdate.getCommoditiesSold().get(soldIndex);
                    CommoditySpecification soldSpec = basketSold.get(soldIndex);

                    if (economy.isAdditive(soldSpec)) {
                        commodity.setQuantity(binaryOperator.applyAsDouble(commodity.getQuantity(),
                                participation.getQuantity(boughtIndex)));
                        commodity.setPeakQuantity(binaryOperator.applyAsDouble(commodity.getPeakQuantity(),
                                participation.getPeakQuantity(boughtIndex)));
                    } else {
                        if (participations == null) {
                            participations = economy.getCustomerParticipations(traderToUpdate);
                        }
                        // for each commodity spec in the basket, find the quantities bought by all participations
                        // and calculate the quantity sold
                        List<Double> quantitiesBought = new ArrayList<>();
                        List<Double> peakQuantitiesBought = new ArrayList<>();
                        for (int pIndex = 0; pIndex < participations.size(); pIndex++) {
                            BuyerParticipation aParticipation = participations.get(pIndex);
                            Basket basket = economy.getMarket(aParticipation).getBasket();
                            int specIndex = basket.indexOf(soldSpec);
                            if (specIndex >= 0) {
                                quantitiesBought.add(aParticipation.getQuantities()[specIndex]);
                                peakQuantitiesBought.add(aParticipation.getPeakQuantities()[specIndex]);
                            }
                        }
                        commodity.setQuantity(economy.getQuantityFunctions()
                                .get(soldSpec).applyAsDouble(quantitiesBought));
                        commodity.setPeakQuantity(economy.getQuantityFunctions()
                                .get(soldSpec).applyAsDouble(peakQuantitiesBought));
                    }
                    ++soldIndex;
                }
            }
        }
    }

} // end Move class
