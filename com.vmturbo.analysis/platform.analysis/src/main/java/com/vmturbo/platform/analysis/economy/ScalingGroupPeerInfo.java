package com.vmturbo.platform.analysis.economy;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Manages information about a scaling group's peers.
 */
public class ScalingGroupPeerInfo {
    /**
     * Dummy ScalingGroupPeerInfo instance to use as the "null" value.  It returns no peers and
     * is a matched group.
     */
    public static final ScalingGroupPeerInfo EMPTY = new ScalingGroupPeerInfo();
    private List<ShoppingList> peers_;
    // True when the scaling group is currently consistent sized.
    private boolean consistentlySized_;

    /**
     * Constructor.
     */
    public ScalingGroupPeerInfo() {
        this.peers_ = new ArrayList<>();
        this.consistentlySized_ = true;
    }

    /**
     * Construct a ScalingGroupPeerInfo instance with an initial member shopping list.
     * @param shoppingList shopping list to add.
     */
    public ScalingGroupPeerInfo(final ShoppingList shoppingList) {
        this();
        this.peers_.add(shoppingList);
    }

    /**
     * Register a shopping list with a scaling group.
     * @param peer peer shopping list to add.
     */
    public void addPeer(ShoppingList peer) {
        if (peer != null) {
            this.peers_.add(peer);
        }
    }

    /**
     * Return the peer shopping lists for the scaling group that the shopping list UUID is in.
     * @param shoppingList shopping list to omit from the return list.  If it is not
     *                     in this scaling group, then all peer shopping lists in the scaling
     *                     group will be returned. It is acceptable to pass in null if this is
     *                     the desired result.
     * @return List of other shopping lists in this scaling group.
     */
    public List<ShoppingList> getPeers(ShoppingList shoppingList) {
        UUID shoppingListId = shoppingList == null ? null : shoppingList.getShoppingListId();
        return peers_.stream()
                        .filter(sl -> sl.getShoppingListId() != shoppingListId)
                        .collect(Collectors.toList());
    }

    /**
     * Return the peer shopping lists for the scaling group.
     * @return List of all shopping lists in this scaling group.
     */
    public List<ShoppingList> getPeers() {
        return peers_;
    }

    public boolean isConsistentlySized() {
        return consistentlySized_;
    }

    public void setConsistentlySized(final boolean matched) {
        this.consistentlySized_ = matched;
    }
}
