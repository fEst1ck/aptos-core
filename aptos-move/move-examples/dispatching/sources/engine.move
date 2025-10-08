/// This engine module dispatches calls.
module dispatching::engine {
<<<<<<< HEAD
    use supra_framework::dispatchable_fungible_asset;
=======
    use aptos_framework::dispatchable_fungible_asset;
>>>>>>> aptos-framework-v1.34.0
    use dispatching::storage;

    /// The dispatch call knows both storage and indirectly the callback, thus the separate module.
    public entry fun dispatch<T>(data: vector<u8>) {
        let metadata = storage::insert<T>(data);
        dispatchable_fungible_asset::derived_supply(metadata);
    }
}
