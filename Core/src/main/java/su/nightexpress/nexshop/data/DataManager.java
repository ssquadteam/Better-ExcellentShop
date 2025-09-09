package su.nightexpress.nexshop.data;

import org.bukkit.entity.Player;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import su.nightexpress.nexshop.ShopPlugin;
import su.nightexpress.nexshop.api.data.Saveable;
import su.nightexpress.nexshop.api.shop.Shop;
import su.nightexpress.nexshop.api.shop.product.Product;
import su.nightexpress.nexshop.api.shop.stock.StockValues;
import su.nightexpress.nexshop.config.Config;
import su.nightexpress.nexshop.data.key.ProductKey;
import su.nightexpress.nexshop.data.key.RotationKey;
import su.nightexpress.nexshop.data.product.PriceData;
import su.nightexpress.nexshop.data.product.StockData;
import su.nightexpress.nexshop.data.shop.RotationData;
import su.nightexpress.nexshop.shop.virtual.impl.Rotation;
import su.nightexpress.nexshop.shop.virtual.impl.VirtualProduct;
import su.nightexpress.nexshop.shop.virtual.impl.VirtualShop;
import su.nightexpress.nightcore.manager.AbstractManager;
import su.nightexpress.nightcore.util.Lists;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

public class DataManager extends AbstractManager<ShopPlugin> {

    private final Map<ProductKey, PriceData>     priceDataMap;
    private final Map<ProductKey, StockData>     stockDataMap;
    private final Map<RotationKey, RotationData> rotationDataMap;

    private boolean loaded;

    public DataManager(@NotNull ShopPlugin plugin) {
        super(plugin);
        this.priceDataMap = new ConcurrentHashMap<>();
        this.stockDataMap = new ConcurrentHashMap<>();
        this.rotationDataMap = new ConcurrentHashMap<>();
    }

    @Override
    protected void onLoad() {
        this.plugin.runTaskAsync(task -> {
            this.loadAllData(); // Load all price & stock datas for all products, then update prices.
        });

        this.addAsyncTask(this::saveScheduledDatas, Config.DATA_SAVE_INTERVAL.get());
    }

    @Override
    protected void onShutdown() {
        this.saveScheduledDatas();
        this.clear();
    }

    public void clear() {
        this.priceDataMap.clear();
        this.stockDataMap.clear();
        this.rotationDataMap.clear();
        this.loaded = false;
    }

    public boolean isLoaded() {
        return this.loaded;
    }

    public void handleSynchronization() {
        if (!this.isLoaded()) return;

        this.saveScheduledDatas();
        this.clear();

        this.loadAllData();
    }

    public void loadAllData() {
        this.loadPriceDatas();
        this.loadStockDatas();
        this.loadRotationDatas();
        this.loaded = true;
        this.plugin.getShopManager().getShops().forEach(shop -> shop.updatePrices(false)); // Update prices in the same thread to prevent data duplications.
    }


    private void loadPriceDatas() {
        this.plugin.getDataHandler().loadPriceDatas().forEach(this::loadPriceData);
        //this.plugin.debug("Loaded " + priceDataMap.size() + " product price datas.");
    }

    private void loadPriceData(@NotNull PriceData data) {
        ProductKey key = new ProductKey(data.getShopId(), data.getProductId(), data.getShopId());
        this.priceDataMap.put(key, data);
    }


    private void loadStockDatas() {
        this.plugin.getDataHandler().loadStockDatas().forEach(this::loadStockData);
        //this.plugin.debug("Loaded " + stockDataMap.size() + " product stock datas.");
    }

    private void loadStockData(@NotNull StockData data) {
        ProductKey key = new ProductKey(data.getShopId(), data.getProductId(), data.getHolder());
        this.stockDataMap.put(key, data);
    }


    private void loadRotationDatas() {
        this.plugin.getDataHandler().loadRotationDatas().forEach(this::loadRotationData);
        //this.plugin.debug("Loaded " + this.rotationDataMap.size() + " rotation datas.");
    }

    private void loadRotationData(@NotNull RotationData data) {
        this.rotationDataMap.put(new RotationKey(data.getShopId(), data.getRotationId()), data);
    }

    // =========================
    // External sync apply (Redis)
    // =========================
    public void applyExternalPriceData(@NotNull PriceData data) {
        this.priceDataMap.put(new ProductKey(data.getShopId(), data.getProductId(), data.getShopId()), data);
    }

    public void applyExternalDeletePriceDataByShop(@NotNull String shopId) {
        this.priceDataMap.keySet().removeIf(k -> k.getShopId().equalsIgnoreCase(shopId));
    }

    public void applyExternalDeletePriceDataByProduct(@NotNull String shopId, @NotNull String productId) {
        this.priceDataMap.keySet().removeIf(k -> k.getShopId().equalsIgnoreCase(shopId) && k.getProductId().equalsIgnoreCase(productId));
    }

    public void applyExternalStockData(@NotNull StockData data) {
        this.stockDataMap.put(new ProductKey(data.getShopId(), data.getProductId(), data.getHolder()), data);
    }

    public void applyExternalDeleteStockDataByShop(@NotNull String shopId) {
        this.stockDataMap.keySet().removeIf(k -> k.getShopId().equalsIgnoreCase(shopId));
    }

    public void applyExternalDeleteStockDataByProduct(@NotNull String shopId, @NotNull String productId) {
        this.stockDataMap.keySet().removeIf(k -> k.getShopId().equalsIgnoreCase(shopId) && k.getProductId().equalsIgnoreCase(productId));
    }

    public void applyExternalRotationData(@NotNull RotationData data) {
        this.rotationDataMap.put(new RotationKey(data.getShopId(), data.getRotationId()), data);
    }

    public void applyExternalDeleteRotationDataByShop(@NotNull String shopId) {
        this.rotationDataMap.keySet().removeIf(k -> k.getShopId().equalsIgnoreCase(shopId));
    }

    public void applyExternalDeleteRotationDataByRotation(@NotNull String shopId, @NotNull String rotationId) {
        this.rotationDataMap.keySet().removeIf(k -> k.getShopId().equalsIgnoreCase(shopId) && k.getRotationId().equalsIgnoreCase(rotationId));
    }


    public void saveScheduledDatas() {
        this.saveScheduledPriceDatas();
        this.saveScheduledStockDatas();
        this.saveScheduledRotationDatas();
    }

    public void saveScheduledPriceDatas() {
        Set<PriceData> toSave = this.getPriceDatas().stream()
            .filter(PriceData::isSaveRequired)
            .peek(d -> d.setSaveRequired(false))
            .collect(java.util.stream.Collectors.toSet());
        if (toSave.isEmpty()) return;

        this.plugin.getFoliaScheduler().runAsync(() -> {
            try {
                this.plugin.getDataHandler().updatePriceDatas(toSave);
                this.plugin.getRedisSyncManager().ifPresent(sync -> toSave.forEach(d -> {
                    sync.publishPriceData(d);
                    sync.cachePriceData(d);
                }));
            } catch (Exception e) {
                this.plugin.error("Error saving scheduled price data: " + e.getMessage());
            }
        });
    }

    public void saveScheduledStockDatas() {
        Set<StockData> toSave = this.getStockDatas().stream()
            .filter(StockData::isSaveRequired)
            .peek(d -> d.setSaveRequired(false))
            .collect(java.util.stream.Collectors.toSet());
        if (toSave.isEmpty()) return;

        this.plugin.getFoliaScheduler().runAsync(() -> {
            try {
                this.plugin.getDataHandler().updateStockDatas(toSave);
                this.plugin.getRedisSyncManager().ifPresent(sync -> toSave.forEach(d -> {
                    sync.publishStockData(d);
                    sync.cacheStockData(d);
                }));
            } catch (Exception e) {
                this.plugin.error("Error saving scheduled stock data: " + e.getMessage());
            }
        });
    }

    public void saveScheduledRotationDatas() {
        Set<RotationData> toSave = this.getRotationDatas().stream()
            .filter(RotationData::isSaveRequired)
            .peek(d -> d.setSaveRequired(false))
            .collect(java.util.stream.Collectors.toSet());
        if (toSave.isEmpty()) return;
        this.plugin.getDataHandler().updateRotationDatas(toSave);
        this.plugin.getRedisSyncManager().ifPresent(sync -> toSave.forEach(sync::publishRotationData));
    }

    private <T extends Saveable> void saveScheduledDatas(@NotNull Set<T> originDatas, @NotNull BiConsumer<DataHandler, Set<T>> consumer, @NotNull String name) {
        Set<T> filteredDatas = originDatas.stream().filter(Saveable::isSaveRequired).peek(data -> data.setSaveRequired(false)).collect(Collectors.toSet());
        if (filteredDatas.isEmpty()) return;

        consumer.accept(this.plugin.getDataHandler(), filteredDatas);
        //this.plugin.debug("Saved " + filteredDatas.size() + " " + name + " datas");
    }



    public void deleteAllData(@NotNull VirtualShop shop) {
        this.plugin.runTaskAsync(task -> {
            // First remove from the database.
            this.plugin.getDataHandler().deleteRotationData(shop);
            this.plugin.getDataHandler().deletePriceData(shop);
            this.plugin.getDataHandler().deleteStockData(shop);

            // Now clean up memory (so no duplicates can be created during the deletion process).
            this.rotationDataMap.keySet().removeIf(key -> key.isShop(shop));
            this.priceDataMap.keySet().removeIf(key -> key.isShop(shop));
            this.stockDataMap.keySet().removeIf(key -> key.isShop(shop));

            this.plugin.getRedisSyncManager().ifPresent(sync -> {
                String shopId = shop.getId();
                sync.publishRotationDataDeleteByShop(shopId);
                sync.publishPriceDataDeleteByShop(shopId);
                sync.publishStockDataDeleteByShop(shopId);
                // Local cache eviction
                sync.evictPriceDataByShop(shopId);
                sync.evictStockDataByShop(shopId);
            });
        });
    }



    @NotNull
    public Map<RotationKey, RotationData> getRotationDataMap() {
        return this.rotationDataMap;
    }

    @NotNull
    public Set<RotationData> getRotationDatas() {
        return new HashSet<>(this.rotationDataMap.values());
    }

    @Nullable
    public RotationData getRotationData(@NotNull Rotation rotation) {
        return this.rotationDataMap.get(RotationKey.from(rotation));
    }

    @NotNull
    public RotationData getRotationDataOrCreate(@NotNull Rotation rotation) {
        RotationData currentData = this.getRotationData(rotation);
        if (currentData != null) return currentData;

        RotationData data = new RotationData(rotation.getShop().getId(), rotation.getId());
        this.loadRotationData(data);
        this.plugin.runTaskAsync(task -> plugin.getDataHandler().insertRotationData(data));
        this.plugin.getRedisSyncManager().ifPresent(sync -> sync.publishRotationData(data));
        return data;
    }

    public void deleteRotationData(@NotNull Rotation rotation) {
        this.plugin.runTaskAsync(task -> {
            this.plugin.getDataHandler().deleteRotationData(rotation); // First remove from the database.
            this.rotationDataMap.remove(RotationKey.from(rotation)); // Now clean up memory (so no duplicates can be created during the deletion process).
            this.plugin.getRedisSyncManager().ifPresent(sync -> sync.publishRotationDataDeleteByRotation(rotation.getShop().getId(), rotation.getId()));
        });
    }

//    public void deleteRotationData(@NotNull VirtualShop shop) {
//        this.plugin.runTaskAsync(task -> {
//            this.plugin.getDataHandler().deleteRotationData(shop); // First remove from the database.
//            this.rotationDataMap.keySet().removeIf(key -> key.isShop(shop)); // Now clean up memory (so no duplicates can be created during the deletion process).
//        });
//    }



    @NotNull
    public Map<ProductKey, PriceData> getPriceDataMap() {
        return this.priceDataMap;
    }

    @NotNull
    public Set<PriceData> getPriceDatas() {
        return new HashSet<>(this.priceDataMap.values());
    }

    @Nullable
    public PriceData getPriceData(@NotNull Product product) {
        return this.priceDataMap.get(ProductKey.global(product));
    }

    @NotNull
    public PriceData getPriceDataOrCreate(@NotNull Product product) {
        PriceData data = this.getPriceData(product);
        if (data != null) return data;

        // Try Redis cache first
        this.plugin.getRedisSyncManager().flatMap(sync ->
            sync.getCachedPriceData(product.getShop().getId(), product.getId())
        ).ifPresent(this::loadPriceData);

        data = this.getPriceData(product);
        if (data != null) return data;

        PriceData fresh = PriceData.create(product);
        this.loadPriceData(fresh);
        this.plugin.runTaskAsync(task -> this.plugin.getDataHandler().insertPriceData(fresh));
        this.plugin.getRedisSyncManager().ifPresent(sync -> { sync.publishPriceData(fresh); sync.cachePriceData(fresh); });
        return fresh;
    }

    public void savePriceData(@NotNull Product product) {
        PriceData data = this.getPriceData(product);
        if (data == null) return;

        data.setSaveRequired(true);
    }

    public void deletePriceData(@NotNull Product product) {
        this.plugin.runTaskAsync(task -> {
            this.plugin.getDataHandler().deletePriceData(product); // First remove from the database.
            this.priceDataMap.remove(ProductKey.global(product)); // Now clean up memory (so no duplicates can be created during the deletion process).
            this.plugin.getRedisSyncManager().ifPresent(sync -> {
                sync.publishPriceDataDeleteByProduct(product.getShop().getId(), product.getId());
                sync.evictPriceDataByProduct(product.getShop().getId(), product.getId());
            });
        });
    }

//    public void deletePriceDatas(@NotNull Set<Product> products) {
//        products.forEach(product -> this.priceDataMap.remove(ProductKey.global(product)));
//
//        this.plugin.runTaskAsync(task -> {
//            this.plugin.getDataHandler().deletePriceDatas(products);
//        });
//    }

//    public void deletePriceDatas(@NotNull Shop shop) {
//        this.plugin.runTaskAsync(task -> {
//            this.plugin.getDataHandler().deletePriceData(shop);  // First remove from the database.
//            this.priceDataMap.keySet().removeIf(key -> key.isShop(shop)); // Now clean up memory (so no duplicates can be created during the deletion process).
//        });
//    }

    public void resetPriceDatas(@NotNull Shop shop) {
        this.resetPriceDatas(new HashSet<>(shop.getValidProducts()));
    }

    public void resetPriceData(@NotNull Product product) {
        this.resetPriceDatas(Lists.newSet(product));
    }

    public void resetPriceDatas(@NotNull Set<Product> products) {
        products.forEach(product -> {
            PriceData data = this.getPriceData(product);
            if (data == null) return;

            data.reset();
            data.setSaveRequired(true);
        });
    }



    @NotNull
    public Map<ProductKey, StockData> getStockDataMap() {
        return this.stockDataMap;
    }

    @NotNull
    public Set<StockData> getStockDatas() {
        return new HashSet<>(this.stockDataMap.values());
    }

    @Nullable
    public StockData getStockData(@NotNull Product product) {
        return this.stockDataMap.get(ProductKey.global(product));
    }

    @Nullable
    public StockData getStockData(@NotNull VirtualProduct product, @Nullable Player player) {
        return this.getStockData(product, player == null ? null : player.getUniqueId());
    }

    @Nullable
    public StockData getStockData(@NotNull VirtualProduct product, @Nullable UUID playerId) {
        return this.stockDataMap.get(ProductKey.globalOrPerosnal(product, playerId));
    }

    @NotNull
    public StockData getStockDataOrCreate(@NotNull VirtualProduct product, @Nullable Player player) {
        return this.getStockDataOrCreate(product, player == null ? null : player.getUniqueId());
    }

    @NotNull
    public StockData getStockDataOrCreate(@NotNull VirtualProduct product, @Nullable UUID playerId) {
        StockValues values = product.getStocksOrLimits(playerId);

        StockData data = this.getStockData(product, playerId);
        if (data != null) {
            if (data.isRestockTime()) {
                data.restock(values);
                data.setSaveRequired(true);
            }
            return data;
        }

        // Try Redis cache first
        String holder = playerId == null ? product.getShop().getId() : playerId.toString();
        this.plugin.getRedisSyncManager().flatMap(sync ->
            sync.getCachedStockData(product.getShop().getId(), product.getId(), holder)
        ).ifPresent(this::loadStockData);

        data = this.getStockData(product, playerId);
        if (data != null) {
            if (data.isRestockTime()) {
                data.restock(values);
                data.setSaveRequired(true);
            }
            return data;
        }

        StockData fresh = StockData.create(product, values, playerId);
        this.loadStockData(fresh);
        this.plugin.runTaskAsync(task -> plugin.getDataHandler().insertStockData(fresh));
        this.plugin.getRedisSyncManager().ifPresent(sync -> { sync.publishStockData(fresh); sync.cacheStockData(fresh); });
        return fresh;
    }

    public void deleteStockData(@NotNull VirtualProduct product) {
        this.plugin.runTaskAsync(task -> {
            this.plugin.getDataHandler().deleteStockData(product);  // First remove from the database.
            this.stockDataMap.remove(ProductKey.global(product)); // Now clean up memory (so no duplicates can be created during the deletion process).
            this.plugin.getRedisSyncManager().ifPresent(sync -> { sync.publishStockDataDeleteByProduct(product.getShop().getId(), product.getId()); sync.evictStockDataByProduct(product.getShop().getId(), product.getId()); });
        });
    }

//    public void deleteStockDatas(@NotNull Set<Product> products) {
//        products.forEach(product -> this.stockDataMap.remove(ProductKey.global(product)));
//
//        this.plugin.runTaskAsync(task -> plugin.getDataHandler().deleteStockDatas(products));
//    }

//    public void deleteStockDatas(@NotNull VirtualShop shop) {
//        this.plugin.runTaskAsync(task -> {
//            this.plugin.getDataHandler().deleteStockData(shop);  // First remove from the database.
//            this.stockDataMap.keySet().removeIf(key -> key.isShop(shop)); // Now clean up memory (so no duplicates can be created during the deletion process).
//        });
//    }

    public void resetStockDatas(@NotNull Product product) {
        this.resetStockDatas(Lists.newSet(product));
    }

    public void resetStockDatas(@NotNull VirtualShop shop) {
        this.resetStockDatas(new HashSet<>(shop.getProducts()));
    }

    public void resetStockDatas(@NotNull Set<Product> products) {
        products.forEach(product -> {
            this.stockDataMap.entrySet().stream().filter(e -> e.getKey().isProduct(product)).map(Map.Entry::getValue).forEach(data -> {
                data.setExpired();
                data.setSaveRequired(true);
            });
        });
    }
}
