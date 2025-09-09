package su.nightexpress.nexshop.data;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import su.nightexpress.nexshop.api.shop.product.Product;
import su.nightexpress.nexshop.data.product.PriceData;
import su.nightexpress.nexshop.data.product.StockData;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ShopDataCache {
    
    private static final ConcurrentMap<String, PriceData> PRICE_CACHE = new ConcurrentHashMap<>();
    private static final ConcurrentMap<String, StockData> STOCK_CACHE = new ConcurrentHashMap<>();
    private static final ConcurrentMap<String, Long> PRICE_CACHE_TIMESTAMPS = new ConcurrentHashMap<>();
    private static final ConcurrentMap<String, Long> STOCK_CACHE_TIMESTAMPS = new ConcurrentHashMap<>();
    
    private static final long CACHE_DURATION = 30000; // 30 seconds
    private static final int MAX_CACHE_SIZE = 1000;
    
    @Nullable
    public static PriceData getCachedPriceData(@NotNull Product product) {
        String key = getPriceKey(product);
        Long timestamp = PRICE_CACHE_TIMESTAMPS.get(key);
        
        if (timestamp == null || System.currentTimeMillis() - timestamp > CACHE_DURATION) {
            PRICE_CACHE.remove(key);
            PRICE_CACHE_TIMESTAMPS.remove(key);
            return null;
        }
        
        return PRICE_CACHE.get(key);
    }
    
    public static void cachePriceData(@NotNull Product product, @NotNull PriceData priceData) {
        if (PRICE_CACHE.size() >= MAX_CACHE_SIZE) {
            clearOldPriceEntries();
        }
        
        String key = getPriceKey(product);
        PRICE_CACHE.put(key, priceData);
        PRICE_CACHE_TIMESTAMPS.put(key, System.currentTimeMillis());
    }
    
    @Nullable
    public static StockData getCachedStockData(@NotNull Product product) {
        String key = getStockKey(product);
        Long timestamp = STOCK_CACHE_TIMESTAMPS.get(key);
        
        if (timestamp == null || System.currentTimeMillis() - timestamp > CACHE_DURATION) {
            STOCK_CACHE.remove(key);
            STOCK_CACHE_TIMESTAMPS.remove(key);
            return null;
        }
        
        return STOCK_CACHE.get(key);
    }
    
    public static void cacheStockData(@NotNull Product product, @NotNull StockData stockData) {
        if (STOCK_CACHE.size() >= MAX_CACHE_SIZE) {
            clearOldStockEntries();
        }
        
        String key = getStockKey(product);
        STOCK_CACHE.put(key, stockData);
        STOCK_CACHE_TIMESTAMPS.put(key, System.currentTimeMillis());
    }
    
    public static void invalidatePriceCache(@NotNull Product product) {
        String key = getPriceKey(product);
        PRICE_CACHE.remove(key);
        PRICE_CACHE_TIMESTAMPS.remove(key);
    }
    
    public static void invalidateStockCache(@NotNull Product product) {
        String key = getStockKey(product);
        STOCK_CACHE.remove(key);
        STOCK_CACHE_TIMESTAMPS.remove(key);
    }
    
    public static void clearAllCaches() {
        PRICE_CACHE.clear();
        STOCK_CACHE.clear();
        PRICE_CACHE_TIMESTAMPS.clear();
        STOCK_CACHE_TIMESTAMPS.clear();
    }
    
    private static void clearOldPriceEntries() {
        long currentTime = System.currentTimeMillis();
        PRICE_CACHE_TIMESTAMPS.entrySet().removeIf(entry -> {
            if (currentTime - entry.getValue() > CACHE_DURATION) {
                PRICE_CACHE.remove(entry.getKey());
                return true;
            }
            return false;
        });
    }
    
    private static void clearOldStockEntries() {
        long currentTime = System.currentTimeMillis();
        STOCK_CACHE_TIMESTAMPS.entrySet().removeIf(entry -> {
            if (currentTime - entry.getValue() > CACHE_DURATION) {
                STOCK_CACHE.remove(entry.getKey());
                return true;
            }
            return false;
        });
    }
    
    @NotNull
    private static String getPriceKey(@NotNull Product product) {
        return product.getShop().getId() + ":" + product.getId() + ":price";
    }
    
    @NotNull
    private static String getStockKey(@NotNull Product product) {
        return product.getShop().getId() + ":" + product.getId() + ":stock";
    }
    
    public static int getPriceCacheSize() {
        return PRICE_CACHE.size();
    }
    
    public static int getStockCacheSize() {
        return STOCK_CACHE.size();
    }
}
