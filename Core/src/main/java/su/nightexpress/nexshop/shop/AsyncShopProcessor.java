package su.nightexpress.nexshop.shop;

import org.jetbrains.annotations.NotNull;
import su.nightexpress.nexshop.ShopPlugin;
import su.nightexpress.nexshop.api.shop.Shop;
import su.nightexpress.nexshop.api.shop.product.Product;
import su.nightexpress.nexshop.product.price.impl.DynamicPricer;
import su.nightexpress.nexshop.product.price.impl.FloatPricer;
import su.nightexpress.nexshop.api.shop.type.PriceType;
import su.nightexpress.nexshop.api.shop.stock.StockValues;
import su.nightexpress.nexshop.api.shop.type.TradeType;
import su.nightexpress.nexshop.data.product.PriceData;
import su.nightexpress.nexshop.data.product.StockData;

import java.util.HashSet;
import java.util.Set;

public class AsyncShopProcessor {
    
    private final ShopPlugin plugin;
    
    public AsyncShopProcessor(@NotNull ShopPlugin plugin) {
        this.plugin = plugin;
    }
    
    @NotNull
    public AsyncShopUpdate processShopsAsync() {
        AsyncShopUpdate update = new AsyncShopUpdate();
        
        this.processShopUpdatesAsync(update);
        this.processPriceCalculationsAsync(update);
        this.processStockUpdatesAsync(update);
        this.processDataSavingAsync(update);
        
        return update;
    }
    
    private void processShopUpdatesAsync(@NotNull AsyncShopUpdate update) {
        try {
            Set<Shop> shopsToCheck = new HashSet<>(plugin.getShopManager().getShops());
            
            for (Shop shop : shopsToCheck) {
                if (shouldUpdateShop(shop)) {
                    update.addShopUpdate(shop);
                }
            }
        } catch (Exception e) {
            plugin.error("Error processing shop updates: " + e.getMessage());
        }
    }
    
    private void processPriceCalculationsAsync(@NotNull AsyncShopUpdate update) {
        try {
            Set<Shop> shops = new HashSet<>(plugin.getShopManager().getShops());
            
            for (Shop shop : shops) {
                for (Product product : shop.getProducts()) {
                    if (shouldUpdatePrice(product)) {
                        calculatePriceAsync(product, update);
                        update.addPriceUpdate(product);
                    }
                }
            }
        } catch (Exception e) {
            plugin.error("Error processing price calculations: " + e.getMessage());
        }
    }
    
    private void processStockUpdatesAsync(@NotNull AsyncShopUpdate update) {
        try {
            Set<Shop> shops = new HashSet<>(plugin.getShopManager().getShops());
            
            for (Shop shop : shops) {
                for (Product product : shop.getProducts()) {
                    long restockDate = product.getRestockDate(null);
                    if (restockDate > 0 && System.currentTimeMillis() >= restockDate) {
                        update.addProductRestock(product);
                    }
                }
            }
        } catch (Exception e) {
            plugin.error("Error processing stock updates: " + e.getMessage());
        }
    }
    
    private void processDataSavingAsync(@NotNull AsyncShopUpdate update) {
        try {
            Set<PriceData> priceDataToSave = plugin.getDataManager().getPriceDatas().stream()
                .filter(PriceData::isSaveRequired)
                .collect(HashSet::new, Set::add, Set::addAll);
            
            priceDataToSave.forEach(update::addPriceDataSave);
            
            Set<StockData> stockDataToSave = plugin.getDataManager().getStockDatas().stream()
                .filter(StockData::isSaveRequired)
                .collect(HashSet::new, Set::add, Set::addAll);
            
            stockDataToSave.forEach(update::addStockDataSave);
            
        } catch (Exception e) {
            plugin.error("Error processing data saving: " + e.getMessage());
        }
    }
    
    private boolean shouldUpdateShop(@NotNull Shop shop) {
        return shop.getProducts().stream().anyMatch(this::shouldUpdatePrice);
    }
    
    private boolean shouldUpdatePrice(@NotNull Product product) {
        if (product.getPricer().getType() == PriceType.FLAT) return false;
        if (product.getPricer().getType() == PriceType.PLAYER_AMOUNT) return false;
        
        PriceData priceData = plugin.getDataManager().getPriceData(product);
        return priceData != null && priceData.isExpired();
    }
    
    private void calculatePriceAsync(@NotNull Product product, @NotNull AsyncShopUpdate update) {
        try {
            PriceData priceData = plugin.getDataManager().getPriceDataOrCreate(product);
            
            if (priceData.isExpired()) {
                double buyPrice = priceData.getLatestBuyPrice();
                double sellPrice = priceData.getLatestSellPrice();
                long expireDate = priceData.getExpireDate();
                
                if (product.getPricer() instanceof FloatPricer floatPricer) {
                    buyPrice = floatPricer.rollPrice(TradeType.BUY);
                    sellPrice = floatPricer.rollPrice(TradeType.SELL);
                    expireDate = floatPricer.getClosestTimestamp();
                }
                else if (product.getPricer() instanceof DynamicPricer dynamicPricer) {
                    double difference = priceData.getPurchases() - priceData.getSales();
                    buyPrice = dynamicPricer.getAdjustedPrice(TradeType.BUY, difference);
                    sellPrice = dynamicPricer.getAdjustedPrice(TradeType.SELL, difference);
                    expireDate = -1L;
                }
                
                if (sellPrice > buyPrice && buyPrice >= 0) {
                    sellPrice = buyPrice;
                }
                
                priceData.setLatestBuyPrice(buyPrice);
                priceData.setLatestSellPrice(sellPrice);
                priceData.setExpireDate(expireDate);
                priceData.setLatestUpdateDate(System.currentTimeMillis());
                priceData.setSaveRequired(true);
                
                update.addPriceDataSave(priceData);
            }
        } catch (Exception e) {
            plugin.error("Error calculating price for product " + product.getId() + ": " + e.getMessage());
        }
    }
}
