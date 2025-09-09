package su.nightexpress.nexshop.shop;

import org.jetbrains.annotations.NotNull;
import su.nightexpress.nexshop.api.shop.Shop;
import su.nightexpress.nexshop.api.shop.product.Product;
import su.nightexpress.nexshop.api.shop.type.TradeType;
import su.nightexpress.nexshop.data.product.PriceData;
import su.nightexpress.nexshop.data.product.StockData;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class AsyncShopUpdate {
    
    private final Set<Shop> shopsToUpdate;
    private final Set<Product> productsToUpdatePrice;
    private final Set<PriceData> priceDataToSave;
    private final Set<StockData> stockDataToSave;
    private final Set<Product> productsToRestock;
    
    public AsyncShopUpdate() {
        this.shopsToUpdate = ConcurrentHashMap.newKeySet();
        this.productsToUpdatePrice = ConcurrentHashMap.newKeySet();
        this.priceDataToSave = ConcurrentHashMap.newKeySet();
        this.stockDataToSave = ConcurrentHashMap.newKeySet();
        this.productsToRestock = ConcurrentHashMap.newKeySet();
    }
    
    public void addShopUpdate(@NotNull Shop shop) {
        this.shopsToUpdate.add(shop);
    }
    
    public void addPriceUpdate(@NotNull Product product) {
        this.productsToUpdatePrice.add(product);
    }
    
    public void addPriceDataSave(@NotNull PriceData priceData) {
        this.priceDataToSave.add(priceData);
    }
    
    public void addStockDataSave(@NotNull StockData stockData) {
        this.stockDataToSave.add(stockData);
    }
    
    public void addProductRestock(@NotNull Product product) {
        this.productsToRestock.add(product);
    }
    
    public boolean hasUpdates() {
        return !shopsToUpdate.isEmpty() || !productsToUpdatePrice.isEmpty() || 
               !priceDataToSave.isEmpty() || !stockDataToSave.isEmpty() || 
               !productsToRestock.isEmpty();
    }
    
    public void applyToMainThread() {
        for (Shop shop : shopsToUpdate) {
            try {
                shop.updatePrices(false);
            } catch (Exception e) {
                // Handle shop update errors gracefully
            }
        }
        
        for (Product product : productsToUpdatePrice) {
            try {
                product.updatePrice(false);
            } catch (Exception e) {
                // Handle price update errors gracefully
            }
        }
        
        for (Product product : productsToRestock) {
            try {
                // Apply restock operations (using null for shop-level stock)
                product.restock(TradeType.BUY, true, null);
                product.restock(TradeType.SELL, true, null);
            } catch (Exception e) {
                // Handle restock errors gracefully
            }
        }
    }
    
    public void applyDatabaseOperations() {
        if (!priceDataToSave.isEmpty()) {
        }
        
        if (!stockDataToSave.isEmpty()) {
        }
    }
    
    @NotNull
    public Set<Shop> getShopsToUpdate() {
        return Set.copyOf(shopsToUpdate);
    }
    
    @NotNull
    public Set<Product> getProductsToUpdatePrice() {
        return Set.copyOf(productsToUpdatePrice);
    }
    
    @NotNull
    public Set<PriceData> getPriceDataToSave() {
        return Set.copyOf(priceDataToSave);
    }
    
    @NotNull
    public Set<StockData> getStockDataToSave() {
        return Set.copyOf(stockDataToSave);
    }
    
    @NotNull
    public Set<Product> getProductsToRestock() {
        return Set.copyOf(productsToRestock);
    }
}
