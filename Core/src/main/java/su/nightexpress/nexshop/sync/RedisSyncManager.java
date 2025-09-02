package su.nightexpress.nexshop.sync;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import org.jetbrains.annotations.NotNull;
import su.nightexpress.economybridge.EconomyBridge;
import su.nightexpress.economybridge.api.Currency;
import su.nightexpress.nexshop.ShopPlugin;
import su.nightexpress.nexshop.auction.data.AuctionDatabase;
import su.nightexpress.nexshop.auction.listing.ActiveListing;
import su.nightexpress.nexshop.auction.listing.CompletedListing;
import su.nightexpress.nexshop.config.Config;
import su.nightexpress.nexshop.data.product.PriceData;
import su.nightexpress.nexshop.data.product.StockData;
import su.nightexpress.nexshop.data.shop.RotationData;
import su.nightexpress.nexshop.shop.chest.impl.ChestBank;
import su.nightexpress.nexshop.api.shop.product.ProductType;
import su.nightexpress.nexshop.api.shop.product.typing.PhysicalTyping;
import su.nightexpress.nightcore.lib.redis.jedis.DefaultJedisClientConfig;
import su.nightexpress.nightcore.lib.redis.jedis.HostAndPort;
import su.nightexpress.nightcore.lib.redis.jedis.Jedis;
import su.nightexpress.nightcore.lib.redis.jedis.JedisPool;
import su.nightexpress.nightcore.lib.redis.jedis.JedisPubSub;
import su.nightexpress.nightcore.lib.commons.pool2.impl.GenericObjectPoolConfig;

import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Redis synchronization manager for ExcellentShop
 * Mirrors the design used in Better-ExcellentCrates and Better-CoinsEngine.
 */
public class RedisSyncManager {

    private final ShopPlugin plugin;
    private JedisPool pool;
    private JedisPubSub subscriber;
    private Thread subscriberThread;

    private final Gson gson;
    private final String nodeId;
    private String channel;
    private volatile boolean active;

    // Cross-server player names cache (optional for UX)
    private final Set<String> crossServerPlayerNames = ConcurrentHashMap.newKeySet();

    public RedisSyncManager(@NotNull ShopPlugin plugin) {
        this.plugin = plugin;
        this.gson = new GsonBuilder().setPrettyPrinting().create();

        String nid = Config.REDIS_NODE_ID.get();
        if (nid == null || nid.isBlank()) {
            nid = UUID.randomUUID().toString();
        }
        this.nodeId = nid;
    }

    public void setup() {
        if (!Config.isRedisEnabled()) return;

        String host = Config.REDIS_HOST.get();
        int port = Config.REDIS_PORT.get();
        String password = Config.REDIS_PASSWORD.get();
        boolean ssl = Config.REDIS_SSL.get();
        this.channel = Config.REDIS_CHANNEL.get();

        try {
            DefaultJedisClientConfig clientConfig = DefaultJedisClientConfig.builder()
                .password((password == null || password.isEmpty()) ? null : password)
                .ssl(ssl)
                .build();

            this.pool = new JedisPool(new GenericObjectPoolConfig<>(), new HostAndPort(host, port), clientConfig);
            this.active = true;
            this.startSubscriber();

            this.plugin.info("Redis sync enabled. Channel: " + this.channel + " | NodeId: " + this.nodeId);
        }
        catch (Exception e) {
            this.plugin.error("Failed to initialize Redis: " + e.getMessage());
            this.active = false;
        }
    }

    public void shutdown() {
        this.active = false;
        try {
            if (this.subscriber != null) this.subscriber.unsubscribe();
        } catch (Exception ignored) {}
        try {
            if (this.subscriberThread != null) this.subscriberThread.interrupt();
        } catch (Exception ignored) {}
        try {
            if (this.pool != null) this.pool.close();
        } catch (Exception ignored) {}
    }

    public boolean isActive() {
        return this.pool != null && this.active;
    }

    @NotNull
    public String getNodeId() { return this.nodeId; }

    /* =========================
       Publisher API
       ========================= */

    public void publishPriceData(@NotNull PriceData data) {
        if (!isActive()) return;
        JsonObject d = new JsonObject();
        d.addProperty("shopId", data.getShopId());
        d.addProperty("productId", data.getProductId());
        d.addProperty("latestBuyPrice", data.getLatestBuyPrice());
        d.addProperty("latestSellPrice", data.getLatestSellPrice());
        d.addProperty("latestUpdateDate", data.getLatestUpdateDate());
        d.addProperty("expireDate", data.getExpireDate());
        d.addProperty("purchases", data.getPurchases());
        d.addProperty("sales", data.getSales());
        publish("PRICE_DATA_UPSERT", d);
    }

    public void publishPriceDataDeleteByShop(@NotNull String shopId) {
        if (!isActive()) return;
        JsonObject d = new JsonObject();
        d.addProperty("shopId", shopId);
        publish("PRICE_DATA_DELETE_BY_SHOP", d);
    }

    public void publishPriceDataDeleteByProduct(@NotNull String shopId, @NotNull String productId) {
        if (!isActive()) return;
        JsonObject d = new JsonObject();
        d.addProperty("shopId", shopId);
        d.addProperty("productId", productId);
        publish("PRICE_DATA_DELETE_BY_PRODUCT", d);
    }

    public void publishStockData(@NotNull StockData data) {
        if (!isActive()) return;
        JsonObject d = new JsonObject();
        d.addProperty("shopId", data.getShopId());
        d.addProperty("productId", data.getProductId());
        d.addProperty("holder", data.getHolder());
        d.addProperty("buyStock", data.getBuyStock());
        d.addProperty("sellStock", data.getSellStock());
        d.addProperty("restockDate", data.getRestockDate());
        publish("STOCK_DATA_UPSERT", d);
    }

    public void publishStockDataDeleteByShop(@NotNull String shopId) {
        if (!isActive()) return;
        JsonObject d = new JsonObject();
        d.addProperty("shopId", shopId);
        publish("STOCK_DATA_DELETE_BY_SHOP", d);
    }

    public void publishStockDataDeleteByProduct(@NotNull String shopId, @NotNull String productId) {
        if (!isActive()) return;
        JsonObject d = new JsonObject();
        d.addProperty("shopId", shopId);
        d.addProperty("productId", productId);
        publish("STOCK_DATA_DELETE_BY_PRODUCT", d);
    }

    public void publishRotationData(@NotNull RotationData data) {
        if (!isActive()) return;
        JsonObject d = new JsonObject();
        d.addProperty("shopId", data.getShopId());
        d.addProperty("rotationId", data.getRotationId());
        d.addProperty("nextRotationDate", data.getNextRotationDate());
        d.add("products", gson.toJsonTree(data.getProducts()));
        publish("ROTATION_DATA_UPSERT", d);
    }

    public void publishRotationDataDeleteByShop(@NotNull String shopId) {
        if (!isActive()) return;
        JsonObject d = new JsonObject();
        d.addProperty("shopId", shopId);
        publish("ROTATION_DATA_DELETE_BY_SHOP", d);
    }

    public void publishRotationDataDeleteByRotation(@NotNull String shopId, @NotNull String rotationId) {
        if (!isActive()) return;
        JsonObject d = new JsonObject();
        d.addProperty("shopId", shopId);
        d.addProperty("rotationId", rotationId);
        publish("ROTATION_DATA_DELETE_BY_ROTATION", d);
    }

    public void publishChestBank(@NotNull ChestBank bank) {
        if (!isActive()) return;
        JsonObject d = new JsonObject();
        d.addProperty("holder", bank.getHolder().toString());
        d.add("balanceMap", gson.toJsonTree(bank.getBalanceMap()));
        publish("CHEST_BANK_UPSERT", d);
    }

    public void publishAuctionListingAdd(@NotNull ActiveListing listing) {
        if (!isActive()) return;
        JsonObject d = new JsonObject();
        d.addProperty("id", listing.getId().toString());
        d.addProperty("owner", listing.getOwner().toString());
        d.addProperty("ownerName", listing.getOwnerName());
        // Serialize typing
        PhysicalTyping typing = listing.getTyping();
        String typingSerialized = AuctionDatabase.typingToJson(typing);
        ProductType type = typing.type();
        d.addProperty("typingType", type.name());
        d.addProperty("typingData", typingSerialized == null ? "" : typingSerialized);
        // Currency and price
        d.addProperty("currency", listing.getCurrency().getInternalId());
        d.addProperty("price", listing.getPrice());
        d.addProperty("creationDate", listing.getCreationDate());
        d.addProperty("expireDate", listing.getExpireDate());
        d.addProperty("deletionDate", listing.getDeleteDate());
        publish("AUCTION_LISTING_ADD", d);
    }

    public void publishAuctionListingDelete(@NotNull UUID listingId) {
        if (!isActive()) return;
        JsonObject d = new JsonObject();
        d.addProperty("id", listingId.toString());
        publish("AUCTION_LISTING_DELETE", d);
    }

    public void publishAuctionCompletedAdd(@NotNull CompletedListing listing) {
        if (!isActive()) return;
        JsonObject d = new JsonObject();
        d.addProperty("id", listing.getId().toString());
        d.addProperty("owner", listing.getOwner().toString());
        d.addProperty("ownerName", listing.getOwnerName());
        d.addProperty("buyerName", listing.getBuyerName());
        // typing
        PhysicalTyping typing = listing.getTyping();
        String typingSerialized = AuctionDatabase.typingToJson(typing);
        ProductType type = typing.type();
        d.addProperty("typingType", type.name());
        d.addProperty("typingData", typingSerialized == null ? "" : typingSerialized);
        // currency, price and dates
        d.addProperty("currency", listing.getCurrency().getInternalId());
        d.addProperty("price", listing.getPrice());
        d.addProperty("creationDate", listing.getCreationDate());
        d.addProperty("buyDate", listing.getBuyDate());
        d.addProperty("deletionDate", listing.getDeleteDate());
        d.addProperty("claimed", listing.isClaimed());
        publish("AUCTION_COMPLETED_ADD", d);
    }

    public void publishAuctionCompletedUpdate(@NotNull UUID id, boolean claimed) {
        if (!isActive()) return;
        JsonObject d = new JsonObject();
        d.addProperty("id", id.toString());
        d.addProperty("claimed", claimed);
        publish("AUCTION_COMPLETED_UPDATE", d);
    }

    public void publishAuctionCompletedDelete(@NotNull UUID id) {
        if (!isActive()) return;
        JsonObject d = new JsonObject();
        d.addProperty("id", id.toString());
        publish("AUCTION_COMPLETED_DELETE", d);
    }

    public void publishPlayerNames(@NotNull Set<String> playerNames) {
        if (!isActive()) return;
        JsonObject data = new JsonObject();
        JsonArray namesArray = new JsonArray();
        playerNames.forEach(namesArray::add);
        data.add("playerNames", namesArray);
        data.addProperty("timestamp", System.currentTimeMillis());
        publish("PLAYER_NAMES_UPDATE", data);
    }

    private void publish(@NotNull String type, @NotNull JsonObject data) {
        if (!isActive()) return;
        JsonObject root = new JsonObject();
        root.addProperty("type", type);
        root.addProperty("nodeId", this.nodeId);
        root.add("data", data);

        this.plugin.getFoliaScheduler().runAsync(() -> {
            try (Jedis jedis = this.pool.getResource()) {
                jedis.publish(this.channel, this.gson.toJson(root));
            }
            catch (Exception e) {
                this.plugin.warn("Redis publish failed: " + e.getMessage());
            }
        });
    }

    /* =========================
       Subscriber
       ========================= */

    private void startSubscriber() {
        this.subscriber = new JedisPubSub() {
            @Override
            public void onMessage(String channel, String message) { handleIncoming(message); }
        };

        this.subscriberThread = new Thread(() -> {
            try (Jedis jedis = this.pool.getResource()) {
                jedis.subscribe(this.subscriber, this.channel);
            }
            catch (Exception e) {
                this.plugin.error("Redis subscriber error: " + e.getMessage());
            }
            finally {
                this.active = false;
            }
        }, "ExcellentShop-RedisSubscriber");

        this.subscriberThread.setDaemon(true);
        this.subscriberThread.start();

        // Periodic player names sharing (optional)
        this.plugin.getFoliaScheduler().runTimerAsync(this::syncPlayerNames, 0L, 600L);
    }

    private void handleIncoming(@NotNull String message) {
        try {
            JsonObject root = gson.fromJson(message, JsonObject.class);
            if (root == null) return;

            String sourceNodeId = root.has("nodeId") && !root.get("nodeId").isJsonNull() ? root.get("nodeId").getAsString() : null;
            if (sourceNodeId != null && sourceNodeId.equals(this.nodeId)) return;

            String type = root.get("type").getAsString();
            JsonObject data = root.getAsJsonObject("data");
            if (type == null || data == null) return;

            switch (type) {
                case "PRICE_DATA_UPSERT" -> applyPriceDataUpsert(data);
                case "PRICE_DATA_DELETE_BY_SHOP" -> applyPriceDataDeleteByShop(data);
                case "PRICE_DATA_DELETE_BY_PRODUCT" -> applyPriceDataDeleteByProduct(data);

                case "STOCK_DATA_UPSERT" -> applyStockDataUpsert(data);
                case "STOCK_DATA_DELETE_BY_SHOP" -> applyStockDataDeleteByShop(data);
                case "STOCK_DATA_DELETE_BY_PRODUCT" -> applyStockDataDeleteByProduct(data);

                case "ROTATION_DATA_UPSERT" -> applyRotationDataUpsert(data);
                case "ROTATION_DATA_DELETE_BY_SHOP" -> applyRotationDataDeleteByShop(data);
                case "ROTATION_DATA_DELETE_BY_ROTATION" -> applyRotationDataDeleteByRotation(data);

                case "CHEST_BANK_UPSERT" -> applyChestBankUpsert(data);

                case "AUCTION_LISTING_ADD" -> applyAuctionListingAdd(data);
                case "AUCTION_LISTING_DELETE" -> applyAuctionListingDelete(data);
                case "AUCTION_COMPLETED_ADD" -> applyAuctionCompletedAdd(data);
                case "AUCTION_COMPLETED_UPDATE" -> applyAuctionCompletedUpdate(data);
                case "AUCTION_COMPLETED_DELETE" -> applyAuctionCompletedDelete(data);

                case "PLAYER_NAMES_UPDATE" -> applyPlayerNamesUpdate(data);
                default -> {}
            }
        }
        catch (Exception e) {
            this.plugin.warn("Failed to handle Redis message: " + e.getMessage());
        }
    }

    private void applyPriceDataUpsert(@NotNull JsonObject d) {
        String shopId = d.get("shopId").getAsString();
        String productId = d.get("productId").getAsString();
        double latestBuyPrice = d.get("latestBuyPrice").getAsDouble();
        double latestSellPrice = d.get("latestSellPrice").getAsDouble();
        long latestUpdateDate = d.get("latestUpdateDate").getAsLong();
        long expireDate = d.get("expireDate").getAsLong();
        int purchases = d.get("purchases").getAsInt();
        int sales = d.get("sales").getAsInt();

        PriceData data = new PriceData(shopId, productId, latestBuyPrice, latestSellPrice, latestUpdateDate, expireDate, purchases, sales);
        this.plugin.runNextTick(() -> this.plugin.getDataManager().applyExternalPriceData(data));
    }

    private void applyPriceDataDeleteByShop(@NotNull JsonObject d) {
        String shopId = d.get("shopId").getAsString();
        this.plugin.runNextTick(() -> this.plugin.getDataManager().applyExternalDeletePriceDataByShop(shopId));
    }

    private void applyPriceDataDeleteByProduct(@NotNull JsonObject d) {
        String shopId = d.get("shopId").getAsString();
        String productId = d.get("productId").getAsString();
        this.plugin.runNextTick(() -> this.plugin.getDataManager().applyExternalDeletePriceDataByProduct(shopId, productId));
    }

    private void applyStockDataUpsert(@NotNull JsonObject d) {
        String shopId = d.get("shopId").getAsString();
        String productId = d.get("productId").getAsString();
        String holder = d.get("holder").getAsString();
        int buyStock = d.get("buyStock").getAsInt();
        int sellStock = d.get("sellStock").getAsInt();
        long restockDate = d.get("restockDate").getAsLong();

        StockData data = new StockData(shopId, productId, holder, buyStock, sellStock, restockDate);
        this.plugin.runNextTick(() -> this.plugin.getDataManager().applyExternalStockData(data));
    }

    private void applyStockDataDeleteByShop(@NotNull JsonObject d) {
        String shopId = d.get("shopId").getAsString();
        this.plugin.runNextTick(() -> this.plugin.getDataManager().applyExternalDeleteStockDataByShop(shopId));
    }

    private void applyStockDataDeleteByProduct(@NotNull JsonObject d) {
        String shopId = d.get("shopId").getAsString();
        String productId = d.get("productId").getAsString();
        this.plugin.runNextTick(() -> this.plugin.getDataManager().applyExternalDeleteStockDataByProduct(shopId, productId));
    }

    private void applyRotationDataUpsert(@NotNull JsonObject d) {
        String shopId = d.get("shopId").getAsString();
        String rotationId = d.get("rotationId").getAsString();
        long nextRotationDate = d.get("nextRotationDate").getAsLong();
        Type mapType = new TypeToken<Map<Integer, List<String>>>(){}.getType();
        Map<Integer, List<String>> products = gson.fromJson(d.get("products"), mapType);
        if (products == null) products = new HashMap<>();
        RotationData data = new RotationData(shopId, rotationId, nextRotationDate, products);
        RotationData finalData = data;
        this.plugin.runNextTick(() -> this.plugin.getDataManager().applyExternalRotationData(finalData));
    }

    private void applyRotationDataDeleteByShop(@NotNull JsonObject d) {
        String shopId = d.get("shopId").getAsString();
        this.plugin.runNextTick(() -> this.plugin.getDataManager().applyExternalDeleteRotationDataByShop(shopId));
    }

    private void applyRotationDataDeleteByRotation(@NotNull JsonObject d) {
        String shopId = d.get("shopId").getAsString();
        String rotationId = d.get("rotationId").getAsString();
        this.plugin.runNextTick(() -> this.plugin.getDataManager().applyExternalDeleteRotationDataByRotation(shopId, rotationId));
    }

    private void applyChestBankUpsert(@NotNull JsonObject d) {
        UUID holder = UUID.fromString(d.get("holder").getAsString());
        Type mapType = new TypeToken<Map<String, Double>>() {}.getType();
        Map<String, Double> balances = gson.fromJson(d.get("balanceMap"), mapType);
        if (balances == null) balances = new HashMap<>();
        ChestBank bank = new ChestBank(holder, balances);
        this.plugin.runNextTick(() -> {
            var module = this.plugin.getChestShop();
            if (module != null) {
                module.getBankMap().put(holder, bank);
            }
        });
    }

    private void applyAuctionListingAdd(@NotNull JsonObject d) {
        try {
            UUID id = UUID.fromString(d.get("id").getAsString());
            UUID owner = UUID.fromString(d.get("owner").getAsString());
            String ownerName = d.get("ownerName").getAsString();
            String typingType = d.get("typingType").getAsString();
            String typingData = d.get("typingData").getAsString();
            ProductType productType = ProductType.valueOf(typingType);
            PhysicalTyping typing = (PhysicalTyping) AuctionDatabase.typingFromJson(productType, typingData);
            Currency currency = EconomyBridge.getCurrencyOrDummy(d.get("currency").getAsString());
            double price = d.get("price").getAsDouble();
            long creationDate = d.get("creationDate").getAsLong();
            long expireDate = d.get("expireDate").getAsLong();
            long deletionDate = d.get("deletionDate").getAsLong();

            ActiveListing listing = new ActiveListing(id, owner, ownerName, typing, currency, price, creationDate, expireDate, deletionDate);
            this.plugin.runNextTick(() -> {
                var auc = this.plugin.getAuction();
                if (auc != null) {
                    auc.getListings().add(listing);
                }
            });
        } catch (Exception ignored) {}
    }

    private void applyAuctionListingDelete(@NotNull JsonObject d) {
        UUID id = UUID.fromString(d.get("id").getAsString());
        this.plugin.runNextTick(() -> {
            var auc = this.plugin.getAuction();
            if (auc != null) {
                ActiveListing existing = auc.getListings().getById(id);
                if (existing != null) auc.getListings().remove(existing);
            }
        });
    }

    private void applyAuctionCompletedAdd(@NotNull JsonObject d) {
        try {
            UUID id = UUID.fromString(d.get("id").getAsString());
            UUID owner = UUID.fromString(d.get("owner").getAsString());
            String ownerName = d.get("ownerName").getAsString();
            String buyerName = d.get("buyerName").getAsString();
            ProductType productType = ProductType.valueOf(d.get("typingType").getAsString());
            String typingData = d.get("typingData").getAsString();
            PhysicalTyping typing = (PhysicalTyping) AuctionDatabase.typingFromJson(productType, typingData);
            Currency currency = EconomyBridge.getCurrencyOrDummy(d.get("currency").getAsString());
            double price = d.get("price").getAsDouble();
            long creationDate = d.get("creationDate").getAsLong();
            long buyDate = d.get("buyDate").getAsLong();
            long deletionDate = d.get("deletionDate").getAsLong();
            boolean claimed = d.get("claimed").getAsBoolean();

            CompletedListing listing = new CompletedListing(id, owner, ownerName, buyerName, typing, currency, price, creationDate, buyDate, deletionDate, claimed);
            this.plugin.runNextTick(() -> {
                var auc = this.plugin.getAuction();
                if (auc != null) {
                    auc.getListings().addCompleted(listing);
                }
            });
        } catch (Exception ignored) {}
    }

    private void applyAuctionCompletedUpdate(@NotNull JsonObject d) {
        UUID id = UUID.fromString(d.get("id").getAsString());
        boolean claimed = d.get("claimed").getAsBoolean();
        this.plugin.runNextTick(() -> {
            var auc = this.plugin.getAuction();
            if (auc != null) {
                var existing = auc.getListings().getCompletedById(id);
                if (existing != null) {
                    existing.setClaimed(claimed);
                }
            }
        });
    }

    private void applyAuctionCompletedDelete(@NotNull JsonObject d) {
        UUID id = UUID.fromString(d.get("id").getAsString());
        this.plugin.runNextTick(() -> {
            var auc = this.plugin.getAuction();
            if (auc != null) {
                var existing = auc.getListings().getCompletedById(id);
                if (existing != null) auc.getListings().removeCompleted(existing);
            }
        });
    }

    private void syncPlayerNames() {
        if (!isActive()) return;
        Set<String> local = new HashSet<>();
        this.plugin.getServer().getOnlinePlayers().forEach(p -> local.add(p.getName()));
        if (!local.isEmpty()) publishPlayerNames(local);
    }

    private void applyPlayerNamesUpdate(@NotNull JsonObject data) {
        JsonArray namesArray = data.getAsJsonArray("playerNames");
        synchronized (this.crossServerPlayerNames) {
            this.crossServerPlayerNames.clear();
            for (int i = 0; i < namesArray.size(); i++) {
                this.crossServerPlayerNames.add(namesArray.get(i).getAsString());
            }
        }
    }

    @NotNull
    public Set<String> getAllPlayerNames() {
        Set<String> all = new HashSet<>();
        this.plugin.getServer().getOnlinePlayers().forEach(p -> all.add(p.getName()));
        synchronized (this.crossServerPlayerNames) { all.addAll(this.crossServerPlayerNames); }
        return all;
    }
}
