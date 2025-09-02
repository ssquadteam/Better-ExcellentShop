package su.nightexpress.nexshop.config;

import org.bukkit.event.inventory.ClickType;
import su.nightexpress.nexshop.api.shop.type.ShopClickAction;
import su.nightexpress.nexshop.module.ModuleConfig;
import su.nightexpress.nexshop.util.ShopUtils;
import su.nightexpress.nightcore.config.ConfigValue;
import su.nightexpress.nightcore.util.Lists;
import su.nightexpress.nightcore.util.StringUtil;

import java.util.HashMap;
import java.util.Map;

public class Config {

    public static final String DIR_MENU = "/menu/";
    public static final String DIR_CARTS = "/menu/product_carts/";

    public static final ConfigValue<String> DATE_FORMAT = ConfigValue.create("General.DateFormat",
        "MM/dd/yyyy HH:mm"
    ).whenRead(ShopUtils::setDateFormatter);

    public static final ConfigValue<Boolean> CURRENCY_NEED_PERMISSION = ConfigValue.create("General.Currency_Need_Permission",
        false,
        "Controls whether players must have '" + Perms.PREFIX_CURRENCY + "[name]' permission to use specific currency for their shops and listings.",
        "[*] Useful only for ChestShop and Auction modules.",
        "[*] Currencies set as default are always allowed to use."
    );

    public static final ConfigValue<Integer> SHOP_UPDATE_INTERVAL = ConfigValue.create("General.Shop_Update_Interval",
        60,
        "Sets how often (in seconds) plugin will check shops for possible 'updates', such as:",
        "- Rotation time for Rotating Shops.",
        "- Available discounts for Static Shops.",
        "- Update time for Float prices of both, Virtual and Static Shops.",
        "Do not touch unless you know what and why you're doing.",
        "[Default is 60 seconds]"
    );

    public static final ConfigValue<Boolean> GENERAL_BUY_WITH_FULL_INVENTORY = ConfigValue.create("General.Buy_With_Full_Inventory",
        false,
        "Sets wheter players can purchase items from shop with full inventory."
    );

    public static final ConfigValue<Boolean> GENERAL_CLOSE_GUI_AFTER_TRADE = ConfigValue.create("General.Close_GUI_After_Trade",
        false,
        "Sets whether or not Shop GUI should be closed when you sold/bought items."
    );

    @Deprecated
    public static final ConfigValue<Boolean> GUI_PLACEHOLDER_API = ConfigValue.create("GUI.Use_PlaceholderAPI",
        false,
        "[Outdated. There is dedicated PlaceholderAPI setting in GUI files]"
    );

    public static final ConfigValue<Map<ClickType, ShopClickAction>> GUI_CLICK_ACTIONS = ConfigValue.create(
        "GUI.Click_Types",
        (cfg, path, def) -> {
            Map<ClickType, ShopClickAction> map = new HashMap<>();
            for (String typeRaw : cfg.getSection(path)) {
                ClickType clickType = StringUtil.getEnum(typeRaw, ClickType.class).orElse(null);
                ShopClickAction shopClick = cfg.getEnum(path + "." + typeRaw, ShopClickAction.class);
                if (clickType == null || shopClick == null) continue;

                map.put(clickType, shopClick);
            }
            return map;
        },
        (cfg, path, map) -> map.forEach((click, action) -> cfg.set(path + "." + click.name(), action.name())),
        () -> Map.of(ClickType.LEFT, ShopClickAction.BUY_SELECTION, ClickType.RIGHT, ShopClickAction.SELL_SELECTION,
            ClickType.SHIFT_LEFT, ShopClickAction.BUY_SINGLE, ClickType.SHIFT_RIGHT, ShopClickAction.SELL_SINGLE,
            ClickType.SWAP_OFFHAND, ShopClickAction.SELL_ALL),
        "Sets actions for specified clicks on products in shop GUIs.",
        "Allowed click types: https://hub.spigotmc.org/javadocs/bukkit/org/bukkit/event/inventory/ClickType.html",
        "Allowed values: " + String.join(", ", Lists.getEnums(ShopClickAction.class))
    );

    public static final ConfigValue<Integer> DATA_SAVE_INTERVAL = ConfigValue.create("Data.SaveInterval",
        5,
        "Sets how often (in seconds) modified product & shop datas will be saved to the database.",
        "Data including:",
        "- Product's price data (float, dynamic).",
        "- Product's stock data (global, player).",
        "- Shop's rotation data.",
        "[*] Data is also saved on server reboot and plugin reload.",
        "[*] You can disable it if you're on SQLite or don't care about syncing across multiple servers."
    );

    public static final ConfigValue<String> DATA_PRICE_TABLE = ConfigValue.create("Data.PriceTable",
        "price_data"
    );

    public static final ConfigValue<String> DATA_STOCKS_TABLE = ConfigValue.create("Data.StocksTable",
        "stocks"
    );

    public static final ConfigValue<String> DATA_ROTATIONS_TABLE = ConfigValue.create("Data.RotationsTable",
        "rotations"
    );

    public static final ConfigValue<Map<String, ModuleConfig>> MODULE_CONFIG = ConfigValue.forMapById("Module",
        ModuleConfig::read,
        map -> map.putAll(ModuleConfig.getDefaultConfigs()),
        "Module settings."
    );

    public static final ConfigValue<Boolean> REDIS_ENABLED = ConfigValue.create("Redis.Enabled",
        false,
        "Enable realtime synchronization over Redis pub/sub."
    );
    public static final ConfigValue<String> REDIS_HOST = ConfigValue.create("Redis.Host",
        "127.0.0.1",
        "Redis server host."
    );
    public static final ConfigValue<Integer> REDIS_PORT = ConfigValue.create("Redis.Port",
        6379,
        "Redis server port."
    );
    public static final ConfigValue<String> REDIS_PASSWORD = ConfigValue.create("Redis.Password",
        "",
        "Redis server password, leave empty if none."
    );
    public static final ConfigValue<Boolean> REDIS_SSL = ConfigValue.create("Redis.SSL",
        false,
        "Use SSL/TLS for Redis connection."
    );
    public static final ConfigValue<String> REDIS_CHANNEL = ConfigValue.create("Redis.Channel",
        "excellentshop:sync",
        "Redis pub/sub channel name used for this plugin."
    );
    public static final ConfigValue<String> REDIS_NODE_ID = ConfigValue.create("Redis.NodeId",
        "",
        "Optional node identifier. If empty, a random UUID is used at runtime."
    );

    public static boolean isRedisEnabled() {
        return REDIS_ENABLED.get();
    }
}
