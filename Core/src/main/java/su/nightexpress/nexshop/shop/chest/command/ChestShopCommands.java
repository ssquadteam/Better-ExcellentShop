package su.nightexpress.nexshop.shop.chest.command;

import org.bukkit.Material;
import org.bukkit.Tag;
import org.bukkit.block.Block;
import org.bukkit.entity.Player;
import org.bukkit.inventory.Inventory;
import org.bukkit.inventory.ItemStack;
import org.jetbrains.annotations.NotNull;
import su.nightexpress.nexshop.Placeholders;
import su.nightexpress.nexshop.ShopPlugin;
import su.nightexpress.nexshop.shop.chest.ChestShopModule;
import su.nightexpress.nexshop.shop.chest.config.ChestConfig;
import su.nightexpress.nexshop.shop.chest.config.ChestLang;
import su.nightexpress.nexshop.shop.chest.config.ChestPerms;
import su.nightexpress.nexshop.shop.chest.impl.ChestShop;
import su.nightexpress.nexshop.shop.chest.impl.ShopBlock;
import su.nightexpress.nightcore.command.experimental.CommandContext;
import su.nightexpress.nightcore.command.experimental.argument.ArgumentTypes;
import su.nightexpress.nightcore.command.experimental.argument.ParsedArguments;
import su.nightexpress.nightcore.command.experimental.builder.ChainedNodeBuilder;
import su.nightexpress.nightcore.core.config.CoreLang;
import su.nightexpress.nightcore.util.BukkitThing;
import su.nightexpress.nightcore.util.ItemUtil;
import su.nightexpress.nightcore.util.Lists;
import su.nightexpress.nightcore.util.Players;
import su.nightexpress.nightcore.util.bridge.RegistryType;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class ChestShopCommands {

    private static final Set<Material> TRANSPARENT = new HashSet<>();

    static {
        TRANSPARENT.addAll(Tag.AIR.getValues());
        TRANSPARENT.addAll(Tag.SIGNS.getValues());
    }

    public static void build(@NotNull ShopPlugin plugin, @NotNull ChestShopModule module, @NotNull ChainedNodeBuilder root) {
        root.addDirect("browse", builder -> builder
            .playerOnly()
            .permission(ChestPerms.COMMAND_BROWSE)
            .description(ChestLang.COMMAND_BROWSE_DESC.text())
            .executes((context, arguments) -> browseShops(module, context, arguments))
        );

        root.addDirect("list", builder -> builder
            .playerOnly()
            .permission(ChestPerms.COMMAND_LIST)
            .description(ChestLang.COMMAND_LIST_DESC.text())
            .executes((context, arguments) -> listShops(module, context, arguments))
        );

        root.addDirect("listall", builder -> builder
            .playerOnly()
            .permission(ChestPerms.COMMAND_LIST)
            .description(ChestLang.COMMAND_LIST_DESC.text())
            .executes((context, arguments) -> listAllShops(module, context, arguments))
        );

        root.addDirect("search", builder -> builder
            .playerOnly()
            .permission(ChestPerms.COMMAND_SEARCH)
            .description(ChestLang.COMMAND_SEARCH_DESC.text())
            .withArgument(ArgumentTypes.string(CommandArguments.ITEM_NAME).required().withSamples(context -> BukkitThing.getValues(RegistryType.MATERIAL)))
            .executes((context, arguments) -> searchShops(module, context, arguments))
        );

        root.addDirect("create", builder -> builder
            .playerOnly()
            .permission(ChestPerms.CREATE)
            .description(ChestLang.COMMAND_CREATE_DESC.text())
            .withArgument(ArgumentTypes.decimalCompactAbs(CommandArguments.BUY_PRICE)
                .localized(ChestLang.COMMAND_ARGUMENT_NAME_BUY_PRICE.text())
                .withSamples(tabContext -> Lists.newList("100", "1000"))
            )
            .withArgument(ArgumentTypes.decimalCompactAbs(CommandArguments.SELL_PRICE)
                .localized(ChestLang.COMMAND_ARGUMENT_NAME_SELL_PRICE.text())
                .withSamples(tabContext -> Lists.newList("50", "500"))
            )
            .executes((context, arguments) -> createShop(module, context, arguments))
        );

        root.addDirect("remove", builder -> builder
            .playerOnly()
            .permission(ChestPerms.REMOVE)
            .description(ChestLang.COMMAND_REMOVE_DESC.text())
            .executes((context, arguments) -> removeShop(module, context, arguments))
        );

        root.addDirect("openinv", builder -> builder
            .playerOnly()
            .permission(ChestPerms.COMMAND_OPEN_INV)
            .description(ChestLang.COMMAND_OPEN_INV_DESC.text())
            .executes((context, arguments) -> openShopInventory(module, context, arguments))
        );

        if (ChestConfig.SHOP_ITEM_CREATION_ENABLED.get()) {
            root.addDirect("giveitem", builder -> builder
                .description(ChestLang.COMMAND_GIVE_ITEM_DESC.text())
                .permission(ChestPerms.COMMAND_GIVE_ITEM)
                .withArgument(ArgumentTypes.player(CommandArguments.PLAYER).required())
                .withArgument(CommandArguments.forShopBlock(module).required())
                .withArgument(ArgumentTypes.integer(CommandArguments.AMOUNT)
                    .localized(CoreLang.COMMAND_ARGUMENT_NAME_AMOUNT.text())
                    .withSamples(context -> Lists.newList("1", "5", "10")))
                .executes((context, arguments) -> giveShopItem(module, context, arguments))
            );
        }

        if (!ChestConfig.isAutoBankEnabled()) {
            root.addDirect("bank", builder -> builder
                .playerOnly()
                .permission(ChestPerms.COMMAND_BANK)
                .description(ChestLang.COMMAND_BANK_DESC.text())
                .withArgument(ArgumentTypes.playerName(CommandArguments.PLAYER).permission(ChestPerms.COMMAND_BANK_OTHERS))
                .executes((context, arguments) -> openBank(plugin, module, context, arguments))
            );
        }
    }

    public static boolean browseShops(@NotNull ChestShopModule module, @NotNull CommandContext context, @NotNull ParsedArguments arguments) {
        Player player = context.getPlayerOrThrow();
        module.browseShopOwners(player);
        return true;
    }

    public static boolean listShops(@NotNull ChestShopModule module, @NotNull CommandContext context, @NotNull ParsedArguments arguments) {
        Player player = context.getPlayerOrThrow();
        module.browsePlayerShops(player, player.getName());
        return true;
    }

    public static boolean listAllShops(@NotNull ChestShopModule module, @NotNull CommandContext context, @NotNull ParsedArguments arguments) {
        Player player = context.getPlayerOrThrow();
        module.browseAllShops(player);
        return true;
    }

    public static boolean searchShops(@NotNull ChestShopModule module, @NotNull CommandContext context, @NotNull ParsedArguments arguments) {
        Player player = context.getPlayerOrThrow();
        String itemName = arguments.getStringArgument(CommandArguments.ITEM_NAME);
        module.browseItemShops(player, itemName);
        return true;
    }

    public static boolean createShop(@NotNull ChestShopModule module, @NotNull CommandContext context, @NotNull ParsedArguments arguments) {
        Player player = context.getPlayerOrThrow();
        Block block = player.getTargetBlock(TRANSPARENT, 100);

        double buyPrice = arguments.getDoubleArgument(CommandArguments.BUY_PRICE, ChestConfig.SHOP_PRODUCT_INITIAL_BUY_PRICE.get());
        double sellPrice = arguments.getDoubleArgument(CommandArguments.SELL_PRICE, ChestConfig.SHOP_PRODUCT_INITIAL_SELL_PRICE.get());

        return module.createShopNaturally(player, block, buyPrice, sellPrice);
    }

    public static boolean removeShop(@NotNull ChestShopModule module, @NotNull CommandContext context, @NotNull ParsedArguments arguments) {
        Player player = context.getPlayerOrThrow();
        Block block = player.getTargetBlock(null, 100);
        return module.deleteShop(player, block);
    }

    public static boolean openShopInventory(@NotNull ChestShopModule module, @NotNull CommandContext context, @NotNull ParsedArguments arguments) {
        Player player = context.getPlayerOrThrow();
        Block block = player.getTargetBlock(null, 10);

        ChestShop shop = module.getShop(block);
        if (shop == null) {
            module.getPrefixed(ChestLang.ERROR_BLOCK_IS_NOT_SHOP).send(context.getSender());
            return false;
        }

        Inventory inventory = shop.inventory();
        if (inventory == null) return false;

        player.openInventory(inventory);
        return true;
    }

    public static boolean giveShopItem(@NotNull ChestShopModule module, @NotNull CommandContext context, @NotNull ParsedArguments arguments) {
        ShopBlock shopBlock = arguments.getArgument(CommandArguments.SHOP_BLOCK, ShopBlock.class);
        Player player = arguments.getPlayerArgument(CommandArguments.PLAYER);
        int amount = arguments.getIntArgument(CommandArguments.AMOUNT, 1);

        ItemStack itemStack = shopBlock.getItemStack();
        itemStack.setAmount(amount);
        Players.addItem(player, itemStack);

        module.getPrefixed(ChestLang.COMMAND_GIVE_ITEM_DONE).send(context.getSender(), replacer -> replacer
            .replace(Placeholders.GENERIC_NAME, ItemUtil.getNameSerialized(itemStack))
            .replace(Placeholders.forPlayer(player))
        );
        return true;
    }

    public static boolean openBank(@NotNull ShopPlugin plugin, @NotNull ChestShopModule module, @NotNull CommandContext context, @NotNull ParsedArguments arguments) {
        Player player = context.getPlayerOrThrow();

        String userName = arguments.getStringArgument(CommandArguments.PLAYER, context.getSender().getName());
        plugin.getUserManager().getUserDataAsync(userName).thenAccept(user -> {
            if (user == null) {
                context.errorBadPlayer();
                return;
            }

            UUID targetId = user.getId();
            plugin.runAtEntity(player, () -> module.openBank(player, targetId));
        });
        return true;
    }
}
