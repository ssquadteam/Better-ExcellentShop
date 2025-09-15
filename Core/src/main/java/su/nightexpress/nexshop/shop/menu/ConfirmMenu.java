package su.nightexpress.nexshop.shop.menu;

import org.bukkit.inventory.Inventory;
import org.bukkit.inventory.InventoryView;
import org.bukkit.inventory.MenuType;
import org.jetbrains.annotations.NotNull;
import su.nightexpress.nexshop.ShopPlugin;
import su.nightexpress.nexshop.config.Config;
import su.nightexpress.nightcore.config.FileConfig;
import su.nightexpress.nightcore.ui.menu.MenuViewer;
import su.nightexpress.nightcore.ui.menu.data.ConfigBased;
import su.nightexpress.nightcore.ui.menu.data.MenuLoader;
import su.nightexpress.nightcore.ui.menu.item.ItemHandler;
import su.nightexpress.nightcore.ui.menu.type.LinkedMenu;
import su.nightexpress.nightcore.util.bukkit.NightItem;

import static su.nightexpress.nightcore.util.Placeholders.*;
import static su.nightexpress.nightcore.util.text.tag.Tags.*;

@SuppressWarnings("UnstableApiUsage")
@Deprecated
public class ConfirmMenu extends LinkedMenu<ShopPlugin, Confirmation> implements ConfigBased {

    public static final String FILE_NAME = "confirmation.yml";

    public ConfirmMenu(@NotNull ShopPlugin plugin) {
        super(plugin, MenuType.HOPPER, BLACK.enclose("Are you sure?"));

        this.load(FileConfig.loadOrExtract(plugin, Config.DIR_MENU, FILE_NAME));
    }

    @Override
    protected void onPrepare(@NotNull MenuViewer viewer, @NotNull InventoryView view) {

    }

    @Override
    protected void onReady(@NotNull MenuViewer viewer, @NotNull Inventory inventory) {

    }

    @Override
    public void loadConfiguration(@NotNull FileConfig config, @NotNull MenuLoader loader) {
        loader.addDefaultItem(NightItem.asCustomHead(SKIN_WRONG_MARK)
            .setDisplayName(LIGHT_RED.enclose(BOLD.enclose("Cancel")))
            .toMenuItem()
            .setPriority(10)
            .setSlots(0)
            .setHandler(new ItemHandler("decline", (viewer, event) -> {
                this.getLink(viewer).returnBack(viewer, event);
            }))
        );

        loader.addDefaultItem(NightItem.asCustomHead(SKIN_CHECK_MARK)
            .setDisplayName(LIGHT_GREEN.enclose(BOLD.enclose("Accept")))
            .toMenuItem()
            .setPriority(10)
            .setSlots(4)
            .setHandler(new ItemHandler("accept", (viewer, event) -> {
                Confirmation confirmation = this.getLink(viewer);
                confirmation.onAccept(viewer, event);
                this.runNextTick(() -> confirmation.returnBack(viewer, event));
            }))
        );
    }
}
