package io.github.jaron2668.client;

import net.fabricmc.api.ClientModInitializer;
import net.fabricmc.fabric.api.client.command.v2.ClientCommandRegistrationCallback;
import net.fabricmc.fabric.api.client.command.v2.ClientCommands;
import net.fabricmc.fabric.api.client.event.lifecycle.v1.ClientLifecycleEvents;
import net.fabricmc.fabric.api.client.event.lifecycle.v1.ClientTickEvents;
import net.fabricmc.fabric.api.client.keymapping.v1.KeyMappingHelper;
import net.minecraft.client.KeyMapping;
import net.minecraft.client.Minecraft;
import net.minecraft.network.chat.Component;
import net.minecraft.resources.Identifier;
import org.lwjgl.glfw.GLFW;

public class SkyBlockAuctionModClient implements ClientModInitializer {
    private static final KeyMapping.Category KEY_CATEGORY = KeyMapping.Category.register(
            Identifier.fromNamespaceAndPath("skyblock-auction-mod", "general"));

    private KafkaFlipConsumer flipConsumer;
    private KeyMapping toggleFlipChatKey;

    @Override
    public void onInitializeClient() {
        flipConsumer = new KafkaFlipConsumer();
        flipConsumer.start();
        toggleFlipChatKey = KeyMappingHelper.registerKeyMapping(new KeyMapping(
                "key.skyblock-auction-mod.toggle_flip_chat",
                GLFW.GLFW_KEY_K,
                KEY_CATEGORY));

        ClientCommandRegistrationCallback.EVENT.register((dispatcher, registryAccess) -> dispatcher.register(
                ClientCommands.literal("flipchat")
                        .executes(context -> toggleFlipChat(context.getSource().getClient()))));

        ClientTickEvents.END_CLIENT_TICK.register(this::handleClientTick);

        ClientLifecycleEvents.CLIENT_STOPPING.register(client -> {
            flipConsumer.stop();
        });
    }

    private void handleClientTick(Minecraft client) {
        while (toggleFlipChatKey.consumeClick()) {
            toggleFlipChat(client);
        }

        flipConsumer.drainMessages(client);
    }

    private int toggleFlipChat(Minecraft client) {
        boolean enabled = flipConsumer.toggleChatMessages();

        if (client.player != null) {
            client.player.sendSystemMessage(Component.literal(
                    "Flip chat messages " + (enabled ? "enabled" : "disabled")));
        }

        return 1;
    }
}