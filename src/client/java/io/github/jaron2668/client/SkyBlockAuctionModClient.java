package io.github.jaron2668.client;

import net.fabricmc.api.ClientModInitializer;
import net.fabricmc.fabric.api.client.event.lifecycle.v1.ClientLifecycleEvents;
import net.fabricmc.fabric.api.client.event.lifecycle.v1.ClientTickEvents;
import net.minecraft.client.Minecraft;

public class SkyBlockAuctionModClient implements ClientModInitializer {
    private KafkaFlipConsumer flipConsumer;

    @Override
    public void onInitializeClient() {
        flipConsumer = new KafkaFlipConsumer();
        flipConsumer.start();
        ClientTickEvents.END_CLIENT_TICK.register(this::handleClientTick);

        ClientLifecycleEvents.CLIENT_STOPPING.register(client -> {
            flipConsumer.stop();
        });
    }

    private void handleClientTick(Minecraft client) {
        flipConsumer.drainMessages(client);
    }
}