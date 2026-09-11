# SkyBlock Auction Mod

This Fabric client mod displays new flip recommendations from the Hypixel SkyBlock auction system directly in Minecraft chat. It consumes Kafka messages published by the flipper service and provides a clickable link to the corresponding auction house listing for each flip. The mod connects to Kafka when the client starts and shuts down the consumer gracefully when the client exits.

## Features

-   Consumes `flipper-newflip` from Kafka.
-   Displays the item name and estimated profit in Minecraft chat.
-   Opens the auction house listing with `[show]` using `/ah view <auctionUuid>`.
-   Processes at most 20 flip messages per client tick so that the chat is not flooded with messages.
-   Discards invalid messages and limits the internal queue to 500 flips.

## Requirements

-   Minecraft `26.1.2`
-   Fabric Loader `0.19.5` or newer
-   Fabric API `0.155.3+26.1.2`
-   Java 25 or newer
-   A running Kafka-compatible broker such as Redpanda
-   The locally installed artifact `io.github.jaron2668:SkyBlockSharedModels:1.0-SNAPSHOT`

The mod is intended for client use. The Kafka broker must be reachable from the Minecraft process.

## Install Shared Models

The mod uses the shared Java models to deserialize `Flip` messages. Install the artifact in the local Maven repository first:

```bash
cd lib/skyblock-shared-models
mvn clean install
```

## Build the Mod

Build the mod from its own directory with the included Gradle wrapper:

```bash
cd mod/skyblock-auction-mod
./gradlew build
```

On Windows:

```powershell
cd mod\skyblock-auction-mod
.\gradlew.bat build
```

The resulting JAR is placed in `build/libs/`. Copy it together with Fabric API into the `mods` directory of your Fabric installation.

## Kafka Configuration

By default, the mod uses:

```text
localhost:19092
```

The broker can be changed through a JVM system property or an environment variable. The system property takes precedence:

```text
-Dskyblock.kafka.bootstrap-servers=host:port
```

Alternatively:

```text
SKYBLOCK_KAFKA_BOOTSTRAP_SERVERS=host:port
```

The mod uses the `skyblock-auction-mod` consumer group and only reads new messages by default (`auto.offset.reset=latest`).

## License

See [LICENSE.txt](LICENSE.txt).

## Disclaimer

This project is not affiliated with, endorsed by, or associated with Hypixel Inc. "Hypixel" and related names are trademarks of Hypixel Inc. This is an independent community project intended for educational and personal use.
