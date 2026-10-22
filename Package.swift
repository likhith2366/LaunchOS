// swift-tools-version:6.0
import PackageDescription

let package = Package(
    name: "LaunchOS",
    platforms: [.macOS(.v14)],
    products: [
        .library(name: "LaunchCore", targets: ["LaunchCore"]),
        .executable(name: "launch-server", targets: ["LaunchServer"]),
        .executable(name: "launch-sim", targets: ["LaunchSim"]),
    ],
    dependencies: [
        .package(url: "https://github.com/vapor/vapor.git", from: "4.106.0"),
        .package(url: "https://github.com/vapor/postgres-nio.git", from: "1.21.0"),
        .package(url: "https://github.com/swift-server/RediStack.git", from: "1.6.0"),
        .package(url: "https://github.com/apple/swift-argument-parser.git", from: "1.4.0"),
        .package(url: "https://github.com/swift-server/async-http-client.git", from: "1.21.0"),
        .package(url: "https://github.com/apple/swift-log.git", from: "1.5.0"),
    ],
    targets: [
        .target(
            name: "LaunchCore",
            dependencies: [
                .product(name: "PostgresNIO", package: "postgres-nio"),
                .product(name: "RediStack", package: "RediStack"),
                .product(name: "Logging", package: "swift-log"),
            ],
            // schema.sql is the editable source of truth; scripts/gen-schema.sh turns it
            // into Schema.swift so migrate() needs no resource bundle at runtime.
            exclude: ["schema.sql"],
            swiftSettings: [.swiftLanguageMode(.v6)]
        ),
        .executableTarget(
            name: "LaunchServer",
            dependencies: [
                "LaunchCore",
                .product(name: "Vapor", package: "vapor"),
            ],
            swiftSettings: [.swiftLanguageMode(.v6)]
        ),
        .executableTarget(
            name: "LaunchSim",
            dependencies: [
                "LaunchCore",
                .product(name: "ArgumentParser", package: "swift-argument-parser"),
                .product(name: "AsyncHTTPClient", package: "async-http-client"),
            ],
            swiftSettings: [.swiftLanguageMode(.v6)]
        ),
        .testTarget(
            name: "LaunchCoreTests",
            dependencies: ["LaunchCore"],
            swiftSettings: [.swiftLanguageMode(.v6)]
        ),
    ]
)
