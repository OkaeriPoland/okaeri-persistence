# Okaeri Persistence

![License](https://img.shields.io/github/license/OkaeriPoland/okaeri-persistence?style=for-the-badge&color=blue)
[![Discord](https://img.shields.io/discord/589089838200913930?style=for-the-badge&logo=discord&color=blue)](https://discord.gg/hASN5eX)

Object Document Mapping (ODM) library for Java - write your data layer once, run it anywhere. MongoDB today, PostgreSQL tomorrow, in-memory for tests.

## Features

- **Write Once, Run Anywhere**: Swap databases with one line - the core Java philosophy (without the XML hell)
- **Fluent Query API**: Basic filtering, ordering, and pagination (MongoDB/PostgreSQL; other backends coming soon)
- **Repository Pattern**: Annotate fields and get auto-implemented finders - simple but effective
- **Unified Indexing**: Declare indexes once, backends create native indexes when supported or emulate them
- **Document-Based**: Store data as JSON/YAML documents - flexible but not schema-free
- **Streaming Support**: Process large datasets with Java streams and automatic batching

## The Philosophy (and the Pitfalls)

**The Good**: Write your persistence code once against our interface, switch from MongoDB to PostgreSQL without changing application code. Your dev team can use H2, staging uses PostgreSQL, production uses MongoDB. Same code.

**The Catch**: You're trading database-specific optimizations for portability. Need MongoDB's aggregation pipeline? You'll have to fetch and process in Java. Need PostgreSQL's full-text search? Same deal. This library is for when you value flexibility and developer velocity over squeezing every bit of performance from your database.

**Good For**: Apps where data naturally clusters around an ID (user profiles, game state, session data), rapid prototyping, when you want to defer the database choice.

**Not Good For**: Complex joins, analytical queries, when you need database-specific features, when you need every bit of performance.

## Requirements

### Java
- **Java 8 or higher** for library code
- **Java 21** for running tests (but your app can use Java 8)

### Backends

Pick one (or multiple):

**Native Document Support:**

| Backend | Artifact | Description |
|---------|----------|-------------|
| **MongoDB** | `okaeri-persistence-mongo` | Uses the official MongoDB driver. Native document store with automatic index creation and native filtering by properties. Best for pure document workloads and horizontal scaling. |
| **PostgreSQL** | `okaeri-persistence-jdbc` | Uses the official PostgreSQL JDBC driver with HikariCP. Stores documents as JSONB with native GIN indexes and JSONB operators for filtering. ACID guarantees and excellent query performance. |

**Emulated/Workaround Storage:**

| Backend | Artifact | Description |
|---------|----------|-------------|
| **Redis** | `okaeri-persistence-redis` | Uses Lettuce client. Stores JSON as strings with Lua script-based filtering and hash/set secondary indexes. Blazing fast key-value access, supports TTL. Filtering is slower than native document stores. |
| **MariaDB/MySQL** | `okaeri-persistence-jdbc` | Uses HikariCP with MySQL/MariaDB. Stores documents using native JSON datatype with json_extract for filtering. Emulated indexes in separate table. Slower JSON queries than PostgreSQL. |
| **H2** | `okaeri-persistence-jdbc` | Uses HikariCP with H2 in MySQL mode. Stores JSON as text with basic string filtering (instr). Emulated indexes in separate table. Good for embedded use and testing. |
| **Flat Files** | `okaeri-persistence-flat` | File-based storage using any okaeri-configs format (YAML/JSON/HOCON). In-memory or file-based indexes. Perfect for small servers, config files, or when you don't want a database. |
| **In-Memory** | `okaeri-persistence-core` | Pure in-memory storage with HashMap-based indexes. Zero persistence, excellent performance. Great for testing, temporary state, or volatile data like game sessions. |

## Installation

![Version](https://img.shields.io/badge/version-3.0.1--beta.5-blue.svg?style=for-the-badge)
![Java](https://img.shields.io/badge/java-8%2B-blue.svg?style=for-the-badge)

### Maven
```xml
<repositories>
    <repository>
        <id>okaeri-releases</id>
        <url>https://repo.okaeri.cloud/releases</url>
    </repository>
</repositories>
```
```xml
<dependency>
    <groupId>eu.okaeri</groupId>
    <artifactId>okaeri-persistence-mongo</artifactId>
    <version>3.0.1-beta.6</version>
</dependency>
```

### Gradle (Kotlin DSL)
```kotlin
repositories {
    maven("https://repo.okaeri.cloud/releases")
}
```
```kotlin
dependencies {
    implementation("eu.okaeri:okaeri-persistence-mongo:3.0.1-beta.6")
}
```

**Replace `mongo` with:** `jdbc`, `redis`, `flat` depending on your backend.

## Quick Start

### 1. Define Your Document

```java
@Data
public class User extends Document {
    private String name;
    private int level;
    private Instant lastLogin;
    private List<String> achievements;
}
```

### 2. Create a Repository

```java
@DocumentCollection(
    path = "users",
    keyLength = 36,
    indexes = {
        @DocumentIndex(path = "name", maxLength = 32),
        @DocumentIndex(path = "level")
    }
)
public interface UserRepository extends DocumentRepository<UUID, User> {

    @DocumentPath("name")
    Optional<User> findByName(String name);

    @DocumentPath("level")
    Stream<User> streamByLevel(int level);
}
```

### 3. Use It

```java
import static eu.okaeri.persistence.filter.OrderBy.*;
import static eu.okaeri.persistence.filter.condition.Condition.*;
import static eu.okaeri.persistence.filter.predicate.SimplePredicate.*;

// Setup (MongoDB example - swap for any backend)
MongoClient mongo = MongoClients.create("mongodb://localhost");
DocumentPersistence persistence = new DocumentPersistence(
    new MongoPersistence(mongo, "mydb"),
    JsonSimpleConfigurer::new
);

// Create repository (convenience method)
UserRepository users = persistence.createRepository(UserRepository.class);

// Advanced: manual approach for custom ClassLoader or collection customization
// PersistenceCollection collection = PersistenceCollection.of(UserRepository.class);
// persistence.registerCollection(collection);
// UserRepository users = RepositoryDeclaration.of(UserRepository.class)
//     .newProxy(persistence, collection, customClassLoader);

// Create (UUID auto-generated on save)
User alice = new User();
alice.setName("alice");
alice.setLevel(42);
alice.setAchievements(List.of("speedrun", "pacifist"));
users.save(alice);

// Find by ID
User found = users.findByPath(alice.getPath()).orElseThrow();

// Find by indexed field (auto-implemented @DocumentPath method)
User byName = users.findByName("alice").orElseThrow();

// Query with filtering and ordering
List<User> topPlayers = users.find(q -> q
  .where(on("level", gt(10)))
  .orderBy(desc("level"), asc("name"))
  .limit(10))
  .collect(Collectors.toList());

// Stream processing
users.streamByLevel(42)
  .filter(u -> u.getAchievements().size() > 1)
  .forEach(u -> System.out.println(u.getName()));
```

## Query API

The `find()` method takes a lambda that builds a query and returns a Stream (MongoDB and PostgreSQL only):

```java
// Filtering
List<User> users = userRepo.find(q -> q
  .where(on("level", gt(10))))
  .collect(Collectors.toList());

// Multiple conditions
List<User> users = userRepo.find(q -> q
  .where(and(
    on("level", gte(10)),
    on("lastLogin", gt(yesterday)))))
  .collect(Collectors.toList());

// Ordering (single or multiple)
List<User> users = userRepo.find(q -> q
  .orderBy(desc("level")))
  .collect(Collectors.toList());

List<User> users = userRepo.find(q -> q
  .orderBy(desc("score"), asc("name")))
  .collect(Collectors.toList());

// Nested properties
List<Profile> profiles = profileRepo.find(q -> q
  .where(on("address.city", eq("London")))
  .orderBy(asc("profile.age")))
  .collect(Collectors.toList());

// Pagination
List<User> users = userRepo.find(q -> q
  .where(on("active", eq(true)))
  .orderBy(desc("score"))
  .skip(20)
  .limit(10))
  .collect(Collectors.toList()); // Page 3 of results

// All together
List<User> results = userRepo.find(q -> q
  .where(and(
    on("level", between(10, 50)),
    on("banned", eq(false))))
  .orderBy(desc("level"), asc("name"))
  .skip(0)
  .limit(25))
  .collect(Collectors.toList());
```

**Supported Operators**: `eq`, `gt`, `gte`, `lt`, `lte`, `between`, `in`, `and`, `or`

**Backend Support**: Native query translation currently available for:
- **MongoDB** → Native queries with `$gt`, `$and`, etc.
- **PostgreSQL** → JSONB operators (`->`, `->>`) with GIN indexes

**Note**: Redis, Flat Files, and In-Memory backends will support `find()` with in-memory filtering in a future release. For now, use `@DocumentPath` methods or `streamAll()` with Java stream filters for these backends.

## Repository Methods

Annotate methods with `@DocumentPath` and they're auto-implemented (works for any field, but indexing recommended for performance):

```java
@DocumentCollection(path = "players", keyLength = 36, indexes = {
    @DocumentIndex(path = "username", maxLength = 16),
    @DocumentIndex(path = "rank", maxLength = 32)
})
public interface PlayerRepository extends DocumentRepository<UUID, Player> {

    // Returns Optional<Player>
    @DocumentPath("username")
    Optional<Player> findByUsername(String username);

    // Returns Stream<Player>
    @DocumentPath("rank")
    Stream<Player> streamByRank(String rank);

    // Returns List<Player>
    @DocumentPath("rank")
    List<Player> listByRank(String rank);

    // Nested properties
    @DocumentPath("stats.level")
    Stream<Player> streamByStatsLevel(int level);

    // Custom logic
    default boolean isUsernameTaken(String username) {
        return findByUsername(username).isPresent();
    }

    default Player getOrCreate(UUID id, String username) {
        return findByPath(id).orElseGet(() -> {
            Player p = new Player();
            p.setPath(id);
            p.setUsername(username);
            return save(p);
        });
    }
}
```

**Built-in Methods** (from `DocumentRepository`):

```java
// Metadata
DocumentPersistence getPersistence()
PersistenceCollection getCollection()
Class<? extends Document> getDocumentType()

// Counting
long count()

// Finding - by path
Optional<T> findByPath(PATH path)
T findOrCreateByPath(PATH path)
Collection<T> findAll()
Collection<T> findAllByPath(Iterable<PATH> paths)
Collection<T> findOrCreateAllByPath(Iterable<PATH> paths)
Stream<T> streamAll()

// Finding - with queries (MongoDB/PostgreSQL only)
Stream<T> find(FindFilter filter)
Stream<T> find(Function<FindFilterBuilder, FindFilterBuilder> function)
Stream<T> find(Condition condition)
Optional<T> findOne(Condition condition)

// Saving
T save(T document)
Iterable<T> saveAll(Iterable<T> documents)

// Deleting - by path
boolean deleteByPath(PATH path)
long deleteAllByPath(Iterable<PATH> paths)
boolean deleteAll()

// Deleting - with queries (MongoDB/PostgreSQL only)
long delete(DeleteFilter filter)
long delete(Function<DeleteFilterBuilder, DeleteFilterBuilder> function)

// Existence
boolean existsByPath(PATH path)
```

## Switching Backends

Change one line, everything else stays the same:

```java
// MongoDB
new DocumentPersistence(new MongoPersistence(mongoClient, "mydb"), JsonSimpleConfigurer::new);

// PostgreSQL
new DocumentPersistence(new PostgresPersistence(hikariDataSource), JsonSimpleConfigurer::new);

// Redis
new DocumentPersistence(new RedisPersistence(redisClient), JsonSimpleConfigurer::new);

// Flat files (YAML/JSON/HOCON)
new DocumentPersistence(new FlatPersistence(new File("./data"), ".yml", YamlBukkitConfigurer::new), YamlBukkitConfigurer::new);

// In-memory (volatile, no persistence)
new DocumentPersistence(new InMemoryDocumentPersistence(), JsonSimpleConfigurer::new);
```

**Namespace support**: Add `PersistencePath.of("prefix")` as first parameter to prevent collection name conflicts when multiple apps share storage (e.g., `new MongoPersistence(PersistencePath.of("app"), mongoClient, "mydb")`).

Your repositories, queries, and business logic stay the same.

## Indexing

Declare indexes once in your `@DocumentCollection`:

```java
@DocumentCollection(
    path = "users",
    keyLength = 36,
    indexes = {
        @DocumentIndex(path = "username", maxLength = 32),
        @DocumentIndex(path = "email", maxLength = 128),
        @DocumentIndex(path = "profile.age"),
        @DocumentIndex(path = "settings.notifications.email")
    }
)
```

What happens:
- **MongoDB**: Creates native `db.collection.createIndex()`
- **PostgreSQL**: Creates GIN indexes on JSONB paths
- **Redis**: Maintains hash-based secondary indexes with Lua scripts
- **Flat/Memory**: Builds in-memory maps for O(1) lookups

**Tradeoff**: Indexes speed up reads but slow down writes and use more memory/storage. Choose wisely.

## Streaming Large Datasets

Process large datasets with automatic batching:

```java
// Stream all users (fetched in batches automatically)
userRepository.streamAll()
  .filter(u -> u.getLevel() > 50)
  .map(User::getName)
  .forEach(System.out::println);

// Custom queries return streams
userRepository.find(q -> q.where(on("active", eq(true))))
  .parallel() // Process in parallel
  .map(this::calculateStats)
  .collect(Collectors.toList());
```

Backend-specific optimizations:
- **MongoDB**: Uses cursors with configurable batch size
- **PostgreSQL**: Streams result set without loading everything into memory
- **Others**: Fetch and stream from storage

## Advanced: Document References

Store references to other documents:

```java
public class Post extends Document {
    private String title;
    private String content;
    private Ref<User> author; // Reference to User document

    public User getAuthor() {
        return author.fetch(); // Lazy-loads when accessed
    }
}

// Usage
Post post = postRepository.findByPath(postId).orElseThrow();
User author = post.getAuthor(); // Fetches user document
System.out.println("Posted by: " + author.getName());
```

## Real-World Example

Complete user management system:

```java
// Document model
@Data
public class UserAccount extends Document {
    private String email;
    private String username;
    private UserProfile profile;
    private String role; // e.g., "USER", "ADMIN", "MODERATOR"
    private Instant createdAt;
    private Instant lastLogin;
}

@Data
public class UserProfile {
    private String displayName;
    private String bio;
    private String avatarUrl;
    private Map<String, Object> preferences;
}

// Repository
@DocumentCollection(
    path = "accounts",
    keyLength = 36,
    indexes = {
        @DocumentIndex(path = "email", maxLength = 128),
        @DocumentIndex(path = "username", maxLength = 32),
        @DocumentIndex(path = "role", maxLength = 16)
    }
)
public interface UserAccountRepository extends DocumentRepository<UUID, UserAccount> {

    @DocumentPath("email")
    Optional<UserAccount> findByEmail(String email);

    @DocumentPath("username")
    Optional<UserAccount> findByUsername(String username);

    @DocumentPath("role")
    Stream<UserAccount> streamByRole(String role);

    default UserAccount register(String email, String username) {
        // WARNING: This has a race condition! In production, use:
        // - External locking (e.g., Redisson distributed locks)
        // - Action queue/message broker for sequential processing
        if (findByEmail(email).isPresent()) {
            throw new IllegalStateException("Email already registered");
        }

        UserAccount account = new UserAccount();
        account.setEmail(email);
        account.setUsername(username);
        account.setRole("USER");
        account.setCreatedAt(Instant.now());

        UserProfile profile = new UserProfile();
        profile.setDisplayName(username);
        account.setProfile(profile);

        return save(account);
    }

    default void updateLastLogin(UUID userId) {
        findByPath(userId).ifPresent(account -> {
            account.setLastLogin(Instant.now());
            save(account);
        });
    }

    default List<UserAccount> getAdmins() {
        return streamByRole("ADMIN").collect(Collectors.toList());
    }
}

// Usage
UserAccountRepository accounts = persistence.createRepository(UserAccountRepository.class);

// Register new user
UserAccount alice = accounts.register("alice@example.com", "alice");

// Login
accounts.findByEmail("alice@example.com").ifPresent(account -> {
    accounts.updateLastLogin(account.getPath());
    System.out.println("Welcome back, " + account.getUsername());
});

// Find all admins (using indexed field)
List<UserAccount> admins = accounts.getAdmins();

// Search users by role with ordering
List<UserAccount> moderators = accounts.find(q -> q
  .where(on("role", eq("MODERATOR")))
  .orderBy(asc("username")))
  .collect(Collectors.toList());
```

## Backend Comparison

| Feature | MongoDB | PostgreSQL | Redis | Flat Files | In-Memory |
|---------|---------|------------|-------|------------|-----------|
| **Indexes** | Native | JSONB GIN | Hash+Set | File/Memory map | HashMap |
| **Query Support** | find()/delete() | find()/delete() | Planned | Planned | Planned |
| **Best For** | Document workloads | Already using Postgres | Because you can | Config files, small apps | Testing, temp state |

## Configurer Support

Serialization formats from [okaeri-configs](https://github.com/OkaeriPoland/okaeri-configs):

```java
// JSON (all backends)
new DocumentPersistence(backend, JsonSimpleConfigurer::new)

// YAML/HOCON/TOML (flat files only)
new DocumentPersistence(new FlatPersistence(new File("./data"), ".yml", YamlBukkitConfigurer::new), YamlBukkitConfigurer::new)
```

MongoDB, PostgreSQL, Redis, and In-Memory use JSON. Flat Files support any format.

## Related Projects

- [okaeri-configs](https://github.com/OkaeriPoland/okaeri-configs) - Configuration library powering the serialization
- [okaeri-platform](https://github.com/OkaeriPoland/okaeri-platform) - Full application framework using okaeri-persistence
