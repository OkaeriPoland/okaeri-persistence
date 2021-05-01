# Okaeri Persistence

![License](https://img.shields.io/github/license/OkaeriPoland/okaeri-persistence)
![Total lines](https://img.shields.io/tokei/lines/github/OkaeriPoland/okaeri-persistence)
![Repo size](https://img.shields.io/github/repo-size/OkaeriPoland/okaeri-persistence)
![Contributors](https://img.shields.io/github/contributors/OkaeriPoland/okaeri-persistence)
[![Discord](https://img.shields.io/discord/589089838200913930)](https://discord.gg/hASN5eX)

Object Document Mapping (ODM) library allowing to focus on data instead of the storage layer.
Originally developed for and part of the [okaeri-platform](https://github.com/OkaeriPoland/okaeri-platform).

## Backends

### Flat & Databases

| Name | Type | Indexes | Comment |
|-|-|-|-|
| FlatPersistence | `flat` | Yes (in-memory or file based) | Allows managing collections of the configuration files with the possibility to index certain properties for quick search, any okaeri-configs provider can be used. With the default saveIndex=false index is automatically created every startup. One may choose to save index to disk. However, we highly advise against using persistent index, especially in write intensive applications. |
| MariaDbPersistence | `jdbc` | Yes (additional table) | Uses [HikariCP](https://github.com/brettwooldridge/HikariCP). Created with MySQL/MariaDB in mind using native JSON datatype, makes use of the json_extract for filtering by properties even when property is not marked as indexed. |
| H2Persistence | `jdbc` | Yes (additional table) | Uses [HikariCP](https://github.com/brettwooldridge/HikariCP). Created for H2 databases in `mode=mysql`. Stores JSON in the text field, makes use of the instr for prefiltering when possible. |
| JdbcPersistence | `jdbc` | Yes (additional table) | Uses [HikariCP](https://github.com/brettwooldridge/HikariCP). Created for generic JDBC support. Stores JSON in the text field, makes no use of any prefiltering whatsoever. Data writes take two queries. |
| RedisPersistence | `redis` | Yes (additional hashes and sets) | Uses [Lettuce](https://lettuce.io/). Created for storing JSON documents with something the redis itself is missing - ability to access entity by property without the need to manually manage additional keys. Makes use of lua scripts for blazing-fast startup index validation and filtering by indexed properties. Currently the fastest implementation avaibile. |

### Special usage

| Name | Type | Indexes | Comment |
|-|-|-|-|
| InMemoryDocumentPersistence | `core` | Yes (in-memory) | Included in the core library implementation allowing to manage volatile collections of configurations. Great to store user state, e.g. on gameservers, can store even unserializable entities (it is required to mark them as @Excluded, because indexing still needs to deconstruct documents). Allows to use the power of indexing without the need for database. |

## Genesis

The library is composed based on the [okaeri-configs](https://github.com/OkaeriPoland/okaeri-configs) and is intended 
to be used as an extension to store configurations or other documents without thinking about a specific backend. 
Core library provides relatively small footprint with size below 100kB (even with file persistence) and allows to use more
sophisticated database drivers when needed.

```java
new DocumentPersistence(new JdbcPersistence(basePath, hikari), JsonSimpleConfigurer::new)
```

## Documents

Being based on the documents allows supporting practically any platform possible. Store as a file? No problem, YAML, HJSON, anything.
What about the databases? Dedicated for the document stores, NoSQL or abusing relational database to store JSON? No problem!

Documents come at the additional benefit of not having to worry about complex relations based on the multiple tables which can
limit the developer possibilities for storing the data. Storing objects in the relational database isn't fun when it comes
to nested maps and lists and may require the change of the project core concepts or means accepting the limitations and potential
performance impact of the complex joins and queries.

There is a great place for optimizations of the object structure and relational databases, but sometimes it is just not the right fit.
We value flexibility and fast development. Carefully designing fine-tuned 6 table schemas to store just a few tens of thousands objects
is the opposite of that.

You are developing a simple concept TODO app, you can take your time and manually create your tables and then write really complex
queries just to get poor performance. You can use complex ORM framework like Hibernate and skip most of the queries part. But then
you realize that you probably just fetch the data for the single user all the time, so why bother? Documents just fit in here.

Anything that closely bounds to some identifier and is used almost exclusively for that scope is the perfect example where spending
time thinking about your database backend may be just not worth it. You just need to define your expectations and make a decision
what do you value more.

```java
// example document vs tables used in the relational databases
public class UserProperties extends Document {
    private UUID uuid;
    private List<Instant> lastLogins; // table: user_logins
    private String name;
    private List<String> aliases; // table: user_aliases
    private Map<String, List<String>> todoLists; // table: user_todo, user_todo_task
    private List<UUID> friends; // table: user_friends
}
```

## Indexes

Implementations may provide indexing support. The idea is the developer does not need to care about the specifics.
The backends just work, better or worse. That does not mean your apps would be poorly performing. It just means
that if you want to leave the choice to the user you can do that. There is nothing wrong with file based storage
for small game server or local app, but a real database may be required for the more demanding environments.

Fetching by indexed property is expected to be almost as quick as using ID, but when the implementation does not 
provide it, fallback methods are used for slower but still working filtering. Thanks to that you can get the
best performance possible on the specific backend and it just works.

Indexing comes at the cost of increased memory or/and storage usage and write penalty, varying depending on the backend. 
It is highly recommended, same as with every database, to chose your indexes wisely. You are trading some of that write 
speed and resources for the greatly reduced read times.

Manual changes done to the databases, depending on the backend, may cause emulated indexes to be inaccurate. We guarantee
however, to never feed you wrong data (e.g. when you are searching by prop=123 you should always get only matching documents).
It is possible to miss some data in such search, where the database was manually altered without updating indexes.

```java
PersistenceCollection.of("player", 36)
    .index(IndexProperty.of("name", 24))
    .index(IndexProperty.of("lastJoinedLocation").sub("world").maxLength(64))
```

## Streams

Streaming API opens multiple possibilities, e.g. filters can be automatically optimized. Implementations may fetch
data in partitions and then parsing is done only when document is about to get into the stream. Everything is 
done automatically and can decrease fetch times dramatically. Smart tricks like prefiltering can be applied to prevent
parsing documents determined to not include searched property.

Example pipeline of the stream:
- Redis cursor, Files.list or other generator
- Optional string prefilter for readByProperty calls
- Format to Document mapper (basically parsing JSON/YAML)
- Optional document filter for readByProperty
- Optional mapping to the custom object
- Your filters and processing

## Repositories

Reducing boilerplate is one of the primary goals for the project. We provide DocumentRepository<PATH, T> interface which allows to access basic methods similar to 
Spring Boot's CrudRepository and allows for simple filters to be automatically implemented. Example repository setup and usage can be found in 
the [TestPersistenceJdbc](https://github.com/OkaeriPoland/okaeri-persistence/blob/master/persistence-jdbc/src/test/java/eu/okaeri/persistencetestjdbc/basic/TestPersistenceJdbc.java).

### Default methods
```java
public interface DocumentRepository<PATH, T extends Document> {
    DocumentPersistence getPersistence();
    PersistenceCollection getCollection();
    Class<? extends Document> getDocumentType();
    long count();
    boolean deleteAll();
    long deleteAllByPath(Iterable<? extends PATH> paths);
    boolean deleteByPath(PATH path);
    boolean existsByPath(PATH path);
    Stream<T> findAll();
    Collection<T> findAllByPath(Iterable<? extends PATH> paths);
    Collection<T> findOrCreateAllByPath(Iterable<? extends PATH> paths);
    Optional<T> findByPath(PATH path);
    T findOrCreateByPath(PATH path);
    T save(T document);
    Iterable<T> saveAll(Iterable<T> documents);
}
```

### Example repository
```java
@DocumentCollection(path = "user", keyLength = 36, indexes = {
        @DocumentIndex(path = "shortId", maxLength = 8),
        @DocumentIndex(path = "meta.name", maxLength = 64)
})
public interface UserRepository extends DocumentRepository<UUID, User> {

    @DocumentPath("shortId")
    Stream<User> streamByShortId(String shortId);

    @DocumentPath("shortId")
    Optional<User> findByShortId(String shortId);

    @DocumentPath("shortId")
    List<User> listByShortId(String shortId);

    @DocumentPath("shortId")
    Stream<PersistenceEntity<User>> streamEntityByShortId(String shortId);

    @DocumentPath("shortId")
    Optional<PersistenceEntity<User>> findEntityByShortId(String shortId);

    @DocumentPath("shortId")
    List<PersistenceEntity<User>> listEntityByShortId(String shortId);

    @DocumentPath("meta.name")
    Stream<User> streamByMetaName(String name);

    // custom method
    default String getMetaDescriptionById(UUID id) {
        return this.findByPath(id)
                .map(user -> user.getMeta().getDescription())
                .orElse(null);
    }
}
```

## Examples
See [PlayerRepository](https://github.com/OkaeriPoland/okaeri-platform/blob/master/bukkit-example/src/main/java/org/example/okaeriplatformtest/persistence/PlayerRepository.java) in the okaeri-platform example.
For the methods available in Persistence classes refer to the [source code comments](https://github.com/OkaeriPoland/okaeri-persistence/blob/master/persistence/src/main/java/eu/okaeri/persistence/Persistence.java).
Thank you for the interest in the project. We wish you an enjoyable stay or a pleasant onward journey.

## Installation
### Maven
Add repository to the `repositories` section:
```xml
<repository>
    <id>okaeri-repo</id>
    <url>https://storehouse.okaeri.eu/repository/maven-public/</url>
</repository>
```
Add dependency to the `dependencies` section:
```xml
<dependency>
  <groupId>eu.okaeri</groupId>
  <artifactId>okaeri-persistence-[type]</artifactId>
  <version>1.3.1</version>
</dependency>
```
### Gradle
Add repository to the `repositories` section:
```groovy
maven { url "https://storehouse.okaeri.eu/repository/maven-public/" }
```
Add dependency to the `maven` section:
```groovy
implementation 'eu.okaeri:okaeri-persistence-[type]:1.3.1'
```
