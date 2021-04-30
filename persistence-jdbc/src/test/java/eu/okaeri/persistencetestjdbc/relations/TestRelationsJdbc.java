package eu.okaeri.persistencetestjdbc.relations;


import com.zaxxer.hikari.HikariConfig;
import eu.okaeri.configs.json.simple.JsonSimpleConfigurer;
import eu.okaeri.configs.schema.GenericsDeclaration;
import eu.okaeri.configs.serdes.DeserializationData;
import eu.okaeri.configs.serdes.ObjectSerializer;
import eu.okaeri.configs.serdes.OkaeriSerdesPack;
import eu.okaeri.configs.serdes.SerializationData;
import eu.okaeri.persistence.PersistenceCollection;
import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.document.Document;
import eu.okaeri.persistence.document.DocumentPersistence;
import eu.okaeri.persistence.jdbc.H2Persistence;
import eu.okaeri.persistence.repository.RepositoryDeclaration;
import eu.okaeri.persistencetestjdbc.relations.entity.Author;
import eu.okaeri.persistencetestjdbc.relations.entity.Book;
import eu.okaeri.persistencetestjdbc.relations.repository.AuthorRepository;
import eu.okaeri.persistencetestjdbc.relations.repository.BookRepository;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.*;
import java.util.stream.Stream;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestRelationsJdbc {

    private static final long CREATE_USERS = 1_000;

    private DocumentPersistence persistence;
    private PersistenceCollection bookCollection;
    private PersistenceCollection authorCollection;
    private BookRepository bookRepository;
    private AuthorRepository authorRepository;

    @BeforeAll
    public void setup() {

        try {
            Class.forName("org.h2.Driver");
        } catch (ClassNotFoundException ignored) {
        }

        // setup hikari
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:h2:mem:test;mode=mysql");

        // create collections
        this.bookCollection = PersistenceCollection.of(BookRepository.class);
        this.authorCollection = PersistenceCollection.of(AuthorRepository.class);

        // cringe
        Map<String, Class<? extends Document>> nameToType = Collections.singletonMap("Author", Author.class);
        Map<Class<? extends Document>, String> typeToName = Collections.singletonMap(Author.class, "Author");

        // magic
        OkaeriSerdesPack serdesPack = registry -> registry.register(new ObjectSerializer<LazyRef<? extends Document>>() {
            @Override
            public boolean supports(Class clazz) {
                return LazyRef.class.isAssignableFrom(clazz);
            }

            @Override
            public void serialize(LazyRef lazyRef, SerializationData serializationData) {
                serializationData.add("_id", lazyRef.getId().getValue());
                serializationData.add("_type", typeToName.getOrDefault(lazyRef.getValueType(), lazyRef.getValueType().getName()));
                serializationData.add("_collection", lazyRef.getCollection().getValue());
            }

            @Override
            @SneakyThrows
            @SuppressWarnings("unchecked")
            public LazyRef<? extends Document> deserialize(DeserializationData deserializationData, GenericsDeclaration genericsDeclaration) {

                PersistencePath id = PersistencePath.of(deserializationData.get("_id", String.class));
                String typeString = deserializationData.get("_type", String.class);
                Class<? extends Document> type = nameToType.get(typeString);
                if (type == null) type = (Class<? extends Document>) Class.forName(typeString);
                PersistencePath collection = PersistencePath.of(deserializationData.get("_collection", String.class));

                return new LazyRef<>(id, collection, type, null, false, TestRelationsJdbc.this.persistence);
            }
        });

        // prepare persistence backend
        this.persistence = new DocumentPersistence(new H2Persistence(PersistencePath.of("storage"), config), JsonSimpleConfigurer::new, serdesPack);
        this.persistence.registerCollection(this.bookCollection);
        this.persistence.registerCollection(this.authorCollection);

        // create repository instance
        this.bookRepository = RepositoryDeclaration.of(BookRepository.class).newProxy(this.persistence, this.bookCollection, TestRelationsJdbc.class.getClassLoader());
        this.authorRepository = RepositoryDeclaration.of(AuthorRepository.class).newProxy(this.persistence, this.authorCollection, TestRelationsJdbc.class.getClassLoader());
    }

    private Author author1;
    private Author author2;
    private Author author3;
    private Author author4;

    private Book book1;
    private Book book2;
    private Book book3;

    @BeforeEach
    public void prepareDb() {

        // flush current
        this.persistence.deleteAll(this.bookCollection);

        // fill with data
        this.author1 = this.authorRepository.findOrCreateByPath(UUID.randomUUID());
        this.author1.setName("Some Author1");
        this.author2 = this.authorRepository.findOrCreateByPath(UUID.randomUUID());
        this.author2.setName("Some Author2");
        this.author3 = this.authorRepository.findOrCreateByPath(UUID.randomUUID());
        this.author3.setName("Some Author3");
        this.author4 = this.authorRepository.findOrCreateByPath(UUID.randomUUID());
        this.author4.setName("Some Author4");

        this.book1 = this.bookRepository.findOrCreateByPath(UUID.randomUUID());
        this.book1.setTitle("Some book: The title of single author book");
        this.book1.setAuthors(Collections.singletonList(LazyRef.of(this.author1)));

        this.book2 = this.bookRepository.findOrCreateByPath(UUID.randomUUID());
        this.book2.setTitle("Some book: The title of two author book");
        this.book2.setAuthors(Arrays.asList(LazyRef.of(this.author1), LazyRef.of(this.author2)));

        this.book3 = this.bookRepository.findOrCreateByPath(UUID.randomUUID());
        this.book3.setTitle("Some book: The title of three author book");
        this.book3.setAuthors(Arrays.asList(LazyRef.of(this.author2), LazyRef.of(this.author3), LazyRef.of(this.author4)));

        Stream.of(this.author1, this.author2, this.author3, this.author4, this.book1, this.book1, this.book3).forEach(Document::save);
    }

    @Test
    public void test_write() {

        this.book3.save();
        System.out.println(this.book3);
        System.out.println(this.book3.saveToString());
        System.out.println();

        Book book = this.bookRepository.findByPath(this.book3.getPath().toUUID()).get();
        System.out.println(book);

        for (LazyRef<Author> author : book.getAuthors()) {
            Author authorr = author.get();
            System.out.println(authorr);
        }

//        System.out.println(book.saveToString());
    }
}
