package eu.okaeri.persistencetestjdbc;

import com.zaxxer.hikari.HikariConfig;
import eu.okaeri.configs.json.simple.JsonSimpleConfigurer;
import eu.okaeri.persistence.PersistenceCollection;
import eu.okaeri.persistence.PersistenceEntity;
import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.document.DocumentPersistence;
import eu.okaeri.persistence.index.IndexProperty;
import eu.okaeri.persistence.jdbc.H2Persistence;
import eu.okaeri.persistence.repository.RepositoryDeclaration;
import eu.okaeri.persistencetestjdbc.entity.User;
import eu.okaeri.persistencetestjdbc.entity.UserMeta;
import eu.okaeri.persistencetestjdbc.repository.UserRepository;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestPersistenceJdbc {

    private static final long CREATE_USERS = 1_000;

    private DocumentPersistence persistence;
    private PersistenceCollection collection;
    private UserRepository repository;
    private User lastUser;

    @BeforeAll
    public void setup() {

        try {
            Class.forName("org.h2.Driver");
        } catch (ClassNotFoundException ignored) {
        }

        // setup hikari
        PersistencePath path = PersistencePath.of("user");
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:h2:mem:test;mode=mysql");

        // create collection FIXME: read from @Collection using RepositoryDeclaration
        this.collection = PersistenceCollection.of("user", 36)
                .index(IndexProperty.of("shortId", 8))
                .index(IndexProperty.of("meta").sub("name").maxLength(64));

        // prepare persistence backend
        this.persistence = new DocumentPersistence(new H2Persistence(path, config), JsonSimpleConfigurer::new);
        this.persistence.registerCollection(this.collection);

        // create repository instance
        this.repository = RepositoryDeclaration.of(UserRepository.class)
                .newProxy(this.persistence, this.collection, TestPersistenceJdbc.class.getClassLoader());
    }

    @BeforeEach
    public void prepareDb() {
        // flush current
        this.persistence.deleteAll(this.collection);
        // fill with users
        for (int i = 0; i < CREATE_USERS; i++) {

            UUID uuid = UUID.randomUUID();
            User user = this.persistence.readOrEmpty(this.collection, PersistencePath.of(uuid)).into(User.class);

            UserMeta meta = new UserMeta();
            meta.setDescription("Some user" + i);
            meta.setName("John" + i);
            meta.setFullName(meta.getName() + " o" + i);

            user.setId(uuid);
            user.setShortId("abc" + i);
            user.setMeta(meta);
            user.save();

            this.lastUser = user;
        }
    }

    @Test
    public void test_repository_count() {
        assertEquals(CREATE_USERS, this.repository.count());
    }

    @Test
    public void test_repository_delete_all() {
        assertTrue(this.repository.deleteAll());
        assertEquals(0, this.repository.count());
    }

    @Test
    public void test_repository_delete_all_by_path() {
        // test non-existent user
        assertEquals(0, this.repository.deleteAllByPath(Collections.singletonList(UUID.randomUUID())));
        assertEquals(CREATE_USERS, this.repository.count());
        // test real user
        assertEquals(1, this.repository.deleteAllByPath(Collections.singletonList(this.lastUser.getId())));
        assertEquals(CREATE_USERS - 1, this.repository.count());
    }

    @Test
    public void test_repository_delete_by_path() {
        // test non-existent user
        assertEquals(false, this.repository.deleteByPath(UUID.randomUUID()));
        assertEquals(CREATE_USERS, this.repository.count());
        // test real user
        assertEquals(true, this.repository.deleteByPath(this.lastUser.getId()));
        assertEquals(CREATE_USERS - 1, this.repository.count());
    }

    @Test
    public void test_repository_exists_by_path() {
        assertFalse(this.repository.existsByPath(UUID.randomUUID()));
        assertTrue(this.repository.existsByPath(this.lastUser.getId()));
    }

    @Test
    public void test_repository_find_all() {
        assertTrue(this.repository.findAll().anyMatch(entity -> entity.getValue().equals(this.lastUser)));
        assertEquals(CREATE_USERS, this.repository.findAll().count());
    }

    @Test
    public void test_repository_find_all_by_path() {
        // test non-existent user
        assertEquals(0, this.repository.findAllByPath(Collections.singletonList(UUID.randomUUID())).size());
        assertEquals(0, this.repository.findAllByPath(Arrays.asList(UUID.randomUUID(), UUID.randomUUID())).size());
        // test real user
        Collection<PersistenceEntity<User>> result1 = this.repository.findAllByPath(Collections.singletonList(this.lastUser.getId()));
        assertEquals(1, result1.size());
        assertEquals(PersistencePath.of(this.lastUser.getId()), new ArrayList<>(result1).get(0).getPath());
        assertEquals(this.lastUser, new ArrayList<>(result1).get(0).getValue());
        // test real user - requires no duplicates
        Collection<PersistenceEntity<User>> result2 = this.repository.findAllByPath(Arrays.asList(this.lastUser.getId(), this.lastUser.getId()));
        assertEquals(1, result2.size());
        assertEquals(PersistencePath.of(this.lastUser.getId()), new ArrayList<>(result2).get(0).getPath());
        assertEquals(this.lastUser, new ArrayList<>(result2).get(0).getValue());
    }

    @Test
    public void test_repository_find_by_path() {
        // test non-existent user
        assertNull(this.repository.findByPath(UUID.randomUUID()).orElse(null));
        // test real user
        assertEquals(this.lastUser, this.repository.findByPath(this.lastUser.getId()).orElse(null));
    }
}
