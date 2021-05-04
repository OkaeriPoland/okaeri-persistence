package eu.okaeri.persistencetestmem.basic;

import eu.okaeri.persistence.PersistenceCollection;
import eu.okaeri.persistence.PersistenceEntity;
import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.document.DocumentPersistence;
import eu.okaeri.persistence.document.InMemoryDocumentPersistence;
import eu.okaeri.persistence.repository.RepositoryDeclaration;
import eu.okaeri.persistencetestmem.basic.entity.User;
import eu.okaeri.persistencetestmem.basic.repository.UserRepository;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import eu.okaeri.persistencetestmem.basic.entity.UnknownEntity;
import eu.okaeri.persistencetestmem.basic.entity.UserMeta;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestPersistenceInMemory {

    private static final long CREATE_USERS = 1_000;

    private DocumentPersistence persistence;
    private PersistenceCollection collection;
    private UserRepository repository;
    private User lastUser;

    @BeforeAll
    public void setup() {
        // create collection
        this.collection = PersistenceCollection.of(UserRepository.class);
        // prepare persistence backend
        this.persistence = new InMemoryDocumentPersistence();
        this.persistence.registerCollection(this.collection);
        // create repository instance
        this.repository = RepositoryDeclaration.of(UserRepository.class).newProxy(this.persistence, this.collection, TestPersistenceInMemory.class.getClassLoader());
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
            user.setEntity(new UnknownEntity(ThreadLocalRandom.current().nextInt(11, 20)));
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
        assertTrue(this.repository.findAll().anyMatch(entity -> entity.equals(this.lastUser)));
        assertEquals(CREATE_USERS, this.repository.findAll().count());
    }

    @Test
    public void test_repository_find_all_by_path() {
        // test non-existent user
        assertEquals(0, this.repository.findAllByPath(Collections.singletonList(UUID.randomUUID())).size());
        assertEquals(0, this.repository.findAllByPath(Arrays.asList(UUID.randomUUID(), UUID.randomUUID())).size());
        // test real user
        Collection<User> result1 = this.repository.findAllByPath(Collections.singletonList(this.lastUser.getId()));
        assertEquals(1, result1.size());
        assertEquals(PersistencePath.of(this.lastUser.getId()), new ArrayList<>(result1).get(0).getPath());
        assertEquals(this.lastUser, new ArrayList<>(result1).get(0));
        // test real user - requires no duplicates
        Collection<User> result2 = this.repository.findAllByPath(Arrays.asList(this.lastUser.getId(), this.lastUser.getId()));
        assertEquals(1, result2.size());
        assertEquals(PersistencePath.of(this.lastUser.getId()), new ArrayList<>(result2).get(0).getPath());
        assertEquals(this.lastUser, new ArrayList<>(result2).get(0));
    }

    @Test
    public void test_repository_find_by_path() {
        // test non-existent user
        assertNull(this.repository.findByPath(UUID.randomUUID()).orElse(null));
        // test real user
        assertEquals(this.lastUser, this.repository.findByPath(this.lastUser.getId()).orElse(null));
    }

    @Test
    public void test_repository_find_or_create() {
        // test non-existent user
        assertEquals(new User(), this.repository.findOrCreateByPath(UUID.randomUUID()));
        // test real user
        assertEquals(this.lastUser, this.repository.findOrCreateByPath(this.lastUser.getId()));
    }

    @Test
    public void test_repository_custom_optional_entity_by_shortid() {
        // test non-existent user
        assertFalse(this.repository.findEntityByShortId("XYZ").isPresent());
        // test real user
        assertEquals(new PersistenceEntity<>(this.lastUser.getPath(), this.lastUser), this.repository.findEntityByShortId(this.lastUser.getShortId()).orElse(null));
    }

    @Test
    public void test_repository_custom_stream_entity_by_shortid() {
        // test non-existent user
        assertEquals(0, this.repository.streamEntityByShortId("XYZ").count());
        // test real user
        List<PersistenceEntity<User>> entities = this.repository.streamEntityByShortId(this.lastUser.getShortId()).collect(Collectors.toList());
        assertEquals(1, entities.size());
        assertEquals(new PersistenceEntity<>(this.lastUser.getPath(), this.lastUser), entities.get(0));
    }

    @Test
    public void test_repository_custom_list_entity_by_shortid() {
        // test non-existent user
        assertEquals(0, this.repository.listEntityByShortId("XYZ").size());
        // test real user
        List<PersistenceEntity<User>> entities = this.repository.listEntityByShortId(this.lastUser.getShortId());
        assertEquals(1, entities.size());
        assertEquals(new PersistenceEntity<>(this.lastUser.getPath(), this.lastUser), entities.get(0));
    }

    @Test
    public void test_repository_custom_optional_by_shortid() {
        // test non-existent user
        assertFalse(this.repository.findByShortId("XYZ").isPresent());
        // test real user
        assertEquals(this.lastUser, this.repository.findByShortId(this.lastUser.getShortId()).orElse(null));
    }

    @Test
    public void test_repository_custom_stream_by_shortid() {
        // test non-existent user
        assertEquals(0, this.repository.streamByShortId("XYZ").count());
        // test real user
        assertEquals(this.lastUser, this.repository.streamByShortId(this.lastUser.getShortId()).findFirst().orElse(null));
    }

    @Test
    public void test_repository_custom_list_by_shortid() {
        // test non-existent user
        assertEquals(0, this.repository.listByShortId("XYZ").size());
        // test real user
        assertIterableEquals(Collections.singletonList(this.lastUser), this.repository.listByShortId(this.lastUser.getShortId()));
    }

    @Test
    public void test_repository_custom_default() {
        assertNull(this.repository.getMetaDescriptionById(UUID.randomUUID()));
        assertEquals(this.lastUser.getMeta().getDescription(), this.repository.getMetaDescriptionById(this.lastUser.getId()));
    }
}
