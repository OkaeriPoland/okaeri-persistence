package eu.okaeri.persistencetestjdbc.basic;

import com.zaxxer.hikari.HikariConfig;
import eu.okaeri.configs.json.simple.JsonSimpleConfigurer;
import eu.okaeri.persistence.PersistenceCollection;
import eu.okaeri.persistence.PersistenceEntity;
import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.document.DocumentPersistence;
import eu.okaeri.persistence.jdbc.H2Persistence;
import eu.okaeri.persistence.jdbc.commons.JdbcHelper;
import eu.okaeri.persistence.repository.RepositoryDeclaration;
import eu.okaeri.persistencetestjdbc.basic.entity.User;
import eu.okaeri.persistencetestjdbc.basic.entity.UserMeta;
import eu.okaeri.persistencetestjdbc.basic.repository.UserRepository;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.*;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class JdbcPersistenceTest {

    private static final long CREATE_USERS = 1_000;

    private DocumentPersistence persistence;
    private PersistenceCollection collection;
    private UserRepository repository;
    private User lastUser;

    @BeforeAll
    public void setup() {

        // setup hikari
        HikariConfig config = JdbcHelper.configureHikari("jdbc:h2:mem:test;mode=mysql", "org.h2.Driver");

        // create collection
        this.collection = PersistenceCollection.of(UserRepository.class);

        // prepare persistence backend
        this.persistence = new DocumentPersistence(new H2Persistence(PersistencePath.of("storage"), config), JsonSimpleConfigurer::new);
        this.persistence.registerCollection(this.collection);

        // create repository instance
        this.repository = RepositoryDeclaration.of(UserRepository.class).newProxy(this.persistence, this.collection, JdbcPersistenceTest.class.getClassLoader());
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
        assertThat(this.repository.count()).isEqualTo(CREATE_USERS);
    }

    @Test
    public void test_repository_delete_all() {
        assertThat(this.repository.deleteAll()).isTrue();
        assertThat(this.repository.count()).isEqualTo(0);
    }

    @Test
    public void test_repository_delete_all_by_path() {
        // test non-existent user
        assertThat(this.repository.deleteAllByPath(Collections.singletonList(UUID.randomUUID()))).isEqualTo(0);
        assertThat(this.repository.count()).isEqualTo(CREATE_USERS);
        // test real user
        assertThat(this.repository.deleteAllByPath(Collections.singletonList(this.lastUser.getId()))).isEqualTo(1);
        assertThat(this.repository.count()).isEqualTo(CREATE_USERS - 1);
    }

    @Test
    public void test_repository_delete_by_path() {
        // test non-existent user
        assertThat(this.repository.deleteByPath(UUID.randomUUID())).isFalse();
        assertThat(this.repository.count()).isEqualTo(CREATE_USERS);
        // test real user
        assertThat(this.repository.deleteByPath(this.lastUser.getId())).isTrue();
        assertThat(this.repository.count()).isEqualTo(CREATE_USERS - 1);
    }

    @Test
    public void test_repository_exists_by_path() {
        assertThat(this.repository.existsByPath(UUID.randomUUID())).isFalse();
        assertThat(this.repository.existsByPath(this.lastUser.getId())).isTrue();
    }

    @Test
    public void test_repository_stream_all() {
        assertThat(this.repository.streamAll().anyMatch(entity -> entity.equals(this.lastUser))).isTrue();
        assertThat(this.repository.streamAll().count()).isEqualTo(CREATE_USERS);
    }

    @Test
    public void test_repository_find_all() {
        assertThat(this.repository.findAll()).contains(this.lastUser);
        assertThat(this.repository.findAll()).hasSize((int) CREATE_USERS);
    }

    @Test
    public void test_repository_find_all_by_path() {
        // test non-existent user
        assertThat(this.repository.findAllByPath(Collections.singletonList(UUID.randomUUID()))).isEmpty();
        assertThat(this.repository.findAllByPath(Arrays.asList(UUID.randomUUID(), UUID.randomUUID()))).isEmpty();
        // test real user
        Collection<User> result1 = this.repository.findAllByPath(Collections.singletonList(this.lastUser.getId()));
        assertThat(result1).hasSize(1);
        assertThat(new ArrayList<>(result1).get(0).getPath()).isEqualTo(PersistencePath.of(this.lastUser.getId()));
        assertThat(new ArrayList<>(result1).get(0)).isEqualTo(this.lastUser);
        // test real user - requires no duplicates
        Collection<User> result2 = this.repository.findAllByPath(Arrays.asList(this.lastUser.getId(), this.lastUser.getId()));
        assertThat(result2).hasSize(1);
        assertThat(new ArrayList<>(result2).get(0).getPath()).isEqualTo(PersistencePath.of(this.lastUser.getId()));
        assertThat(new ArrayList<>(result2).get(0)).isEqualTo(this.lastUser);
    }

    @Test
    public void test_repository_find_by_path() {
        // test non-existent user
        assertThat(this.repository.findByPath(UUID.randomUUID())).isEmpty();
        // test real user
        assertThat(this.repository.findByPath(this.lastUser.getId())).contains(this.lastUser);
    }

    @Test
    public void test_repository_find_or_create() {
        // test non-existent user
        assertThat(this.repository.findOrCreateByPath(UUID.randomUUID())).isEqualTo(new User());
        // test real user
        assertThat(this.repository.findOrCreateByPath(this.lastUser.getId())).isEqualTo(this.lastUser);
    }

    @Test
    public void test_repository_custom_optional_entity_by_shortid() {
        // test non-existent user
        assertThat(this.repository.findEntityByShortId("XYZ")).isEmpty();
        // test real user
        assertThat(this.repository.findEntityByShortId(this.lastUser.getShortId()))
            .contains(new PersistenceEntity<>(this.lastUser.getPath(), this.lastUser));
    }

    @Test
    public void test_repository_custom_stream_entity_by_shortid() {
        // test non-existent user
        assertThat(this.repository.streamEntityByShortId("XYZ").count()).isEqualTo(0);
        // test real user
        List<PersistenceEntity<User>> entities = this.repository.streamEntityByShortId(this.lastUser.getShortId()).collect(Collectors.toList());
        assertThat(entities).hasSize(1);
        assertThat(entities.get(0)).isEqualTo(new PersistenceEntity<>(this.lastUser.getPath(), this.lastUser));
    }

    @Test
    public void test_repository_custom_list_entity_by_shortid() {
        // test non-existent user
        assertThat(this.repository.listEntityByShortId("XYZ")).isEmpty();
        // test real user
        List<PersistenceEntity<User>> entities = this.repository.listEntityByShortId(this.lastUser.getShortId());
        assertThat(entities).hasSize(1);
        assertThat(entities.get(0)).isEqualTo(new PersistenceEntity<>(this.lastUser.getPath(), this.lastUser));
    }

    @Test
    public void test_repository_custom_optional_by_shortid() {
        // test non-existent user
        assertThat(this.repository.findByShortId("XYZ")).isEmpty();
        // test real user
        assertThat(this.repository.findByShortId(this.lastUser.getShortId())).contains(this.lastUser);
    }

    @Test
    public void test_repository_custom_stream_by_shortid() {
        // test non-existent user
        assertThat(this.repository.streamByShortId("XYZ").count()).isEqualTo(0);
        // test real user
        assertThat(this.repository.streamByShortId(this.lastUser.getShortId()).findFirst()).contains(this.lastUser);
    }

    @Test
    public void test_repository_custom_list_by_shortid() {
        // test non-existent user
        assertThat(this.repository.listByShortId("XYZ")).isEmpty();
        // test real user
        assertThat(this.repository.listByShortId(this.lastUser.getShortId()))
            .containsExactly(this.lastUser);
    }

    @Test
    public void test_repository_custom_default() {
        assertThat(this.repository.getMetaDescriptionById(UUID.randomUUID())).isNull();
        assertThat(this.repository.getMetaDescriptionById(this.lastUser.getId()))
            .isEqualTo(this.lastUser.getMeta().getDescription());
    }
}
