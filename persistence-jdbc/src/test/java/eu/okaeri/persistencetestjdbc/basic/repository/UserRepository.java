package eu.okaeri.persistencetestjdbc.basic.repository;

import eu.okaeri.persistence.PersistenceEntity;
import eu.okaeri.persistence.repository.DocumentRepository;
import eu.okaeri.persistence.repository.annotation.Collection;
import eu.okaeri.persistence.repository.annotation.Index;
import eu.okaeri.persistence.repository.annotation.PropertyPath;
import eu.okaeri.persistencetestjdbc.basic.entity.User;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

@Collection(path = "user", keyLength = 36, indexes = {
        @Index(path = "shortId", maxLength = 8),
        @Index(path = "meta.name", maxLength = 64)
})
public interface UserRepository extends DocumentRepository<UUID, User> {

    @PropertyPath("shortId")
    Stream<User> streamByShortId(String shortId);

    @PropertyPath("shortId")
    Optional<User> findByShortId(String shortId);

    @PropertyPath("shortId")
    List<User> listByShortId(String shortId);

    @PropertyPath("shortId")
    Stream<PersistenceEntity<User>> streamEntityByShortId(String shortId);

    @PropertyPath("shortId")
    Optional<PersistenceEntity<User>> findEntityByShortId(String shortId);

    @PropertyPath("shortId")
    List<PersistenceEntity<User>> listEntityByShortId(String shortId);

    @PropertyPath("meta.name")
    Stream<User> streamByMetaName(String name);

    // custom method
    default String getMetaDescriptionById(UUID id) {
        return this.findByPath(id)
                .map(user -> user.getMeta().getDescription())
                .orElse(null);
    }
}
