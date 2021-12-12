package eu.okaeri.persistencetestjdbc.basic.repository;

import eu.okaeri.persistence.PersistenceEntity;
import eu.okaeri.persistence.repository.DocumentRepository;
import eu.okaeri.persistence.repository.annotation.DocumentCollection;
import eu.okaeri.persistence.repository.annotation.DocumentIndex;
import eu.okaeri.persistence.repository.annotation.DocumentPath;
import eu.okaeri.persistencetestjdbc.basic.entity.User;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

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
