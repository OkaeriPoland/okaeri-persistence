package eu.okaeri.persistencetestjdbc.repository;

import eu.okaeri.persistence.repository.DocumentRepository;
import eu.okaeri.persistence.repository.annotation.Collection;
import eu.okaeri.persistence.repository.annotation.Index;
import eu.okaeri.persistence.repository.annotation.PropertyPath;
import eu.okaeri.persistencetestjdbc.entity.User;

import java.util.UUID;
import java.util.stream.Stream;

@Collection(path = "user", keyLength = 36, indexes = {
        @Index(path = "shortId", maxLength = 8),
        @Index(path = "meta.name", maxLength = 64)
})
public interface UserRepository extends DocumentRepository<UUID, User> {

    @PropertyPath("shortId")
    Stream<User> findByShortId(String shortId);

    @PropertyPath("meta.name")
    Stream<User> findByMetaName(String name);

    // custom method
    default String getMetaDescriptionById(UUID id) {
        return this.findByPath(id)
                .map(user -> user.getMeta().getDescription())
                .orElse(null);
    }
}
