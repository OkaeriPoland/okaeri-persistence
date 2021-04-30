package eu.okaeri.persistencetestjdbc.refs.repository;

import eu.okaeri.persistence.repository.DocumentRepository;
import eu.okaeri.persistence.repository.annotation.Collection;
import eu.okaeri.persistence.repository.annotation.Index;
import eu.okaeri.persistencetestjdbc.refs.entity.Author;

import java.util.UUID;

@Collection(path = "author", keyLength = 36, indexes = {
        @Index(path = "name", maxLength = 128)
})
public interface AuthorRepository extends DocumentRepository<UUID, Author> {
}
