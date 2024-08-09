package eu.okaeri.persistencetestjdbc.refs.repository;

import eu.okaeri.persistence.repository.DocumentRepository;
import eu.okaeri.persistence.repository.annotation.DocumentCollection;
import eu.okaeri.persistence.repository.annotation.DocumentIndex;
import eu.okaeri.persistencetestjdbc.refs.entity.Author;
import java.util.UUID;

@DocumentCollection(path = "author", keyLength = 36, indexes = @DocumentIndex(path = "name", maxLength = 128))
public interface AuthorRepository extends DocumentRepository<UUID, Author> {}
