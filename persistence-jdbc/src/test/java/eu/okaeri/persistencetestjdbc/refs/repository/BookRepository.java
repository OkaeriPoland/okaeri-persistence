package eu.okaeri.persistencetestjdbc.refs.repository;

import eu.okaeri.persistence.repository.DocumentRepository;
import eu.okaeri.persistence.repository.annotation.Collection;
import eu.okaeri.persistence.repository.annotation.Index;
import eu.okaeri.persistencetestjdbc.refs.entity.Book;

import java.util.UUID;

@Collection(path = "book", keyLength = 36, indexes = {
        @Index(path = "title", maxLength = 255)
})
public interface BookRepository extends DocumentRepository<UUID, Book> {
}
