package eu.okaeri.persistencetestjdbc.refs.repository;

import eu.okaeri.persistence.repository.DocumentRepository;
import eu.okaeri.persistence.repository.annotation.DocumentCollection;
import eu.okaeri.persistence.repository.annotation.DocumentIndex;
import eu.okaeri.persistencetestjdbc.refs.entity.Book;

import java.util.UUID;

@DocumentCollection(path = "book", keyLength = 36, indexes = {
        @DocumentIndex(path = "title", maxLength = 255)
})
public interface BookRepository extends DocumentRepository<UUID, Book> {
}
