package eu.okaeri.persistence.repository;

import eu.okaeri.persistence.PersistenceCollection;
import eu.okaeri.persistence.document.DocumentPersistence;

public interface RepositoryMethodCaller {
    Object call(DocumentPersistence persistence, PersistenceCollection collection, Object[] args);
}
