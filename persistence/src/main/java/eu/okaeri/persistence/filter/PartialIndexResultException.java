package eu.okaeri.persistence.filter;

import eu.okaeri.persistence.PersistenceEntity;
import lombok.Getter;

import java.util.stream.Stream;

/**
 * Exception thrown when index provides partial coverage for a query.
 * Contains the narrowed-down candidates that still need remaining filter applied.
 * <p>
 * Used by FlatPersistence to signal to DocumentPersistence that:
 * - Index was used to narrow down candidates (significant speedup)
 * - Remaining condition needs to be evaluated in-memory
 */
@Getter
public class PartialIndexResultException extends UnsupportedOperationException {

    private final Stream<PersistenceEntity<String>> partialResults;

    public PartialIndexResultException(Stream<PersistenceEntity<String>> partialResults) {
        super("Partial index coverage - results need additional filtering");
        this.partialResults = partialResults;
    }
}
