package eu.okaeri.persistencetest.fixtures;

import eu.okaeri.persistence.document.Document;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.util.UUID;

/**
 * Entity designed for index performance testing.
 * Has multiple fields of different types to test various query patterns.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class IndexTestEntity extends Document {

    /**
     * String field - will be indexed in indexed repository.
     * Values: "category_0" through "category_19" (20 unique values).
     */
    private String category;

    /**
     * Integer field - will be indexed in indexed repository.
     * Values: 0 through 19 (20 unique values).
     */
    private int level;

    /**
     * Boolean field - will be indexed in indexed repository.
     * Values: true for even levels, false for odd.
     */
    private boolean active;

    /**
     * Non-indexed string field for mixed query tests.
     */
    private String description;

    /**
     * Non-indexed integer field for mixed query tests.
     */
    private int score;

    /**
     * Sequence number within category (0 to FACTOR-1).
     */
    private int sequence;

    public UUID getId() {
        return this.getPath().toUUID();
    }
}
