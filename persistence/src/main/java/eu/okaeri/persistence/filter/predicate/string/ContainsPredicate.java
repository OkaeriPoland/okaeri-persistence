package eu.okaeri.persistence.filter.predicate.string;

import eu.okaeri.persistence.filter.predicate.SimplePredicate;
import lombok.Getter;
import lombok.NonNull;

/**
 * String contains predicate.
 * {@code field contains "substring"}
 */
public class ContainsPredicate extends SimplePredicate {

    @Getter
    private final boolean ignoreCase;

    public ContainsPredicate(@NonNull String substring) {
        this(substring, false);
    }

    public ContainsPredicate(@NonNull String substring, boolean ignoreCase) {
        super(substring);
        this.ignoreCase = ignoreCase;
    }

    /**
     * Returns a case-insensitive version of this predicate.
     */
    public ContainsPredicate ignoreCase() {
        return new ContainsPredicate((String) this.getRightOperand(), true);
    }

    @Override
    public boolean check(@NonNull Object leftOperand) {
        String left = String.valueOf(leftOperand);
        String right = (String) this.getRightOperand();

        if (this.ignoreCase) {
            return left.toLowerCase().contains(right.toLowerCase());
        }
        return left.contains(right);
    }
}
