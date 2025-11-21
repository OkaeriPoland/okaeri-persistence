package eu.okaeri.persistence.filter.predicate.string;

import eu.okaeri.persistence.filter.predicate.SimplePredicate;
import lombok.Getter;
import lombok.NonNull;

/**
 * String ends with predicate.
 * {@code field endsWith "suffix"}
 */
public class EndsWithPredicate extends SimplePredicate {

    @Getter
    private final boolean ignoreCase;

    public EndsWithPredicate(@NonNull String suffix) {
        this(suffix, false);
    }

    public EndsWithPredicate(@NonNull String suffix, boolean ignoreCase) {
        super(suffix);
        this.ignoreCase = ignoreCase;
    }

    /**
     * Returns a case-insensitive version of this predicate.
     */
    public SimplePredicate ignoreCase() {
        return new EndsWithPredicate((String) this.getRightOperand(), true);
    }

    @Override
    public boolean check(@NonNull Object leftOperand) {
        String left = String.valueOf(leftOperand);
        String right = (String) this.getRightOperand();
        if (this.ignoreCase) {
            return left.toLowerCase().endsWith(right.toLowerCase());
        }
        return left.endsWith(right);
    }
}
