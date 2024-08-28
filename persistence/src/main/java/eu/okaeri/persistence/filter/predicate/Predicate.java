package eu.okaeri.persistence.filter.predicate;

public interface Predicate {
    abstract boolean check(Object leftOperand);
}
