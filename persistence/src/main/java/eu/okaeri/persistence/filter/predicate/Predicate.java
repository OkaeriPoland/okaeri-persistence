package eu.okaeri.persistence.filter.predicate;

public interface Predicate<T> {
    abstract boolean check(Object leftOperand);
}
