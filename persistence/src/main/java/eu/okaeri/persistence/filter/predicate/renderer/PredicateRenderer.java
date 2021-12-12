package eu.okaeri.persistence.filter.predicate.renderer;

import eu.okaeri.persistence.filter.predicate.Predicate;

public interface PredicateRenderer {

    String render(Predicate<?> predicate);

    String render(Object leftOperand, Predicate<?> predicate);

    String renderOperand(Object operand);
}
