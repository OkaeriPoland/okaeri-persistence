package eu.okaeri.persistence.filter.condition.renderer;

import eu.okaeri.persistence.filter.condition.Condition;
import eu.okaeri.persistence.filter.predicate.renderer.PredicateRenderer;
import eu.okaeri.persistence.filter.predicate.renderer.PredicateRendererLiteral;
import eu.okaeri.persistence.filter.renderer.VariableRenderer;
import lombok.Data;
import lombok.NonNull;

import java.util.Arrays;
import java.util.stream.Collectors;

@Data
public class DefaultConditionRenderer implements ConditionRenderer {

    private final VariableRenderer variableRenderer;
    private final PredicateRenderer predicateRenderer;

    @Override
    public String render(@NonNull Condition condition) {

        String variable = this.variableRenderer.render(condition.getPath());
        PredicateRendererLiteral variableLiteral = new PredicateRendererLiteral(variable);

        String expression = Arrays.stream(condition.getPredicates())
                .map(predicate -> this.predicateRenderer.render(variableLiteral, predicate))
                .collect(Collectors.joining(" && ")); // FIXME: or/and/nesting

        return (condition.getPredicates().length == 1)
                ? expression
                : ("(" + expression + ")");
    }
}
