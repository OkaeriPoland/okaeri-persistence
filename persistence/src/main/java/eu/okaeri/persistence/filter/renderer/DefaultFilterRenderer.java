package eu.okaeri.persistence.filter.renderer;

import eu.okaeri.persistence.filter.condition.Condition;
import eu.okaeri.persistence.filter.condition.LogicalOperator;
import eu.okaeri.persistence.filter.predicate.*;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.stream.Collectors;

@RequiredArgsConstructor
public class DefaultFilterRenderer implements FilterRenderer {

    private static final FilterRendererLiteral LITERAL_X = new FilterRendererLiteral("x");

    protected final VariableRenderer variableRenderer;

    @Override
    public String renderOperator(@NonNull LogicalOperator operator) {
        if (operator == LogicalOperator.AND) {
            return " && ";
        }
        if (operator == LogicalOperator.OR) {
            return " || ";
        }
        throw new IllegalArgumentException("Unsupported operator: " + operator);
    }

    @Override
    public String renderOperator(@NonNull Predicate<?> predicate) {

        if (predicate instanceof EqPredicate) {
            return "==";
        } else if (predicate instanceof GePredicate) {
            return ">=";
        } else if (predicate instanceof GtPredicate) {
            return ">";
        } else if (predicate instanceof LePredicate) {
            return "<=";
        } else if (predicate instanceof LtPredicate) {
            return "<";
        } else if (predicate instanceof NePredicate) {
            return "!=";
        }

        throw new IllegalArgumentException("cannot render operator " + predicate + " [" + predicate.getClass() + "]");
    }

    @Override
    public String renderCondition(@NonNull Condition condition) {

        String expression = Arrays.stream(condition.getPredicates())
            .map(predicate -> {
                if (predicate instanceof Condition) {
                    return this.renderPredicate(predicate);
                } else {
                    String variable = this.variableRenderer.render(condition.getPath());
                    FilterRendererLiteral variableLiteral = new FilterRendererLiteral(variable);
                    return this.renderPredicate(variableLiteral, predicate);
                }
            })
            .collect(Collectors.joining(this.renderOperator(condition.getOperator())));

        return (condition.getPredicates().length == 1)
            ? expression
            : ("(" + expression + ")");
    }

    @Override
    public String renderPredicate(@NonNull Object leftOperand, @NonNull Predicate<?> predicate) {
        return "(" + this.renderOperand(leftOperand) + " " + this.renderOperator(predicate) + " " + this.renderOperand(predicate) + ")";
    }

    @Override
    public String renderPredicate(@NonNull Predicate<?> predicate) {

        if (predicate instanceof Condition) {
            return this.renderCondition((Condition) predicate);
        }

        return this.renderPredicate(LITERAL_X, predicate);
    }

    @Override
    public String renderOperand(@NonNull Object operand) {

        if (operand instanceof FilterRendererLiteral) {
            return ((FilterRendererLiteral) operand).getValue();
        }

        if (operand instanceof SimplePredicate) {
            operand = ((SimplePredicate<?>) operand).getRightOperand();
        }

        if (operand instanceof Condition) {
            return this.renderCondition((Condition) operand);
        }

        if (operand instanceof Double) {
            double dOp = (Double) operand;
            if (dOp == (int) dOp) {
                return String.valueOf((int) dOp);
            }
        }

        if (operand instanceof Float) {
            float dOp = (Float) operand;
            if (dOp == (int) dOp) {
                return String.valueOf((int) dOp);
            }
        }

        if (operand instanceof BigDecimal) {
            return ((BigDecimal) operand).toPlainString();
        }

        if ((operand instanceof Double) || (operand instanceof Float)) {
            return new BigDecimal(String.valueOf(operand)).toPlainString();
        }

        if (operand instanceof Number) {
            return String.valueOf(operand);
        }

        if (operand instanceof CharSequence) {
            return String.valueOf(String.valueOf(operand).length());
        }

        throw new IllegalArgumentException("cannot render operand " + operand + " [" + operand.getClass() + "]");
    }
}
