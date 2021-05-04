package eu.okaeri.persistence.filter.predicate.renderer;

import eu.okaeri.persistence.filter.predicate.*;

import java.math.BigDecimal;

public class DefaultPredicateRenderer implements PredicateRenderer {

    private static final PredicateRendererLiteral LITERAL_X = new PredicateRendererLiteral("x");

    @Override
    public String render(Object leftOperand, Predicate<?> predicate) {

        if (predicate == null) {
            throw new IllegalArgumentException("predicate cannot be null");
        }

        if (predicate instanceof EqPredicate) {
            return "(" + this.renderOperand(leftOperand) + " == " + this.renderOperand(predicate) + ")";
        }

        if (predicate instanceof GePredicate) {
            return "(" + this.renderOperand(leftOperand) + " >= " + this.renderOperand(predicate) + ")";
        }

        if (predicate instanceof GtPredicate) {
            return "(" + this.renderOperand(leftOperand) + " > " + this.renderOperand(predicate) + ")";
        }

        if (predicate instanceof LePredicate) {
            return "(" + this.renderOperand(leftOperand) + " <= " + this.renderOperand(predicate) + ")";
        }

        if (predicate instanceof LtPredicate) {
            return "(" + this.renderOperand(leftOperand) + " < " + this.renderOperand(predicate) + ")";
        }

        if (predicate instanceof NePredicate) {
            return "(" + this.renderOperand(leftOperand) + " != " + this.renderOperand(predicate) + ")";
        }

        throw new IllegalArgumentException("cannot render predicate " + predicate + " [" + predicate.getClass() + "]");
    }

    @Override
    public String render(Predicate<?> predicate) {
        return this.render(LITERAL_X, predicate);
    }

    @Override
    public String renderOperand(Object operand) {

        if (operand == null) {
            throw new IllegalArgumentException("predicate cannot be null");
        }

        if (operand instanceof PredicateRendererLiteral) {
            return ((PredicateRendererLiteral) operand).getValue();
        }

        if (operand instanceof Predicate) {
            operand = ((Predicate<?>) operand).getRightOperand();
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
