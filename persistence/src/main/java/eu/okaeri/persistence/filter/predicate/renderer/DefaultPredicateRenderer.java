package eu.okaeri.persistence.filter.predicate.renderer;

import eu.okaeri.persistence.filter.predicate.EqPredicate;
import eu.okaeri.persistence.filter.predicate.GePredicate;
import eu.okaeri.persistence.filter.predicate.GtPredicate;
import eu.okaeri.persistence.filter.predicate.LePredicate;
import eu.okaeri.persistence.filter.predicate.LtPredicate;
import eu.okaeri.persistence.filter.predicate.NePredicate;
import eu.okaeri.persistence.filter.predicate.Predicate;
import java.math.BigDecimal;
import lombok.NonNull;

public class DefaultPredicateRenderer implements PredicateRenderer {

  private static final PredicateRendererLiteral LITERAL_X = new PredicateRendererLiteral("x");

  @Override
  public String render(@NonNull final Object leftOperand, @NonNull final Predicate<?> predicate) {

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

    throw new IllegalArgumentException(
        "cannot render predicate " + predicate + " [" + predicate.getClass() + "]");
  }

  @Override
  public String render(@NonNull final Predicate<?> predicate) {
    return this.render(LITERAL_X, predicate);
  }

  @Override
  public String renderOperand(@NonNull Object operand) {

    if (operand instanceof PredicateRendererLiteral) {
      return ((PredicateRendererLiteral) operand).getValue();
    }

    if (operand instanceof Predicate) {
      operand = ((Predicate<?>) operand).getRightOperand();
    }

    if (operand instanceof Double) {
      final double dOp = (Double) operand;
      if (dOp == (int) dOp) {
        return String.valueOf((int) dOp);
      }
    }

    if (operand instanceof Float) {
      final float dOp = (Float) operand;
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

    throw new IllegalArgumentException(
        "cannot render operand " + operand + " [" + operand.getClass() + "]");
  }
}
