package eu.okaeri.persistence.filter.condition.renderer;

import eu.okaeri.persistence.filter.condition.Condition;

public interface ConditionRenderer {

  String render(Condition condition);
}
