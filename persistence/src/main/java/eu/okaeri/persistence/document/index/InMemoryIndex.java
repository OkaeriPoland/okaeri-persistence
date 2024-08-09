package eu.okaeri.persistence.document.index;

import eu.okaeri.persistence.document.Document;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;

@Getter
public class InMemoryIndex extends Document {

  // full type hint for okaeri-configs
  private final ConcurrentHashMap<String, String> keyToValue = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, Set<String>> valueToKeys = new ConcurrentHashMap<>();
}
