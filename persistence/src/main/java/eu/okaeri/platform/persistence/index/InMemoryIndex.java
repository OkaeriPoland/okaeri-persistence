package eu.okaeri.platform.persistence.index;

import eu.okaeri.platform.persistence.document.Document;
import lombok.Getter;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@Getter
public class InMemoryIndex extends Document {
    private Map<String, String> keyToValue = new HashMap<>();
    private Map<String, Set<String>> valueToKeys = new HashMap<>();
}
