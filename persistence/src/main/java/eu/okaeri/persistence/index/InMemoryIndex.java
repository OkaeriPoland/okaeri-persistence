package eu.okaeri.persistence.index;

import eu.okaeri.persistence.document.Document;
import lombok.Getter;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Getter
public class InMemoryIndex extends Document {
    // full type hint for okaeri-configs
    private ConcurrentHashMap<String, String> keyToValue = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, Set<String>> valueToKeys = new ConcurrentHashMap<>();
}
