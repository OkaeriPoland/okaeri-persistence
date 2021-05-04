package eu.okaeri.persistencetestmem.basic.entity;

import lombok.AllArgsConstructor;
import lombok.Data;

// this class represtends unknown non-serializable
// by the okaeri-configs entity that may be used
// for the purposes of in-memory documents
@Data
@AllArgsConstructor
public class UnknownEntity {
    private int age;
}
