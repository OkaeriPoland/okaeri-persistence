package eu.okaeri.persistencetestjdbc.relations.entity;

import eu.okaeri.persistence.document.Document;
import eu.okaeri.persistencetestjdbc.relations.LazyRef;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;

@Data
@Builder(toBuilder = true)
@EqualsAndHashCode(callSuper = false)
public class Book extends Document {
    private String title;
    private List<LazyRef<Author>> authors;
}
