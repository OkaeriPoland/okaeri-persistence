package eu.okaeri.persistencetestjdbc.refs.entity;

import eu.okaeri.persistence.document.Document;
import eu.okaeri.persistence.document.ref.EagerRef;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;

@Data
@Builder(toBuilder = true)
@EqualsAndHashCode(callSuper = false)
public class Book extends Document {
    private String title;
    private List<EagerRef<Author>> authors;
}
