package eu.okaeri.persistence.repository.annotation;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface Index {
    String path();
    int maxLength();
}
