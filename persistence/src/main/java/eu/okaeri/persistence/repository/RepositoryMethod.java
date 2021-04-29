package eu.okaeri.persistence.repository;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.lang.reflect.Method;
import java.util.Arrays;

@Data
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@SuppressWarnings("FieldNamingConvention")
public class RepositoryMethod {

    private final String returnType;
    private final String name;
    private final String[] args;

    public static RepositoryMethod of(String returnType, String name, String... args) {
        return new RepositoryMethod(returnType, name, args);
    }

    public static RepositoryMethod of(Class returnType, String name, Class<?>... args) {
        return of(returnType.getName(), name, Arrays.stream(args).map(Class::getName).toArray(String[]::new));
    }

    public static RepositoryMethod of(Method method) {
        return of(method.getReturnType(), method.getName(), method.getParameterTypes());
    }
}