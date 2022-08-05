package eu.okaeri.persistence.repository;

import eu.okaeri.persistence.PersistenceCollection;
import eu.okaeri.persistence.PersistenceEntity;
import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.document.Document;
import eu.okaeri.persistence.document.DocumentPersistence;
import eu.okaeri.persistence.repository.annotation.DocumentPath;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class RepositoryDeclaration<T extends DocumentRepository> {

    private final Class<T> type;
    private final Map<Method, RepositoryMethodCaller> methods;
    private final Class<?> pathType;
    private final Class<? extends Document> entityType;

    @SuppressWarnings("unchecked")
    public static <A extends DocumentRepository> RepositoryDeclaration<A> of(@NonNull Class<A> clazz) {

        Map<Method, RepositoryMethodCaller> methods = new HashMap<>();
        Type[] types = ((ParameterizedType) clazz.getGenericInterfaces()[0]).getActualTypeArguments();
        Class<?> pathType = (Class<?>) types[0];
        Class<? extends Document> entityType = (Class<? extends Document>) types[1];

        for (Method method : clazz.getDeclaredMethods()) {

            DocumentPath property = method.getAnnotation(DocumentPath.class);
            if (property == null) {
                continue;
            }

            if (method.getParameterCount() != 1) {
                throw new RuntimeException("Methods using @DocumentPath must have a single argument: " + method);
            }

            PersistencePath path = PersistencePath.parse(property.value(), ".");
            Class<?> insideType = getInsideType(method);

            if (method.getReturnType() == Optional.class) {
                if (insideType == PersistenceEntity.class) {
                    methods.put(method, (persistence, collection, args) -> persistence.readByProperty(collection, path, args[0])
                        .findFirst()
                        .map(entity -> entity.into(entityType)));
                } else {
                    methods.put(method, (persistence, collection, args) -> persistence.readByProperty(collection, path, args[0])
                        .findFirst()
                        .map(entity -> entity.into(entityType))
                        .map(PersistenceEntity::getValue));
                }
                continue;
            }

            if (method.getReturnType() == Stream.class) {
                if (insideType == PersistenceEntity.class) {
                    methods.put(method, (persistence, collection, args) -> persistence.readByProperty(collection, path, args[0])
                        .map(entity -> entity.into(entityType)));
                } else {
                    methods.put(method, (persistence, collection, args) -> persistence.readByProperty(collection, path, args[0])
                        .map(entity -> entity.into(entityType))
                        .map(PersistenceEntity::getValue));
                }
                continue;
            }

            if ((method.getReturnType() == List.class) || (method.getReturnType() == Collection.class)) {
                if (insideType == PersistenceEntity.class) {
                    methods.put(method, (persistence, collection, args) -> persistence.readByProperty(collection, path, args[0])
                        .map(entity -> entity.into(entityType))
                        .collect(Collectors.toList()));
                } else {
                    methods.put(method, (persistence, collection, args) -> persistence.readByProperty(collection, path, args[0])
                        .map(entity -> entity.into(entityType))
                        .map(PersistenceEntity::getValue)
                        .collect(Collectors.toList()));
                }
                continue;
            }

            if (method.getReturnType() == Set.class) {
                if (insideType == PersistenceEntity.class) {
                    methods.put(method, (persistence, collection, args) -> persistence.readByProperty(collection, path, args[0])
                        .map(entity -> entity.into(entityType))
                        .collect(Collectors.toSet()));
                } else {
                    methods.put(method, (persistence, collection, args) -> persistence.readByProperty(collection, path, args[0])
                        .map(entity -> entity.into(entityType))
                        .map(PersistenceEntity::getValue)
                        .collect(Collectors.toSet()));
                }
            }
        }

        return new RepositoryDeclaration<A>(clazz, methods, pathType, entityType);
    }

    private static Class<?> getInsideType(Method method) {

        ParameterizedType genericReturnType = (ParameterizedType) method.getGenericReturnType();
        Type actualTypeArgument = genericReturnType.getActualTypeArguments()[0];

        if (actualTypeArgument instanceof Class<?>) {
            return (Class<?>) actualTypeArgument;
        }

        if (actualTypeArgument instanceof ParameterizedType) {
            return ((Class<?>) ((ParameterizedType) actualTypeArgument).getRawType());
        }

        throw new IllegalArgumentException("cannot resolve inside type of " + method);
    }

    @SuppressWarnings("unchecked")
    public T newProxy(@NonNull DocumentPersistence persistence, @NonNull PersistenceCollection collection, @NonNull ClassLoader classLoader) {

        DefaultDocumentRepository defaultRepository = new DefaultDocumentRepository(persistence, collection, this.entityType);
        Map<Method, Method> defaultRepositoryMethods = new HashMap<>();

        return (T) Proxy.newProxyInstance(classLoader, new Class[]{this.type}, (proxy, method, args) -> {

            // third party interface methods
            Class<?> dClass = method.getDeclaringClass();
            if (method.isDefault()) {
                try {
                    MethodType methodType = MethodType.methodType(method.getReturnType(), method.getParameterTypes());
                    return MethodHandles.lookup().findSpecial(dClass, method.getName(), methodType, dClass).bindTo(proxy).invokeWithArguments(args);
                } catch (IllegalAccessException ignored) {
                }
                // java 8 fallback
                Constructor<MethodHandles.Lookup> constructor = MethodHandles.Lookup.class.getDeclaredConstructor(Class.class);
                constructor.setAccessible(true);
                return constructor.newInstance(dClass).in(dClass).unreflectSpecial(method, dClass).bindTo(proxy).invokeWithArguments(args);
            }

            // okaeri-persistence provided impl
            try {
                Method defaultMethod;
                if (defaultRepositoryMethods.containsKey(method)) {
                    defaultMethod = defaultRepositoryMethods.get(method);
                } else {
                    defaultMethod = defaultRepository.getClass().getMethod(method.getName(), method.getParameterTypes());
                    defaultRepositoryMethods.put(method, defaultMethod);
                }
                if (defaultMethod != null) {
                    try {
                        return defaultMethod.invoke(defaultRepository, args);
                    } catch (InvocationTargetException exception) {
                        throw exception.getCause();
                    }
                }
            } catch (NoSuchMethodException | SecurityException ignored) {
                defaultRepositoryMethods.put(method, null);
            }

            // okaeri-persistence generated (e.g. @PersistencePath)
            RepositoryMethodCaller caller = this.methods.get(method);
            if (caller == null) {
                throw new IllegalArgumentException("cannot proxy " + method);
            }

            return caller.call(persistence, collection, args);
        });
    }
}
