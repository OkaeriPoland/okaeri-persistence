package eu.okaeri.persistence.repository;

import eu.okaeri.persistence.PersistenceCollection;
import eu.okaeri.persistence.PersistenceEntity;
import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.document.Document;
import eu.okaeri.persistence.document.DocumentPersistence;
import eu.okaeri.persistence.repository.annotation.PropertyPath;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import sun.reflect.generics.reflectiveObjects.ParameterizedTypeImpl;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.lang.reflect.Type;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class RepositoryDeclaration<T extends DocumentRepository> {

    @SuppressWarnings("unchecked")
    public static <A extends DocumentRepository> RepositoryDeclaration<A> of(Class<A> clazz) {

        Map<RepositoryMethod, RepositoryMethodCaller> methods = new HashMap<>();
        Type[] types = ((ParameterizedTypeImpl) clazz.getGenericInterfaces()[0]).getActualTypeArguments();
        Class<?> pathType = (Class<?>) types[0];
        Class<? extends Document> entityType = (Class<? extends Document>) types[1];

        for (Method method : clazz.getDeclaredMethods()) {

            PropertyPath property = method.getAnnotation(PropertyPath.class);
            if (property == null) {
                continue;
            }

            RepositoryMethod repositoryMethod = RepositoryMethod.of(method);
            PersistencePath path = PersistencePath.parse(property.value(), ".");
            Class<?> insideType = getInsideType(method);

            if (method.getParameterCount() != 1) {
                continue;
            }

            if (method.getReturnType() == Optional.class) {
                if (insideType == PersistenceEntity.class) {
                    methods.put(repositoryMethod, (persistence, collection, args) -> persistence.readByProperty(collection, path, args[0])
                            .findFirst()
                            .map(entity -> entity.into(entityType)));
                } else {
                    methods.put(repositoryMethod, (persistence, collection, args) -> persistence.readByProperty(collection, path, args[0])
                            .findFirst()
                            .map(entity -> entity.into(entityType))
                            .map(PersistenceEntity::getValue));
                }
                continue;
            }

            if (method.getReturnType() == Stream.class) {
                if (insideType == PersistenceEntity.class) {
                    methods.put(repositoryMethod, (persistence, collection, args) -> persistence.readByProperty(collection, path, args[0])
                            .map(entity -> entity.into(entityType)));
                } else {
                    methods.put(repositoryMethod, (persistence, collection, args) -> persistence.readByProperty(collection, path, args[0])
                            .map(entity -> entity.into(entityType))
                            .map(PersistenceEntity::getValue));
                }
                continue;
            }

            if ((method.getReturnType() == List.class) || (method.getReturnType() == Collection.class)) {
                if (insideType == PersistenceEntity.class) {
                    methods.put(repositoryMethod, (persistence, collection, args) -> persistence.readByProperty(collection, path, args[0])
                            .map(entity -> entity.into(entityType))
                            .collect(Collectors.toList()));
                } else {
                    methods.put(repositoryMethod, (persistence, collection, args) -> persistence.readByProperty(collection, path, args[0])
                            .map(entity -> entity.into(entityType))
                            .map(PersistenceEntity::getValue)
                            .collect(Collectors.toList()));
                }
                continue;
            }

            if (method.getReturnType() == Set.class) {
                if (insideType == PersistenceEntity.class) {
                    methods.put(repositoryMethod, (persistence, collection, args) -> persistence.readByProperty(collection, path, args[0])
                            .map(entity -> entity.into(entityType))
                            .collect(Collectors.toSet()));
                } else {
                    methods.put(repositoryMethod, (persistence, collection, args) -> persistence.readByProperty(collection, path, args[0])
                            .map(entity -> entity.into(entityType))
                            .map(PersistenceEntity::getValue)
                            .collect(Collectors.toSet()));
                }
            }
        }

        return new RepositoryDeclaration<A>(clazz, methods, pathType, entityType);
    }

    private final Class<T> type;
    private final Map<RepositoryMethod, RepositoryMethodCaller> methods;
    private final Class<?> pathType;
    private final Class<? extends Document> entityType;

    @SuppressWarnings("unchecked")
    public T newProxy(DocumentPersistence persistence, PersistenceCollection collection, ClassLoader classLoader) {

        DefaultDocumentRepository defaultRepository = new DefaultDocumentRepository(persistence, collection, this.entityType);
        Map<Method, Method> defaultRepositoryMethods = new HashMap<>();

        return (T) Proxy.newProxyInstance(classLoader, new Class[]{this.type}, (proxy, method, args) -> {

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

            try {
                Method defaultMethod = defaultRepositoryMethods.get(method);
                if (defaultMethod == null) {
                    defaultMethod = defaultRepository.getClass().getMethod(method.getName(), method.getParameterTypes());
                    defaultRepositoryMethods.put(method, defaultMethod);
                }
                return defaultMethod.invoke(defaultRepository, args);
            } catch (NoSuchMethodException | SecurityException ignored) {
            }

            RepositoryMethod repositoryMethod = RepositoryMethod.of(method);
            RepositoryMethodCaller caller = this.methods.get(repositoryMethod);

            if (caller == null) {
                throw new IllegalArgumentException("cannot proxy " + method);
            }

            return caller.call(persistence, collection, args);
        });
    }

    private static Class<?> getInsideType(Method method) {

        ParameterizedTypeImpl genericReturnType = (ParameterizedTypeImpl) method.getGenericReturnType();
        Type actualTypeArgument = genericReturnType.getActualTypeArguments()[0];

        return (actualTypeArgument instanceof Class<?>)
                ? (Class<?>) actualTypeArgument
                : ((ParameterizedTypeImpl) actualTypeArgument).getRawType();
    }
}