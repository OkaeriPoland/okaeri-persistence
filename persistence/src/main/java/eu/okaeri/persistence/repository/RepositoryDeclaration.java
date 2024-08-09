package eu.okaeri.persistence.repository;

import eu.okaeri.persistence.PersistenceCollection;
import eu.okaeri.persistence.PersistenceEntity;
import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.document.Document;
import eu.okaeri.persistence.document.DocumentPersistence;
import eu.okaeri.persistence.repository.annotation.DocumentPath;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Proxy;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class RepositoryDeclaration<T extends DocumentRepository> {

  private final Class<T> type;
  private final Map<Method, RepositoryMethodCaller> methods;
  private final Class<?> pathType;
  private final Class<? extends Document> entityType;

  @SuppressWarnings("unchecked")
  public static <A extends DocumentRepository> RepositoryDeclaration<A> of(
      @NonNull final Class<A> clazz) {

    final Map<Method, RepositoryMethodCaller> methods = new HashMap<>();
    final Type[] types =
        ((ParameterizedType) clazz.getGenericInterfaces()[0]).getActualTypeArguments();
    final Class<?> pathType = (Class<?>) types[0];
    final Class<? extends Document> entityType = (Class<? extends Document>) types[1];

    for (final Method method : clazz.getDeclaredMethods()) {

      final DocumentPath property = method.getAnnotation(DocumentPath.class);
      if (property == null) {
        continue;
      }

      if (method.getParameterCount() != 1) {
        throw new RuntimeException(
            "Methods using @DocumentPath must have a single argument: " + method);
      }

      final PersistencePath path = PersistencePath.parse(property.value(), ".");
      final Class<?> insideType = getInsideType(method);

      final boolean ignoreCase = property.ignoreCase() && method.getParameterTypes()[0] == String.class;
      System.out.println("IGNORECASE: " + ignoreCase + "\n");

      if (method.getReturnType() == Optional.class) {
        if (insideType == PersistenceEntity.class) {
          methods.put(
              method,
              (persistence, collection, args) ->
                  (ignoreCase
                          ? persistence.readByPropertyIgnoreCase(collection, path, (String) args[0])
                          : persistence.readByProperty(collection, path, args[0]))
                      .findFirst()
                      .map(entity -> entity.into(entityType)));
        } else {
          methods.put(
              method,
              (persistence, collection, args) ->
                  (ignoreCase
                          ? persistence.readByPropertyIgnoreCase(collection, path, (String) args[0])
                          : persistence.readByProperty(collection, path, args[0]))
                      .findFirst()
                      .map(entity -> entity.into(entityType))
                      .map(PersistenceEntity::getValue));
        }
        continue;
      }

      if (method.getReturnType() == Stream.class) {
        if (insideType == PersistenceEntity.class) {
          methods.put(
              method,
              (persistence, collection, args) ->
                  (ignoreCase
                          ? persistence.readByPropertyIgnoreCase(collection, path, (String) args[0])
                          : persistence.readByProperty(collection, path, args[0]))
                      .map(entity -> entity.into(entityType)));
        } else {
          methods.put(
              method,
              (persistence, collection, args) ->
                  (ignoreCase
                          ? persistence.readByPropertyIgnoreCase(collection, path, (String) args[0])
                          : persistence.readByProperty(collection, path, args[0]))
                      .map(entity -> entity.into(entityType))
                      .map(PersistenceEntity::getValue));
        }
        continue;
      }

      if ((method.getReturnType() == List.class) || (method.getReturnType() == Collection.class)) {
        if (insideType == PersistenceEntity.class) {
          methods.put(
              method,
              (persistence, collection, args) ->
                  (ignoreCase
                          ? persistence.readByPropertyIgnoreCase(collection, path, (String) args[0])
                          : persistence.readByProperty(collection, path, args[0]))
                      .map(entity -> entity.into(entityType))
                      .collect(Collectors.toList()));
        } else {
          methods.put(
              method,
              (persistence, collection, args) ->
                  (ignoreCase
                          ? persistence.readByPropertyIgnoreCase(collection, path, (String) args[0])
                          : persistence.readByProperty(collection, path, args[0]))
                      .map(entity -> entity.into(entityType))
                      .map(PersistenceEntity::getValue)
                      .collect(Collectors.toList()));
        }
        continue;
      }

      if (method.getReturnType() == Set.class) {
        if (insideType == PersistenceEntity.class) {
          methods.put(
              method,
              (persistence, collection, args) ->
                  (ignoreCase
                          ? persistence.readByPropertyIgnoreCase(collection, path, (String) args[0])
                          : persistence.readByProperty(collection, path, args[0]))
                      .map(entity -> entity.into(entityType))
                      .collect(Collectors.toSet()));
        } else {
          methods.put(
              method,
              (persistence, collection, args) ->
                  (ignoreCase
                          ? persistence.readByPropertyIgnoreCase(collection, path, (String) args[0])
                          : persistence.readByProperty(collection, path, args[0]))
                      .map(entity -> entity.into(entityType))
                      .map(PersistenceEntity::getValue)
                      .collect(Collectors.toSet()));
        }
      }
    }

    return new RepositoryDeclaration<A>(clazz, methods, pathType, entityType);
  }

  private static Class<?> getInsideType(final Method method) {

    final ParameterizedType genericReturnType = (ParameterizedType) method.getGenericReturnType();
    final Type actualTypeArgument = genericReturnType.getActualTypeArguments()[0];

    if (actualTypeArgument instanceof Class<?>) {
      return (Class<?>) actualTypeArgument;
    }

    if (actualTypeArgument instanceof ParameterizedType) {
      return ((Class<?>) ((ParameterizedType) actualTypeArgument).getRawType());
    }

    throw new IllegalArgumentException("cannot resolve inside type of " + method);
  }

  @SuppressWarnings("unchecked")
  public T newProxy(
      @NonNull final DocumentPersistence persistence,
      @NonNull final PersistenceCollection collection,
      @NonNull final ClassLoader classLoader) {

    final DefaultDocumentRepository defaultRepository =
        new DefaultDocumentRepository(persistence, collection, this.entityType);
    final Map<Method, Method> defaultRepositoryMethods = new HashMap<>();

    return (T)
        Proxy.newProxyInstance(
            classLoader,
            new Class[] {this.type},
            (proxy, method, args) -> {

              // third party interface methods
              final Class<?> dClass = method.getDeclaringClass();
              if (method.isDefault()) {
                try {
                  final MethodType methodType =
                      MethodType.methodType(method.getReturnType(), method.getParameterTypes());
                  return MethodHandles.lookup()
                      .findSpecial(dClass, method.getName(), methodType, dClass)
                      .bindTo(proxy)
                      .invokeWithArguments(args);
                } catch (final IllegalAccessException ignored) {
                }
                // java 8 fallback
                final Constructor<MethodHandles.Lookup> constructor =
                    MethodHandles.Lookup.class.getDeclaredConstructor(Class.class);
                constructor.setAccessible(true);
                return constructor
                    .newInstance(dClass)
                    .in(dClass)
                    .unreflectSpecial(method, dClass)
                    .bindTo(proxy)
                    .invokeWithArguments(args);
              }

              // okaeri-persistence provided impl
              try {
                final Method defaultMethod;
                if (defaultRepositoryMethods.containsKey(method)) {
                  defaultMethod = defaultRepositoryMethods.get(method);
                } else {
                  defaultMethod =
                      defaultRepository
                          .getClass()
                          .getMethod(method.getName(), method.getParameterTypes());
                  defaultRepositoryMethods.put(method, defaultMethod);
                }
                if (defaultMethod != null) {
                  try {
                    return defaultMethod.invoke(defaultRepository, args);
                  } catch (final InvocationTargetException exception) {
                    throw exception.getCause();
                  }
                }
              } catch (final NoSuchMethodException | SecurityException ignored) {
                defaultRepositoryMethods.put(method, null);
              }

              // okaeri-persistence generated (e.g. @PersistencePath)
              final RepositoryMethodCaller caller = this.methods.get(method);
              if (caller == null) {
                throw new IllegalArgumentException("cannot proxy " + method);
              }

              return caller.call(persistence, collection, args);
            });
  }
}
