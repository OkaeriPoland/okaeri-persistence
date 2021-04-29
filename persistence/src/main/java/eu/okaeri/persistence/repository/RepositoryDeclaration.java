package eu.okaeri.persistence.repository;

import eu.okaeri.persistence.PersistenceCollection;
import eu.okaeri.persistence.document.Document;
import eu.okaeri.persistence.document.DocumentPersistence;
import lombok.RequiredArgsConstructor;
import sun.reflect.generics.reflectiveObjects.ParameterizedTypeImpl;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

@RequiredArgsConstructor
public class RepositoryDeclaration<T extends DocumentRepository> {

    public static <A extends DocumentRepository> RepositoryDeclaration<A> of(Class<A> clazz) {

        Map<RepositoryMethod, RepositoryMethodCaller> methods = new HashMap<>();

        for (Method method : clazz.getDeclaredMethods()) {
            RepositoryMethod repositoryMethod = RepositoryMethod.of(method);
            methods.put(repositoryMethod, null); // FIXME: implement dynamic methods
        }

        return new RepositoryDeclaration<A>(clazz, methods);
    }

    private final Class<T> type;
    private final Map<RepositoryMethod, RepositoryMethodCaller> methods;

    @SuppressWarnings("unchecked")
    public T newProxy(DocumentPersistence persistence, PersistenceCollection collection, ClassLoader classLoader) {

        Type[] types = ((ParameterizedTypeImpl) this.type.getGenericInterfaces()[0]).getActualTypeArguments();
        Class<?> pathType = (Class<?>) types[0];
        Class<? extends Document> entityType = (Class<? extends Document>) types[1];
        DefaultDocumentRepository defaultRepository = new DefaultDocumentRepository(persistence, collection, entityType);
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
}