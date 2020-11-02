/*
 * To change this license header, choose License Headers in Project Properties. To change this template file, choose
 * Tools | Templates and open the template in the editor.
 */
package com.cloudimpl.cluster4j.core;

import java.lang.reflect.GenericDeclaration;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author nuwansa
 */
public class CloudUtil {

    public static <T> T newInstance(Injector injector, Class<T> classType) {
        return injector.inject(classType);
    }

    public static <T> T newInstance(Class<T> classType) {
        try {
            return classType.getConstructor().newInstance();
        } catch (NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
            throw new CloudException(ex.getMessage());
        }
    }

    public static <T> Class<T> classForName(String clazz) {
        try {
            return (Class<T>) Class.forName(clazz);
        } catch (ClassNotFoundException ex) {
            throw new CloudException(ex.getMessage());
        }
    }

    public static String getHostIpAddr() {
        try {
            InetAddress localMachine = InetAddress.getLocalHost();

            byte[] addr = localMachine.getAddress();

            // Convert to dot representation
            String ipaddr = "";
            for (int i = 0; i < addr.length; i++) {
                if (i > 0) {
                    ipaddr += ".";
                }
                ipaddr += addr[i] & 0xFF;
            }

            return ipaddr;
        } catch (UnknownHostException ex) {
            throw new CloudException(ex);
        }

    }

    @SuppressWarnings("unchecked")
    public static <T> Class<T> extractGenericParameter(final Class<?> parameterizedSubClass,
            final Class<?> genericSuperClass, final int pos) {
        // a mapping from type variables to actual values (classes)
        Map<TypeVariable<?>, Class<?>> mapping = new HashMap<>();

        Class<?> klass = parameterizedSubClass;
        while (klass != null) {
            Type superType = klass.getGenericSuperclass();
            Type[] genericInterfaces = klass.getGenericInterfaces();
            for (int j = 0; j <= genericInterfaces.length; ++j) {
                Type type = (j == 0) ? superType : genericInterfaces[j - 1];
                if (type instanceof ParameterizedType) {
                    ParameterizedType parType = (ParameterizedType) type;
                    Type rawType = parType.getRawType();
                    if (rawType.equals(genericSuperClass)) {
                        // found
                        Type t = parType.getActualTypeArguments()[pos];
                        if (t instanceof Class<?>) {
                            return (Class<T>) t;
                        } else {
                            return (Class<T>) mapping.get((TypeVariable<?>) t);
                        }
                    }
                    // resolve
                    Type[] vars = ((GenericDeclaration) (parType.getRawType())).getTypeParameters();
                    Type[] args = parType.getActualTypeArguments();
                    for (int i = 0; i < vars.length; i++) {
                        if (args[i] instanceof Class<?>) {
                            mapping.put((TypeVariable) vars[i], (Class<?>) args[i]);
                        } else {
                            mapping.put((TypeVariable) vars[i], mapping.get((TypeVariable<?>) (args[i])));
                        }
                    }
                    klass = (Class<?>) rawType;
                } else {
                    klass = klass.getSuperclass();
                }
            }
        }
        throw new CloudServiceException("template not found");
    }
}
