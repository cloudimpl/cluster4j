/*
 * To change this license header, choose License Headers in Project Properties. To change this template file, choose
 * Tools | Templates and open the template in the editor.
 */
package com.cloudimpl.cluster4j.core;

import com.cloudimpl.cluster4j.common.Pair;
import com.cloudimpl.cluster4j.core.logger.ILogger;
import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 *
 * @author nuwansa
 */
public class Injector {

  private final Map<Class<?>, Object> map;
  protected final Map<String, Object> nameBinds;

  public Injector() {
    map = new ConcurrentHashMap<>();
    nameBinds = new ConcurrentHashMap<>();
  }

  private Injector(Map<Class<?>, Object> map, Map<String, Object> nameBinds) {
    this.map = new ConcurrentHashMap<>(map);
    this.nameBinds = new ConcurrentHashMap<>(nameBinds);
  }

  public BindHolder bind(Class<?> cls) {
    return new BindHolder(cls, this);
  }

  public NamedBindHolder bind(String name) {
    return new NamedBindHolder(name, this);
  }

  public void nameBind(String name, Object value) {
    nameBinds.put(name, value);
  }

  public <T> T nameBind(String name) {
    return (T) nameBinds.get(name);
  }

  public Injector with(String name, Object value) {
    Injector injector = new Injector(map, nameBinds);
    injector.nameBind(name, value);
    return injector;
  }

  public <T> Injector with(Class<T> clazz, Object value) {
    Injector injector = new Injector(map, nameBinds);
    injector.bind(clazz).to(value);
    return injector;
  }

  public <T> T inject(Class<T> clazz) {
    try {
      T returnObject;
      Constructor<?> constructor = getInjectableConstructorOrDefault(clazz);
      Class<?>[] paramTypes = constructor.getParameterTypes();
      Annotation[][] annotations = constructor.getParameterAnnotations();
      if (constructor.getParameterCount() == 0) {
        returnObject = (T) constructor.newInstance();
      } else {
        returnObject = (T) constructor.newInstance(getConstructorInjects(paramTypes, annotations));
      }
      inject(returnObject);
      return returnObject;
    } catch (InstantiationException | IllegalAccessException | IllegalArgumentException
        | InvocationTargetException ex) {
      throw new InjectException(ex);
    }

  }

  protected Object[] getConstructorInjects(Class<?>[] paramTypes, Annotation[][] annotations) {
    Object[] params = IntStream.range(0, paramTypes.length)
        .mapToObj(i -> new Pair<Class<?>, Annotation[]>(paramTypes[i], annotations[i]))
        .map(tuple -> getInjectAnonValue(tuple)).collect(Collectors.toList()).toArray();
    return params;
  }

  // protected Object[] getConstructorInjects(Class<?>[] paramTypes,
  // Annotation[][] annotations) {
  // return Arrays.asList(paramTypes).stream().map(type ->
  // getInjectValue(type, null)).collect(Collectors.toList()).toArray();
  // }
  public void inject(Object injectableObject) {
    injectMembers(injectableObject);
  }

  public <T> T getInjecterbleInstance(Class<? extends T> cls) {
    return (T) map.get(cls);
  }

  private void injectMembers(Object injectableObject) {
    Class clazz = injectableObject.getClass();
    while (clazz != null) {
      Field[] fields = clazz.getDeclaredFields();
      Arrays.asList(fields).stream().filter(f -> isInjectable(f)).forEach(f -> injectMember(injectableObject, f));
      clazz = clazz.getSuperclass();
    }
  }

  private Constructor getInjectableConstructorOrDefault(Class<?> clazz) {
    Constructor<?>[] constructors = clazz.getConstructors();
    Constructor injectableConstructor = Arrays.asList(constructors).stream()
        .filter(constructor -> isInjectable(constructor)).findFirst().orElse(constructors[0]);
    return injectableConstructor;
  }

  protected boolean isInjectable(Field f) {
    Annotation[] annotations = f.getDeclaredAnnotations();
    return Arrays.stream(annotations).anyMatch(annotation -> annotation instanceof Inject);
  }

  protected boolean isInjectable(Constructor<?> constructor) {
    Annotation[] annotations = constructor.getDeclaredAnnotations();
    return Arrays.stream(annotations).anyMatch(annotation -> annotation instanceof Inject);
  }

  protected void injectMember(Object injectObject, Field field) {
    try {
      field.setAccessible(true);
      Class<?> clazzType = field.getType();
      Object value = getInjectValue(clazzType, field);
      field.set(injectObject, value);
    } catch (IllegalArgumentException | IllegalAccessException ex) {
      throw new InjectException(ex);
    }
  }

  @SuppressWarnings("unchecked")
  protected <T> T getInjectValue(Class<T> clazz, Field field) {
    Object val;
    if (field != null && field.isAnnotationPresent(Named.class)) {
      Named named = field.getAnnotation(Named.class);
      val = nameBinds.get(named.value());
    } else {
      val = map.get(clazz);
    }
    if (val == null) {
      throw new InjectException("inject value not found for clazz " + clazz.getName());
    }
    return (T) val;
  }

  private Object getInjectAnonValue(Pair<Class<?>, Annotation[]> pair) {
    for (Annotation anno : pair.getValue()) {
      if (anno instanceof Named) {
        Named named = (Named) anno;
        if (ILogger.class.isAssignableFrom(pair.getKey())) {
          String[] p = named.value().split("\\.");
          return ((ILogger) map.get(ILogger.class)).createSubLogger(p[0], p[1]);
        }
        if (nameBinds.get(named.value()) == null) {
          throw new InjectException("bind value for " + named.value() + " not found");
        }

        return nameBinds.get(named.value());
      }

    }
    return getInjectValue(pair.getKey(), null);
  }

  public static final class BindHolder {

    private final Class<?> clazz;
    private final Injector injector;

    protected BindHolder(Class<?> clazz, Injector injector) {
      this.clazz = clazz;
      this.injector = injector;
    }

    public void to(Object value) {
      injector.map.put(clazz, value);
    }
  }

  public static final class NamedBindHolder {

    private final String name;
    private final Injector injector;

    protected NamedBindHolder(String name, Injector injector) {
      this.name = name;
      this.injector = injector;
    }

    public void to(Object value) {
      injector.nameBind(name, value);
    }
  }
}
