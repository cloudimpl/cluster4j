/*
 * To change this license header, choose License Headers in Project Properties. To change this template file, choose
 * Tools | Templates and open the template in the editor.
 */
package com.cloudimpl.cluster4j.common;

import java.util.function.Function;

/**
 *
 * @author nuwansa
 */
@FunctionalInterface
public interface CheckedFunction<T, R> {
  R apply(T t) throws Exception;

  public static <T, R> Function<T, R> wrap(CheckedFunction<T, R> checkedFunction) {
    return t -> {
      try {
        return checkedFunction.apply(t);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    };
  }
}
