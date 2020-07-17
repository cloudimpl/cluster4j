package com.cloudimpl.cluster4j.collection.error;

import com.cloudimpl.error.core.CloudImplException;
import com.cloudimpl.error.core.ErrorBuilder;
import java.util.function.Consumer;

public class CollectionException extends CloudImplException {
  CollectionException(ErrorBuilder builder) {
    super(builder);
  }

  /**
   * errorNo : 1 
   * format : "class [className] not found" 
   * tags : "[className]"
   * @param consumer
   * @return
   */
  public static CollectionException CLASS_NOT_FOUND(Consumer<CLASS_NOT_FOUND> consumer) {
    CLASS_NOT_FOUND error = new CLASS_NOT_FOUND();
    consumer.accept(error);
    return new CollectionException(error);}

  /**
   * errorNo : 2 
   * format : "operation [opName] not supported" 
   * tags : "[opName]"
   * @param consumer
   * @return
   */
  public static CollectionException OPERATION_NOT_SUPPORTED(
      Consumer<OPERATION_NOT_SUPPORTED> consumer) {
    OPERATION_NOT_SUPPORTED error = new OPERATION_NOT_SUPPORTED();
    consumer.accept(error);
    return new CollectionException(error);}

  /**
   * errorNo : 3 
   * format : "map key violation" 
   * tags : "[]"
   * @param consumer
   * @return
   */
  public static CollectionException KEY_VIOLATION(Consumer<KEY_VIOLATION> consumer) {
    KEY_VIOLATION error = new KEY_VIOLATION();
    consumer.accept(error);
    return new CollectionException(error);}

  /**
   * errorNo : 4 
   * format : "constructor not supported" 
   * tags : "[]"
   * @param consumer
   * @return
   */
  public static CollectionException CONSTRUCTOR_NOT_SUPPORTED(
      Consumer<CONSTRUCTOR_NOT_SUPPORTED> consumer) {
    CONSTRUCTOR_NOT_SUPPORTED error = new CONSTRUCTOR_NOT_SUPPORTED();
    consumer.accept(error);
    return new CollectionException(error);}
}
