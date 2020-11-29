package com.cloudimpl.cluster4j.collection.error;

import com.cloudimpl.error.core.ErrorCode;



public enum Collection implements ErrorCode {

  CLASS_NOT_FOUND(1, "class [className] not found"),
  OPERATION_NOT_SUPPORTED(2, "operation [opName] not supported"),
  KEY_VIOLATION(3, "map key violation"),
  CONSTRUCTOR_NOT_SUPPORTED(4, "constructor not supported");
 
  private final int errorNo;

  private final String errorFormat;

  Collection(int errorNo, String errorFormat) {
    this.errorNo = errorNo;
    this.errorFormat = errorFormat;
  }

  @Override
  public int getErrorNo() {
    return errorNo;
  }

  @Override
  public String getFormat() {
    return errorFormat;
  }
}
