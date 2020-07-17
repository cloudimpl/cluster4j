package com.cloudimpl.cluster4j.collection.error;

import com.cloudimpl.error.core.ErrorBuilder;

public class OPERATION_NOT_SUPPORTED extends ErrorBuilder {
  OPERATION_NOT_SUPPORTED() {
     withCode(com.cloudimpl.cluster4j.collection.error.Collection.OPERATION_NOT_SUPPORTED);
  }

  public OPERATION_NOT_SUPPORTED setOpName(Object opName) {
    withTag("opName", opName);
     return this;
  }
}
