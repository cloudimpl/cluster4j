package com.cloudimpl.cluster4j.collection.error;

import com.cloudimpl.error.core.ErrorBuilder;

public class CLASS_NOT_FOUND extends ErrorBuilder {
  CLASS_NOT_FOUND() {
     withCode(com.cloudimpl.cluster4j.collection.error.Collection.CLASS_NOT_FOUND);
  }

  public CLASS_NOT_FOUND setClassName(Object className) {
    withTag("className", className);
     return this;
  }
}
