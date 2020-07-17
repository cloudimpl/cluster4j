package com.cloudimpl.cluster4j.collection.error;

import com.cloudimpl.error.core.ErrorBuilder;

public class KEY_VIOLATION extends ErrorBuilder {
  KEY_VIOLATION() {
     withCode(com.cloudimpl.cluster4j.collection.error.Collection.KEY_VIOLATION);
  }
}
