/*
 * To change this license header, choose License Headers in Project Properties. To change this template file, choose
 * Tools | Templates and open the template in the editor.
 */
package com.cloudimpl.cluster4j.common;

import io.rsocket.Payload;
import java.nio.ByteBuffer;

/**
 *
 * @author nuwansa
 */
public interface MessageCodec {

   
  Object decode(Payload payload);

  ByteBuffer encode(Object obj);

}
