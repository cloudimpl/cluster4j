/*
 * To change this license header, choose License Headers in Project Properties. To change this template file, choose
 * Tools | Templates and open the template in the editor.
 */
package com.cloudimpl.cluster4j.common;

import io.netty.buffer.ByteBuf;
import java.nio.ByteBuffer;

/**
 *
 * @author nuwansa
 */
public interface MessageCodec {

  <T> T decode(Class<T> cls, ByteBuf buffer);

  <T> T decode(Class<T> cls, ByteBuffer buffer);

  ByteBuffer encode(Object obj);

}
