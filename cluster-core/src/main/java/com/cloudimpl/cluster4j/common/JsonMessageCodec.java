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
public class JsonMessageCodec implements MessageCodec {

  @Override
  public <T> T decode(Class<T> cls, ByteBuf buffer) {
    byte[] bytes = new byte[buffer.readableBytes()];
    buffer.readBytes(bytes);
    return GsonCodec.decode(cls, new String(bytes));
  }

  @Override
  public <T> T decode(Class<T> cls, ByteBuffer buffer) {
    byte[] bytesArray = new byte[buffer.remaining()];
    buffer.get(bytesArray, 0, bytesArray.length);
    return GsonCodec.decode(cls, new String(bytesArray));
  }

  @Override
  public ByteBuffer encode(Object obj) {
    return ByteBuffer.wrap(GsonCodec.encode(obj).getBytes());
  }

}
