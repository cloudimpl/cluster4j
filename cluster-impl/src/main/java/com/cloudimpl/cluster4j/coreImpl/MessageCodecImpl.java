/*
 * To change this license header, choose License Headers in Project Properties. To change this template file, choose
 * Tools | Templates and open the template in the editor.
 */
package com.cloudimpl.cluster4j.coreImpl;

import com.cloudimpl.cluster4j.common.MessageCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 *
 * @author nuwansa
 */
public class MessageCodecImpl implements MessageCodec {

  public static final int DEFAULT_BUFFER_SIZE = 1024 * 1024;
  private final int bufferSize;
  private final ThreadLocal<LinkedBuffer> linkedBuffer;
  private static MessageCodec instance;

  static {
    instance = new MessageCodecImpl();
  }

  public MessageCodecImpl() {
    this.bufferSize = DEFAULT_BUFFER_SIZE;
    this.linkedBuffer = ThreadLocal.withInitial(()->LinkedBuffer.allocate(bufferSize));
  }

  public MessageCodecImpl(int bufferSize) {
    this.linkedBuffer = new ThreadLocal<>();
    this.bufferSize = bufferSize;
  }

  @Override
  public <T> T decode(Class<T> cls, ByteBuf buffer) {
    Schema<T> schema = RuntimeSchema.getSchema(cls);
    T msg = schema.newMessage();
    try {
      ProtostuffIOUtil.mergeFrom(new ByteBufInputStream(buffer), msg, schema);
    } catch (IOException ex) {
      throw new EncoderException(ex);
    }
    return msg;
  }

  @Override
  public <T> T decode(Class<T> cls, ByteBuffer buffer) {
    Schema<T> schema = RuntimeSchema.getSchema(cls);
    T msg = schema.newMessage();
    ProtostuffIOUtil.mergeFrom(buffer.array(), msg, schema);
    return msg;
  }

  @Override
  public ByteBuffer encode(Object obj) {
    Schema schema = RuntimeSchema.getSchema(obj.getClass());
    byte[] buffer = ProtostuffIOUtil.toByteArray(obj, schema, getLinkedBuffer());
    return ByteBuffer.wrap(buffer);
  }

  public static MessageCodec instance() {
    return instance;
  }

  private LinkedBuffer getLinkedBuffer() {
    if (linkedBuffer.get() == null) {
      linkedBuffer.set(LinkedBuffer.allocate(bufferSize));
    }

    LinkedBuffer buffer = linkedBuffer.get();
    buffer.clear();
    return buffer;
  }

}
