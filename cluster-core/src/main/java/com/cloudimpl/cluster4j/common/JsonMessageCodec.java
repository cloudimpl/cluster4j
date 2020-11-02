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
public class JsonMessageCodec implements MessageCodec {
    private static final MessageCodec instance;

    static {
        instance = new JsonMessageCodec();
    }

    public static MessageCodec instance() {
        return instance;
    }

    @Override
    public Object decode(Payload payload) {
        System.out.println("payload :"+payload.getDataUtf8());
        return GsonCodec.decode(GsonCodec.toJsonObject(payload.getDataUtf8()));
    }

//  @Override
//  public <T> T decode(Class<T> cls, ByteBuffer buffer) {
//    byte[] bytesArray = new byte[buffer.remaining()];
//    buffer.get(bytesArray, 0, bytesArray.length);
//    return GsonCodec.decode(cls, new String(bytesArray));
//  }
    @Override
    public ByteBuffer encode(Object obj) {
        return ByteBuffer.wrap(GsonCodec.encodeWithType(obj).getBytes());
    }

}
