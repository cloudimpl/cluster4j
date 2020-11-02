//package com.cloudimpl.cluster4j.node;
//
////import com.cloudimpl.cluster4j.coreImpl.MessageCodecImpl;
//import com.fasterxml.jackson.core.type.TypeReference;
//import io.rsocket.util.DefaultPayload;
//import io.scalecube.cluster.metadata.MetadataDecoder;
//import io.scalecube.cluster.metadata.MetadataEncoder;
//import java.nio.ByteBuffer;
//import java.util.Collections;
//import java.util.Map;
//import reactor.core.Exceptions;
//
//public final class SimpleMapMetadataCodec implements MetadataEncoder, MetadataDecoder {
//
//  public static final SimpleMapMetadataCodec INSTANCE = new SimpleMapMetadataCodec();
//
//  private static final TypeReference TYPE = new TypeReference<Map<String, String>>() {};
//
//  @Override
//  public Object decode(ByteBuffer buffer) {
//    try {
//      if (buffer.remaining() == 0) {
//        return Collections.emptyMap();
//      }
//      return MessageCodecImpl.instance().decode(Map.class, DefaultPayload.create(buffer));
//    } catch (Exception e) {
//      throw Exceptions.propagate(e);
//    }
//  }
//
//  @Override
//  public ByteBuffer encode(Object metadata) {
//    try {
//      return MessageCodecImpl.instance().encode(metadata);
//    } catch (Exception e) {
//      throw Exceptions.propagate(e);
//    }
//  }
//}
