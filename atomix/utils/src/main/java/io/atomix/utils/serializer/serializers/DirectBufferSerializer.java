/*
 * Copyright © 2020 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.utils.serializer.serializers;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.nio.ByteBuffer;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public class DirectBufferSerializer extends Serializer<DirectBuffer> {
  @Override
  public void write(final Kryo kryo, final Output output, final DirectBuffer object) {
    output.writeByte(getType(object));
    output.writeInt(object.capacity());
    for (int i = 0; i < object.capacity(); i++) {
      output.writeByte(object.getByte(i));
    }
  }

  @Override
  public DirectBuffer read(final Kryo kryo, final Input input, final Class<DirectBuffer> type) {
    final byte bufferType = input.readByte();
    final int capacity = input.readInt();
    final MutableDirectBuffer buffer = allocateBufferFromType(bufferType, capacity);

    for (int i = 0; i < capacity; i++) {
      buffer.putByte(i, input.readByte());
    }

    return buffer;
  }

  private byte getType(final DirectBuffer buffer) {
    if (buffer instanceof UnsafeBuffer) {
      if (buffer.byteArray() != null) {
        return 1;
      }

      return 2;
    }

    if (buffer instanceof ExpandableArrayBuffer) {
      return 3;
    }

    if (buffer instanceof ExpandableDirectByteBuffer) {
      return 4;
    }

    return 5;
  }

  private MutableDirectBuffer allocateBufferFromType(final byte type, final int length) {
    if (type == 1) {
      return new UnsafeBuffer(ByteBuffer.allocate(length));
    }

    if (type == 2) {
      return new UnsafeBuffer(ByteBuffer.allocateDirect(length));
    }

    if (type == 3) {
      return new ExpandableArrayBuffer(length);
    }

    if (type == 4) {
      return new ExpandableDirectByteBuffer(length);
    }

    throw new IllegalArgumentException("Unknown type " + type);
  }
}
