/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.io.stream;

import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.io.stream.Writeable.Writer;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.core.CharArrays;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.Text;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetTime;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.IntFunction;

import static java.util.Map.entry;

/**
 * 一个从其他节点流向本节点的流。技术上讲，它也可以从字节数组中读取流，但这主要是用于测试。
 *
 * 此类的方法经过优化，使得你可以将读取和写入某个类的方法并列放置，并方便地在视觉上进行对比，查找差异。
 * 这意味着大多数变量应该在一行内完成读或写操作，这样即使对象较大，也能让读写操作同时显示在屏幕上。
 * 同时这也意味着此类中的方法命名应与 {@link StreamInput} 类非常相似。
 * 最后，这意味着即便这是一个被广泛使用的类，其方法的新增门槛也相对较低。
 * 顺便提一句，这个类主要处理的是 {@code List} 而不是数组。
 * 大多数情况下，调用应该适配列表，可以通过内部存储 {@code List}，
 * 或者在调用时进行列表的转换。
 * 此段注释同样出现在 {@link StreamInput} 类中。
 */
public abstract class StreamOutput extends OutputStream {

    private TransportVersion version = TransportVersion.current();

    /**
     * The transport version to serialize the data as.
     */
    public TransportVersion getTransportVersion() {
        return this.version;
    }

    /**
     * Set the transport version of the data in this stream.
     */
    public void setTransportVersion(TransportVersion version) {
        this.version = version;
    }

    public long position() throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Writes a single byte.
     */
    public abstract void writeByte(byte b) throws IOException;

    /**
     * Writes an array of bytes.
     *
     * @param b the bytes to write
     */
    public void writeBytes(byte[] b) throws IOException {
        writeBytes(b, 0, b.length);
    }

    /**
     * Writes an array of bytes.
     *
     * @param b      the bytes to write
     * @param length the number of bytes to write
     */
    public void writeBytes(byte[] b, int length) throws IOException {
        writeBytes(b, 0, length);
    }

    /**
     * Writes an array of bytes.
     *
     * @param b      the bytes to write
     * @param offset the offset in the byte array
     * @param length the number of bytes to write
     */
    public abstract void writeBytes(byte[] b, int offset, int length) throws IOException;

    /**
     * Writes an array of bytes.
     *
     * @param b the bytes to write
     */
    public void writeByteArray(byte[] b) throws IOException {
        writeVInt(b.length);
        writeBytes(b, 0, b.length);
    }

    /**
     * Serializes a writable just like {@link Writeable#writeTo(StreamOutput)} would but prefixes it with the serialized size of the result.
     *
     * @param writeable {@link Writeable} to serialize
     * @deprecated use {@link #writeWithSizePrefix} instead
     */
    @Deprecated
    public void legacyWriteWithSizePrefix(Writeable writeable) throws IOException {
        final BytesStreamOutput tmp = new BytesStreamOutput();
        tmp.setTransportVersion(version);
        writeable.writeTo(tmp);
        writeBytesReference(tmp.bytes());
    }

    /**
     * Serializes a writable just like {@link Writeable#writeTo(StreamOutput)} would but also compresses and prefixes it with the serialized
     * size of the result.

     *
     * @param writeable {@link Writeable} to serialize
     */
    public void writeWithSizePrefix(Writeable writeable) throws IOException {
        final BytesStreamOutput tmp = new BytesStreamOutput();
        try (var o = new OutputStreamStreamOutput(CompressorFactory.COMPRESSOR.threadLocalOutputStream(tmp))) {
            o.setTransportVersion(version);
            writeable.writeTo(o);
        }
        var bytes = tmp.bytes();
        this.writeInt(bytes.length());
        bytes.writeTo(this);
    }

    /**
     * Writes the bytes reference, including a length header.
     */
    public void writeBytesReference(@Nullable BytesReference bytes) throws IOException {
        if (bytes == null) {
            writeVInt(0);
            return;
        }
        writeVInt(bytes.length());
        bytes.writeTo(this);
    }

    /**
     * Writes an optional bytes reference including a length header. Use this if you need to differentiate between null and empty bytes
     * references. Use {@link #writeBytesReference(BytesReference)} and {@link StreamInput#readBytesReference()} if you do not.
     */
    public void writeOptionalBytesReference(@Nullable BytesReference bytes) throws IOException {
        if (bytes == null) {
            writeVInt(0);
            return;
        }
        writeVInt(bytes.length() + 1);
        bytes.writeTo(this);
    }

    public void writeBytesRef(BytesRef bytes) throws IOException {
        if (bytes == null) {
            writeVInt(0);
            return;
        }
        writeVInt(bytes.length);
        write(bytes.bytes, bytes.offset, bytes.length);
    }

    private static final ThreadLocal<byte[]> scratch = ThreadLocal.withInitial(() -> new byte[1024]);

    public final void writeShort(short v) throws IOException {
        final byte[] buffer = scratch.get();
        ByteUtils.writeShortBE(v, buffer, 0);
        writeBytes(buffer, 0, 2);
    }

    /**
     * Writes an int as four bytes.
     */
    public void writeInt(int i) throws IOException {
        final byte[] buffer = scratch.get();
        ByteUtils.writeIntBE(i, buffer, 0);
        writeBytes(buffer, 0, 4);
    }

    /**
     * Writes an int as four bytes, least significant bytes first.
     */
    public void writeIntLE(int i) throws IOException {
        final byte[] buffer = scratch.get();
        ByteUtils.writeIntLE(i, buffer, 0);
        writeBytes(buffer, 0, 4);
    }

    /**
     * Writes an int in a variable-length format.  Writes between one and
     * five bytes.  Smaller values take fewer bytes.  Negative numbers
     * will always use all 5 bytes and are therefore better serialized
     * using {@link #writeInt}
     */
    public void writeVInt(int i) throws IOException {
        /*
         * Shortcut writing single byte because it is very, very common and
         * can skip grabbing the scratch buffer. This is marginally slower
         * than hand unrolling the entire encoding loop but hand unrolling
         * the encoding loop blows out the method size so it can't be inlined.
         * In that case benchmarks of the method itself are faster but
         * benchmarks of methods that use this method are slower.
         * This is philosophically in line with vint in general - it biases
         * twoards being simple and fast for smaller numbers.
         */
        if (Integer.numberOfLeadingZeros(i) >= 25) {
            writeByte((byte) i);
            return;
        }
        byte[] buffer = scratch.get();
        int index = putMultiByteVInt(buffer, i, 0);
        writeBytes(buffer, 0, index);
    }

    public static int putVInt(byte[] buffer, int i, int off) {
        if (Integer.numberOfLeadingZeros(i) >= 25) {
            buffer[off] = (byte) i;
            return 1;
        }
        return putMultiByteVInt(buffer, i, off);
    }

    private static int putMultiByteVInt(byte[] buffer, int i, int off) {
        int index = off;
        do {
            buffer[index++] = ((byte) ((i & 0x7f) | 0x80));
            i >>>= 7;
        } while ((i & ~0x7F) != 0);
        buffer[index++] = (byte) i;
        return index - off;
    }

    /**
     * Writes a long as eight bytes.
     */
    public void writeLong(long i) throws IOException {
        final byte[] buffer = scratch.get();
        ByteUtils.writeLongBE(i, buffer, 0);
        writeBytes(buffer, 0, 8);
    }

    /**
     * Writes a long as eight bytes.
     */
    public void writeLongLE(long i) throws IOException {
        final byte[] buffer = scratch.get();
        ByteUtils.writeLongLE(i, buffer, 0);
        writeBytes(buffer, 0, 8);
    }

    /**
     * 以可变长度格式写入一个非负长整数。写入1到10个字节。值越小，占用的字节越少。负数
     * 数字占用十个字节，且在测试中运行时会进行断言（如果运行），因此建议使用{@link #writeLong(long)}或{@link #writeZLong(long)}
     * 负数。
     */
    public void writeVLong(long i) throws IOException {
        if (i < 0) {
            throw new IllegalStateException("Negative longs unsupported, use writeLong or writeZLong for negative numbers [" + i + "]");
        }
        writeVLongNoCheck(i);
    }

    public void writeOptionalVLong(@Nullable Long l) throws IOException {
        if (l == null) {
            writeBoolean(false);
        } else {
            writeBoolean(true);
            writeVLong(l);
        }
    }

    /**
     * Writes a long in a variable-length format without first checking if it is negative. Package private for testing. Use
     * {@link #writeVLong(long)} instead.
     */
    void writeVLongNoCheck(long i) throws IOException {
        final byte[] buffer = scratch.get();
        int index = 0;
        while ((i & ~0x7F) != 0) {
            buffer[index++] = ((byte) ((i & 0x7f) | 0x80));
            i >>>= 7;
        }
        buffer[index++] = ((byte) i);
        writeBytes(buffer, 0, index);
    }

    /**
     * 以可变长度格式写入 long。写入 1 到 10 个字节。
     * 通过将符号位滑入 lsb 然后编码为无符号数字来重新映射值
     * 例如，0 -;&gt; 0， -1 -;&gt; 1， 1 -;&gt; 2， ...， Long.MIN_VALUE -;&gt; -1， Long.MAX_VALUE -;&gt; -2
     * 绝对值较小的数字将具有较小的编码
     * 如果已知数字为非负数，请使用 {@link #writeVLong（long）}
     */
    public void writeZLong(long i) throws IOException {
        final byte[] buffer = scratch.get();
        int index = 0;
        // zig-zag encoding cf. https://developers.google.com/protocol-buffers/docs/encoding?hl=en
        long value = BitUtil.zigZagEncode(i);
        while ((value & 0xFFFFFFFFFFFFFF80L) != 0L) {
            buffer[index++] = (byte) ((value & 0x7F) | 0x80);
            value >>>= 7;
        }
        buffer[index++] = (byte) (value & 0x7F);
        writeBytes(buffer, 0, index);
    }

    public void writeOptionalLong(@Nullable Long l) throws IOException {
        if (l == null) {
            writeBoolean(false);
        } else {
            writeBoolean(true);
            writeLong(l);
        }
    }

    public void writeOptionalString(@Nullable String str) throws IOException {
        if (str == null) {
            writeBoolean(false);
        } else {
            byte[] buffer = scratch.get();
            // put the true byte into the buffer instead of writing it outright to do fewer flushes
            buffer[0] = ONE;
            writeString(str, buffer, 1);
        }
    }

    public void writeOptionalSecureString(@Nullable SecureString secureStr) throws IOException {
        if (secureStr == null) {
            writeOptionalBytesReference(null);
        } else {
            final byte[] secureStrBytes = CharArrays.toUtf8Bytes(secureStr.getChars());
            try {
                writeOptionalBytesReference(new BytesArray(secureStrBytes));
            } finally {
                Arrays.fill(secureStrBytes, (byte) 0);
            }
        }
    }

    /**
     * Writes an optional {@link Integer}.
     */
    public void writeOptionalInt(@Nullable Integer integer) throws IOException {
        if (integer == null) {
            writeBoolean(false);
        } else {
            writeBoolean(true);
            writeInt(integer);
        }
    }

    public void writeOptionalVInt(@Nullable Integer integer) throws IOException {
        if (integer == null) {
            writeBoolean(false);
        } else {
            writeBoolean(true);
            writeVInt(integer);
        }
    }

    public void writeOptionalFloat(@Nullable Float floatValue) throws IOException {
        if (floatValue == null) {
            writeBoolean(false);
        } else {
            writeBoolean(true);
            writeFloat(floatValue);
        }
    }

    public void writeOptionalText(@Nullable Text text) throws IOException {
        if (text == null) {
            writeInt(-1);
        } else {
            writeText(text);
        }
    }

    private static final ThreadLocal<BytesRefBuilder> spareBytesRefBuilder = ThreadLocal.withInitial(BytesRefBuilder::new);

    public void writeText(Text text) throws IOException {
        if (text.hasBytes() == false) {
            final String string = text.string();
            var spare = spareBytesRefBuilder.get();
            spare.copyChars(string);
            writeInt(spare.length());
            write(spare.bytes(), 0, spare.length());
        } else {
            var encoded = text.bytes();
            BytesReference bytes = new BytesArray(encoded.bytes(), encoded.offset(), encoded.length());
            writeInt(bytes.length());
            bytes.writeTo(this);
        }
    }

    public void writeString(String str) throws IOException {
        writeString(str, scratch.get(), 0);
    }

    /**
     * Write string as well as possibly the beginning of the given {@code buffer}. The given {@code buffer} will also be used when encoding
     * the given string.
     *
     * @param str string to write
     * @param buffer buffer that may hold some bytes to write
     * @param off how many bytes in {code buffer} to write
     * @throws IOException on failure
     */
    private void writeString(String str, byte[] buffer, int off) throws IOException {
        final int charCount = str.length();
        int offset = off + putVInt(buffer, charCount, off);
        for (int i = 0; i < charCount; i++) {
            final int c = str.charAt(i);
            if (c <= 0x007F) {
                buffer[offset++] = ((byte) c);
            } else if (c > 0x07FF) {
                buffer[offset++] = ((byte) (0xE0 | c >> 12 & 0x0F));
                buffer[offset++] = ((byte) (0x80 | c >> 6 & 0x3F));
                buffer[offset++] = ((byte) (0x80 | c >> 0 & 0x3F));
            } else {
                buffer[offset++] = ((byte) (0xC0 | c >> 6 & 0x1F));
                buffer[offset++] = ((byte) (0x80 | c >> 0 & 0x3F));
            }
            // make sure any possible char can fit into the buffer in any possible iteration
            // we need at most 3 bytes so we flush the buffer once we have less than 3 bytes
            // left before we start another iteration
            if (offset > buffer.length - 3) {
                writeBytes(buffer, offset);
                offset = 0;
            }
        }
        writeBytes(buffer, offset);
    }

    public void writeSecureString(SecureString secureStr) throws IOException {
        final byte[] secureStrBytes = CharArrays.toUtf8Bytes(secureStr.getChars());
        try {
            writeBytesReference(new BytesArray(secureStrBytes));
        } finally {
            Arrays.fill(secureStrBytes, (byte) 0);
        }
    }

    public void writeFloat(float v) throws IOException {
        writeInt(Float.floatToIntBits(v));
    }

    public void writeDouble(double v) throws IOException {
        writeLong(Double.doubleToLongBits(v));
    }

    public void writeDoubleLE(double v) throws IOException {
        writeLongLE(Double.doubleToLongBits(v));
    }

    public void writeOptionalDouble(@Nullable Double v) throws IOException {
        if (v == null) {
            writeBoolean(false);
        } else {
            writeBoolean(true);
            writeDouble(v);
        }
    }

    private static final byte ZERO = 0;
    private static final byte ONE = 1;
    private static final byte TWO = 2;

    /**
     * Writes a boolean.
     */
    public void writeBoolean(boolean b) throws IOException {
        writeByte(b ? ONE : ZERO);
    }

    public void writeOptionalBoolean(@Nullable Boolean b) throws IOException {
        if (b == null) {
            writeByte(TWO);
        } else {
            writeBoolean(b);
        }
    }

    /**
     * Forces any buffered output to be written.
     */
    @Override
    public abstract void flush() throws IOException;

    /**
     * Closes this stream to further operations.
     */
    @Override
    public abstract void close() throws IOException;

    @Override
    public void write(int b) throws IOException {
        writeByte((byte) b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        writeBytes(b, off, len);
    }

    public void writeStringArray(String[] array) throws IOException {
        writeVInt(array.length);
        for (String s : array) {
            writeString(s);
        }
    }

    /**
     * Writes a string array, for nullable string, writes it as 0 (empty string).
     */
    public void writeStringArrayNullable(@Nullable String[] array) throws IOException {
        if (array == null) {
            writeVInt(0);
        } else {
            writeStringArray(array);
        }
    }

    /**
     * Writes a string array, for nullable string, writes false.
     */
    public void writeOptionalStringArray(@Nullable String[] array) throws IOException {
        if (array == null) {
            writeBoolean(false);
        } else {
            writeBoolean(true);
            writeStringArray(array);
        }
    }

    /**
     * Writes a byte array, for null arrays it writes false.
     * @param array an array or null
     */
    public void writeOptionalByteArray(@Nullable byte[] array) throws IOException {
        if (array == null) {
            writeBoolean(false);
        } else {
            writeBoolean(true);
            writeByteArray(array);
        }
    }

    /**
     * Writes a float array, for null arrays it writes false.
     * @param array an array or null
     */
    public void writeOptionalFloatArray(@Nullable float[] array) throws IOException {
        if (array == null) {
            writeBoolean(false);
        } else {
            writeBoolean(true);
            writeFloatArray(array);
        }
    }

    public void writeGenericMap(@Nullable Map<String, Object> map) throws IOException {
        writeGenericValue(map);
    }

    /**
     * write map to stream with consistent order
     * to make sure every map generated bytes order are same.
     * This method is compatible with {@code StreamInput.readMap} and {@code StreamInput.readGenericValue}
     * This method only will handle the map keys order, not maps contained within the map
     */
    public void writeMapWithConsistentOrder(@Nullable Map<String, ? extends Object> map) throws IOException {
        if (map == null) {
            writeByte((byte) -1);
            return;
        }
        assert false == (map instanceof LinkedHashMap);
        this.writeByte((byte) 10);
        this.writeVInt(map.size());
        Iterator<? extends Map.Entry<String, ?>> iterator = map.entrySet().stream().sorted(Map.Entry.comparingByKey()).iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, ?> next = iterator.next();
            if (this.getTransportVersion().onOrAfter(TransportVersions.V_8_7_0)) {
                this.writeGenericValue(next.getKey());
            } else {
                this.writeString(next.getKey());
            }
            this.writeGenericValue(next.getValue());
        }
    }

    /**
     * Writes values of a map as a collection
     */
    public final <V> void writeMapValues(final Map<?, V> map, final Writer<V> valueWriter) throws IOException {
        writeCollection(map.values(), valueWriter);
    }

    /**
     * Writes values of a map as a collection
     */
    public final <V extends Writeable> void writeMapValues(final Map<?, V> map) throws IOException {
        writeMapValues(map, StreamOutput::writeWriteable);
    }

    /**
     * Write a {@link Map} of {@code K}-type keys to {@code V}-type.
     */
    public final <K extends Writeable, V extends Writeable> void writeMap(final Map<K, V> map) throws IOException {
        writeMap(map, StreamOutput::writeWriteable, StreamOutput::writeWriteable);
    }

    /**
     * Write a {@link Map} of {@code K}-type keys to {@code V}-type.
     * <pre><code>
     * Map&lt;String, String&gt; map = ...;
     * out.writeMap(map, StreamOutput::writeString, StreamOutput::writeString);
     * </code></pre>
     *
     * @param keyWriter The key writer
     * @param valueWriter The value writer
     */
    public final <K, V> void writeMap(final Map<K, V> map, final Writer<K> keyWriter, final Writer<V> valueWriter) throws IOException {
        int size = map.size();
        writeVInt(size);
        if (size > 0) {
            for (final Map.Entry<K, V> entry : map.entrySet()) {
                keyWriter.write(this, entry.getKey());
                valueWriter.write(this, entry.getValue());
            }
        }
    }

    /**
     * Same as {@link #writeMap(Map, Writer, Writer)} but for {@code String} keys.
     */
    public final <V> void writeMap(final Map<String, V> map, final Writer<V> valueWriter) throws IOException {
        writeMap(map, StreamOutput::writeString, valueWriter);
    }

    /**
     * Writes an {@link Instant} to the stream with nanosecond resolution
     */
    public final void writeInstant(Instant instant) throws IOException {
        writeLong(instant.getEpochSecond());
        writeInt(instant.getNano());
    }

    /**
     * Writes an {@link Instant} to the stream, which could possibly be null
     */
    public final void writeOptionalInstant(@Nullable Instant instant) throws IOException {
        if (instant == null) {
            writeBoolean(false);
        } else {
            writeBoolean(true);
            writeInstant(instant);
        }
    }

    private static final Map<Class<?>, Writer<?>> WRITERS = Map.ofEntries(
        entry(String.class, (o, v) -> o.writeGenericString((String) v)),
        entry(Integer.class, (o, v) -> {
            o.writeByte((byte) 1);
            o.writeInt((Integer) v);
        }),
        entry(Long.class, (o, v) -> {
            o.writeByte((byte) 2);
            o.writeLong((Long) v);
        }),
        entry(Float.class, (o, v) -> {
            o.writeByte((byte) 3);
            o.writeFloat((float) v);
        }),
        entry(Double.class, (o, v) -> {
            o.writeByte((byte) 4);
            o.writeDouble((double) v);
        }),
        entry(Boolean.class, (o, v) -> {
            o.writeByte((byte) 5);
            o.writeBoolean((boolean) v);
        }),
        entry(byte[].class, (o, v) -> {
            o.writeByte((byte) 6);
            final byte[] bytes = (byte[]) v;
            o.writeVInt(bytes.length);
            o.writeBytes(bytes);
        }),
        entry(List.class, (o, v) -> o.writeGenericList((List<?>) v, StreamOutput::writeGenericValue)),
        entry(Object[].class, (o, v) -> {
            o.writeByte((byte) 8);
            final Object[] list = (Object[]) v;
            o.writeArray(StreamOutput::writeGenericValue, list);
        }),
        entry(Map.class, (o, v) -> {
            if (v instanceof LinkedHashMap) {
                o.writeByte((byte) 9);
            } else {
                o.writeByte((byte) 10);
            }
            if (o.getTransportVersion().onOrAfter(TransportVersions.V_8_7_0)) {
                final Map<?, ?> map = (Map<?, ?>) v;
                o.writeMap(map, StreamOutput::writeGenericValue, StreamOutput::writeGenericValue);
            } else {
                @SuppressWarnings("unchecked")
                final Map<String, ?> map = (Map<String, ?>) v;
                o.writeMap(map, StreamOutput::writeGenericValue);
            }
        }),
        entry(Byte.class, (o, v) -> {
            o.writeByte((byte) 11);
            o.writeByte((Byte) v);
        }),
        entry(Date.class, (o, v) -> {
            o.writeByte((byte) 12);
            o.writeLong(((Date) v).getTime());
        }),
        entry(BytesReference.class, (o, v) -> {
            o.writeByte((byte) 14);
            o.writeBytesReference((BytesReference) v);
        }),
        entry(Text.class, (o, v) -> {
            o.writeByte((byte) 15);
            o.writeText((Text) v);
        }),
        entry(Short.class, (o, v) -> {
            o.writeByte((byte) 16);
            o.writeShort((Short) v);
        }),
        entry(int[].class, (o, v) -> {
            o.writeByte((byte) 17);
            o.writeIntArray((int[]) v);
        }),
        entry(long[].class, (o, v) -> {
            o.writeByte((byte) 18);
            o.writeLongArray((long[]) v);
        }),
        entry(float[].class, (o, v) -> {
            o.writeByte((byte) 19);
            o.writeFloatArray((float[]) v);
        }),
        entry(double[].class, (o, v) -> {
            o.writeByte((byte) 20);
            o.writeDoubleArray((double[]) v);
        }),
        entry(BytesRef.class, (o, v) -> {
            o.writeByte((byte) 21);
            o.writeBytesRef((BytesRef) v);
        }),
        entry(GeoPoint.class, (o, v) -> {
            o.writeByte((byte) 22);
            o.writeGeoPoint((GeoPoint) v);
        }),
        entry(ZonedDateTime.class, (o, v) -> {
            o.writeByte((byte) 23);
            final ZonedDateTime zonedDateTime = (ZonedDateTime) v;
            o.writeString(zonedDateTime.getZone().getId());
            Instant instant = zonedDateTime.toInstant();
            if (o.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)) {
                o.writeZLong(instant.getEpochSecond());
                o.writeInt(instant.getNano());
            } else {
                o.writeLong(instant.toEpochMilli());
            }
        }),
        entry(Set.class, (o, v) -> {
            if (v instanceof LinkedHashSet) {
                o.writeByte((byte) 24);
            } else {
                o.writeByte((byte) 25);
            }
            o.writeCollection((Set<?>) v, StreamOutput::writeGenericValue);
        }),
        entry(
            // TODO: improve serialization of BigInteger
            BigInteger.class,
            (o, v) -> {
                o.writeByte((byte) 26);
                o.writeString(v.toString());
            }
        ),
        entry(OffsetTime.class, (o, v) -> {
            o.writeByte((byte) 27);
            final OffsetTime offsetTime = (OffsetTime) v;
            o.writeString(offsetTime.getOffset().getId());
            o.writeLong(offsetTime.toLocalTime().toNanoOfDay());
        }),
        entry(Duration.class, (o, v) -> {
            o.writeByte((byte) 28);
            final Duration duration = (Duration) v;
            o.writeLong(duration.getSeconds());
            o.writeLong(duration.getNano());
        }),
        entry(Period.class, (o, v) -> {
            o.writeByte((byte) 29);
            final Period period = (Period) v;
            o.writeInt(period.getYears());
            o.writeInt(period.getMonths());
            o.writeInt(period.getDays());
        }),
        entry(GenericNamedWriteable.class, (o, v) -> {
            // Note that we do not rely on the checks in VersionCheckingStreamOutput because that only applies to CCS
            final var genericNamedWriteable = (GenericNamedWriteable) v;
            if (genericNamedWriteable.supportsVersion(o.getTransportVersion()) == false) {
                final var message = Strings.format(
                    "[%s] doesn't support serialization with transport version [%s]",
                    genericNamedWriteable.getWriteableName(),
                    o.getTransportVersion()
                );
                assert false : message;
                throw new IllegalStateException(message);
            }
            o.writeByte((byte) 30);
            o.writeNamedWriteable(genericNamedWriteable);
        })
    );

    public static final byte GENERIC_LIST_HEADER = (byte) 7;

    public <T> void writeGenericList(List<T> v, Writer<T> writer) throws IOException {
        writeByte(GENERIC_LIST_HEADER);
        writeCollection(v, writer);
    }

    public void writeGenericString(String value) throws IOException {
        byte[] buffer = scratch.get();
        // put the 0 type identifier byte into the buffer instead of writing it outright to do fewer flushes
        buffer[0] = 0;
        writeString(value, buffer, 1);
    }

    public void writeGenericNull() throws IOException {
        writeByte((byte) -1);
    }

    private static Class<?> getGenericType(Object value) {
        if (value instanceof List) {
            return List.class;
        } else if (value instanceof Object[]) {
            return Object[].class;
        } else if (value instanceof Map) {
            return Map.class;
        } else if (value instanceof Set) {
            return Set.class;
        } else if (value instanceof BytesReference) {
            return BytesReference.class;
        } else if (value instanceof GenericNamedWriteable) {
            return GenericNamedWriteable.class;
        } else {
            return value.getClass();
        }
    }

    /**
     * Notice: when serialization a map, the stream out map with the stream in map maybe have the
     * different key-value orders, they will maybe have different stream order.
     * If want to keep stream out map and stream in map have the same stream order when stream,
     * can use {@code writeMapWithConsistentOrder}
     */
    public void writeGenericValue(@Nullable Object value) throws IOException {
        if (value == null) {
            writeGenericNull();
            return;
        }
        final Class<?> type = getGenericType(value);
        @SuppressWarnings("unchecked")
        final Writer<Object> writer = (Writer<Object>) WRITERS.get(type);
        if (writer != null) {
            writer.write(this, value);
        } else {
            throw new IllegalArgumentException("can not write type [" + type + "]");
        }
    }

    public static void checkWriteable(@Nullable Object value) throws IllegalArgumentException {
        if (value == null) {
            return;
        }
        final Class<?> type = getGenericType(value);

        if (type == List.class) {
            @SuppressWarnings("unchecked")
            List<Object> list = (List<Object>) value;
            for (Object v : list) {
                checkWriteable(v);
            }
        } else if (type == Object[].class) {
            Object[] array = (Object[]) value;
            for (Object v : array) {
                checkWriteable(v);
            }
        } else if (type == Map.class) {
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) value;
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                checkWriteable(entry.getKey());
                checkWriteable(entry.getValue());
            }
        } else if (type == Set.class) {
            @SuppressWarnings("unchecked")
            Set<Object> set = (Set<Object>) value;
            for (Object v : set) {
                checkWriteable(v);
            }
        } else if (WRITERS.containsKey(type) == false) {
            throw new IllegalArgumentException("Cannot write type [" + type.getCanonicalName() + "] to stream");
        }
    }

    public void writeIntArray(int[] values) throws IOException {
        writeVInt(values.length);
        for (int value : values) {
            writeInt(value);
        }
    }

    public void writeVIntArray(int[] values) throws IOException {
        writeVInt(values.length);
        for (int value : values) {
            writeVInt(value);
        }
    }

    public void writeLongArray(long[] values) throws IOException {
        writeVInt(values.length);
        for (long value : values) {
            writeLong(value);
        }
    }

    public void writeVLongArray(long[] values) throws IOException {
        writeVInt(values.length);
        for (long value : values) {
            writeVLong(value);
        }
    }

    public void writeFloatArray(float[] values) throws IOException {
        writeVInt(values.length);
        for (float value : values) {
            writeFloat(value);
        }
    }

    public void writeDoubleArray(double[] values) throws IOException {
        writeVInt(values.length);
        for (double value : values) {
            writeDouble(value);
        }
    }

    /**
     * Writes the specified array to the stream using the specified {@link Writer} for each element in the array. This method can be seen as
     * writer version of {@link StreamInput#readArray(Writeable.Reader, IntFunction)}. The length of array encoded as a variable-length
     * integer is first written to the stream, and then the elements of the array are written to the stream.
     *
     * @param writer the writer used to write individual elements
     * @param array  the array
     * @param <T>    the type of the elements of the array
     * @throws IOException if an I/O exception occurs while writing the array
     */
    public <T> void writeArray(final Writer<T> writer, final T[] array) throws IOException {
        writeVInt(array.length);
        for (T value : array) {
            writer.write(this, value);
        }
    }

    /**
     * Same as {@link #writeArray(Writer, Object[])} but the provided array may be null. An additional boolean value is
     * serialized to indicate whether the array was null or not.
     */
    public <T> void writeOptionalArray(final Writer<T> writer, final @Nullable T[] array) throws IOException {
        if (array == null) {
            writeBoolean(false);
        } else {
            writeBoolean(true);
            writeArray(writer, array);
        }
    }

    /**
     * Writes the specified array of {@link Writeable}s. This method can be seen as
     * writer version of {@link StreamInput#readArray(Writeable.Reader, IntFunction)}. The length of array encoded as a variable-length
     * integer is first written to the stream, and then the elements of the array are written to the stream.
     */
    public <T extends Writeable> void writeArray(T[] array) throws IOException {
        writeArray(StreamOutput::writeWriteable, array);
    }

    /**
     * Same as {@link #writeArray(Writeable[])} but the provided array may be null. An additional boolean value is
     * serialized to indicate whether the array was null or not.
     */
    public <T extends Writeable> void writeOptionalArray(@Nullable T[] array) throws IOException {
        writeOptionalArray(StreamOutput::writeWriteable, array);
    }

    /**
     * Writes a boolean value indicating whether the given object is {@code null}, followed by the object's serialization if it is not
     * {@code null}.
     *
     * @see StreamInput#readOptionalWriteable
     */
    public void writeOptionalWriteable(@Nullable Writeable writeable) throws IOException {
        if (writeable != null) {
            writeBoolean(true);
            writeable.writeTo(this);
        } else {
            writeBoolean(false);
        }
    }

    /**
     * Writes a boolean value indicating whether the given object is {@code null}, followed by the object's serialization if it is not
     * {@code null}.
     *
     * @see StreamInput#readOptional
     */
    public <T> void writeOptional(Writer<T> writer, @Nullable T maybeItem) throws IOException {
        if (maybeItem != null) {
            writeBoolean(true);
            writer.write(this, maybeItem);
        } else {
            writeBoolean(false);
        }
    }

    /**
     * This method allow to use a method reference when writing collection elements such as
     * {@code out.writeMap(map, StreamOutput::writeString, StreamOutput::writeWriteable)}
     */
    public void writeWriteable(Writeable writeable) throws IOException {
        writeable.writeTo(this);
    }

    public void writeException(Throwable throwable) throws IOException {
        ElasticsearchException.writeException(throwable, this);
    }

    /**
     * Writes a {@link NamedWriteable} to the current stream, by first writing its name and then the object itself
     */
    public void writeNamedWriteable(NamedWriteable namedWriteable) throws IOException {
        writeString(namedWriteable.getWriteableName());
        namedWriteable.writeTo(this);
    }

    /**
     * Write an optional {@link NamedWriteable} to the stream.
     */
    public void writeOptionalNamedWriteable(@Nullable NamedWriteable namedWriteable) throws IOException {
        if (namedWriteable == null) {
            writeBoolean(false);
        } else {
            writeBoolean(true);
            writeNamedWriteable(namedWriteable);
        }
    }

    /**
     * Writes the given {@link GeoPoint} to the stream
     */
    public void writeGeoPoint(GeoPoint geoPoint) throws IOException {
        writeDouble(geoPoint.lat());
        writeDouble(geoPoint.lon());
    }

    /**
     * Write a {@linkplain ZoneId} to the stream.
     */
    public void writeZoneId(ZoneId timeZone) throws IOException {
        writeString(timeZone.getId());
    }

    /**
     * Write an optional {@linkplain ZoneId} to the stream.
     */
    public void writeOptionalZoneId(@Nullable ZoneId timeZone) throws IOException {
        if (timeZone == null) {
            writeBoolean(false);
        } else {
            writeBoolean(true);
            writeZoneId(timeZone);
        }
    }

    /**
     * Writes a collection which can then be read using {@link StreamInput#readCollectionAsList} or another {@code readCollectionAs*}
     * method. Make sure to read the collection back into the same type as was originally written.
     */
    public void writeCollection(final Collection<? extends Writeable> collection) throws IOException {
        writeCollection(collection, StreamOutput::writeWriteable);
    }

    /**
     * Writes a collection which can then be read using {@link StreamInput#readCollectionAsList} or another {@code readCollectionAs*}
     * method. Make sure to read the collection back into the same type as was originally written.
     */
    public <T> void writeCollection(final Collection<T> collection, final Writer<T> writer) throws IOException {
        writeVInt(collection.size());
        for (final T val : collection) {
            writer.write(this, val);
        }
    }

    /**
     * Writes a collection of strings which can then be read using {@link StreamInput#readStringCollectionAsList} or another {@code
     * readStringCollectionAs*} method. Make sure to read the collection back into the same type as was originally written.
     */
    public void writeStringCollection(final Collection<String> collection) throws IOException {
        writeCollection(collection, StreamOutput::writeString);
    }

    /**
     * Writes a possibly-{@code null} collection which can then be read using {@link StreamInput#readOptionalCollectionAsList}.
     */
    public <T extends Writeable> void writeOptionalCollection(@Nullable final Collection<T> collection) throws IOException {
        writeOptionalCollection(collection, StreamOutput::writeWriteable);
    }

    /**
     * Writes a possibly-{@code null} collection which can then be read using {@link StreamInput#readOptionalCollectionAsList}.
     */
    public <T> void writeOptionalCollection(@Nullable final Collection<T> collection, final Writer<T> writer) throws IOException {
        if (collection != null) {
            writeBoolean(true);
            writeCollection(collection, writer);
        } else {
            writeBoolean(false);
        }
    }

    /**
     * Writes a possibly-{@code null} collection of strings which can then be read using
     * {@link StreamInput#readOptionalStringCollectionAsList}.
     */
    public void writeOptionalStringCollection(@Nullable final Collection<String> collection) throws IOException {
        writeOptionalCollection(collection, StreamOutput::writeString);
    }

    /**
     * Writes a collection of {@link NamedWriteable} objects which can then be read using {@link
     * StreamInput#readNamedWriteableCollectionAsList}.
     */
    public void writeNamedWriteableCollection(Collection<? extends NamedWriteable> list) throws IOException {
        writeCollection(list, StreamOutput::writeNamedWriteable);
    }

    /**
     * Writes an enum with type {@code E} in terms of the value of its ordinal. Enums serialized like this must have a corresponding test
     * which uses {@code EnumSerializationTestUtils#assertEnumSerialization} to fix the wire protocol.
     */
    public <E extends Enum<E>> void writeEnum(E enumValue) throws IOException {
        assert enumValue instanceof XContentType == false : "XContentHelper#writeTo should be used for XContentType serialisation";
        writeVInt(enumValue.ordinal());
    }

    /**
     * Writes an optional enum with type {@code E} in terms of the value of its ordinal. Enums serialized like this must have a
     * corresponding test which uses {@code EnumSerializationTestUtils#assertEnumSerialization} to fix the wire protocol.
     */
    public <E extends Enum<E>> void writeOptionalEnum(@Nullable E enumValue) throws IOException {
        if (enumValue == null) {
            writeBoolean(false);
        } else {
            writeBoolean(true);
            assert enumValue instanceof XContentType == false : "XContentHelper#writeTo should be used for XContentType serialisation";
            writeVInt(enumValue.ordinal());
        }
    }

    /**
     * Writes a set of enum with type {@code E} in terms of the value of its ordinal. Enums serialized like this must have a corresponding
     * test which uses {@code EnumSerializationTestUtils#assertEnumSerialization} to fix the wire protocol.
     */
    public <E extends Enum<E>> void writeEnumSet(EnumSet<E> enumSet) throws IOException {
        writeVInt(enumSet.size());
        for (E e : enumSet) {
            writeEnum(e);
        }
    }

    /**
     * Write a {@link TimeValue} to the stream
     */
    public void writeTimeValue(TimeValue timeValue) throws IOException {
        writeZLong(timeValue.duration());
        writeByte((byte) timeValue.timeUnit().ordinal());
    }

    /**
     * Write an optional {@link TimeValue} to the stream.
     */
    public void writeOptionalTimeValue(@Nullable TimeValue timeValue) throws IOException {
        if (timeValue == null) {
            writeBoolean(false);
        } else {
            writeBoolean(true);
            writeTimeValue(timeValue);
        }
    }

    /**
     * Similar to {@link #writeOptionalWriteable} but for use when the value is always missing.
     */
    public <T extends Writeable> void writeMissingWriteable(Class<T> ignored) throws IOException {
        writeBoolean(false);
    }

    /**
     * Similar to {@link #writeOptionalString} but for use when the value is always missing.
     */
    public void writeMissingString() throws IOException {
        writeBoolean(false);
    }

    /**
     * Write a {@link BigInteger} to the stream
     */
    public void writeBigInteger(BigInteger bigInteger) throws IOException {
        writeString(bigInteger.toString());
    }
}
