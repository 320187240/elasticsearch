/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.io.stream;

import java.io.IOException;

/**
 * 实现者可以写入 {@linkplain StreamOutput} 并从 {@linkplain StreamInput} 读取。这允许他们被“扔出
 * across the wire“使用 Elasticsearch 的内部协议。如果实现者还实现了 equals 和 hashCode，则由
 * 序列化和反序列化必须相等，并且具有相同的 hashCode。不要求这样的副本完全不变。
 */
public interface Writeable {

    /**
     * Write this into the {@linkplain StreamOutput}.
     */
    void writeTo(StreamOutput out) throws IOException;

    /**
     * 引用可以将某些对象写入 {@link StreamOutput} 的方法。
     * <p>
     * 按照惯例，这是来自 {@link StreamOutput} 本身的方法（例如，{@link StreamOutput# writeString（String）}）。如果该值可以是
     * {@code null}，那么应该使用方法的 “optional” 变体！
     * <p>
     * 大多数类都应该实现 {@link Writeable}，而 {@link Writeable#writeTo（StreamOutput）} 方法应该<em>使用</em>
     * {@link StreamOutput} 方法：
     * <pre><code>
     * public void writeTo（StreamOutput out） 抛出 IOException {
     * out.writeVInt（someValue）;
     * out.writeMapOfLists（someMap， StreamOutput：：writeString， StreamOutput：：writeString）;
     * }
     * </code></pre>
     */
    @FunctionalInterface
    interface Writer<V> {

        /**
         * 将 {@code V} 类型的 {@code value} 写入 {@code out} put 流。
         *
         * @param out Output 也写入 {@code value}
         * @param value 要添加的值
         */
        void write(StreamOutput out, V value) throws IOException;

    }

    /**
     * 对可以从流中读取某些对象的方法的引用。按照惯例，这是一个采用
     * {@linkplain StreamInput} 作为大多数类的参数和枚举等内容的静态方法。从其中一个返回 null
     * 总是错误的 - 为此，我们使用 {@link StreamInput#readOptionalWriteable（Reader）} 等方法。
     * <p>
     * 由于大多数类将通过构造函数（或在枚举的情况下使用静态方法）来实现这一点，因此应该
     *肖：
     * <pre><code>
     * public MyClass（final StreamInput in） 抛出 IOException {
     * this.someValue = in.readVInt（）;
     * this.someMap = in.readMapOfLists（StreamInput：：readString， StreamInput：：readString）;
     * }
     * </code></pre>
     */
    @FunctionalInterface
    interface Reader<V> {

        /**
         * 从流中读取 {@code V} 类型的值。
         *
         * @param输入中读取值
         */
        V read(StreamInput in) throws IOException;
    }

}
