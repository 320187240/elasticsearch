/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.xcontent;

/**
 * 这是一个允许使用 {@link XContentBuilder} 将对象转换为 "XContent" 的接口。
 * {@link ToXContentFragment} 与 {@link ToXContentObject} 之间的区别在于，
 * 前者输出的是一个片段，需要外部开始和结束一个新的匿名对象；而后者则保证输出的内容在语法上是完整的，不需要任何外部补充。
 */
public interface ToXContentObject extends ToXContent {

    @Override
    default boolean isFragment() {
        return false;
    }
}
