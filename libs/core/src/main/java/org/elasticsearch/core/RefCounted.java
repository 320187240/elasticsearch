/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.core;

/**
 *  一个为那些需要在自身不再被任何用户引用时收到通知的对象设计的接口。
 *  此接口实现了基本的引用计数功能。例如，当异步操作持有一个服务对象，
 *  而该服务可能被并发地关闭时，只要还有异步操作在使用它，就应保证其功能可用。
 *  实现此接口的类应在任何时候都进行引用计数。也就是说，当一个对象将被使用时，
 *  必须在其使用前通过调用 #incRef() 方法增加引用计数；
 *  并且在使用结束后，必须在 try/finally 块中调用对应的 #decRef() 方法来释放该对象。
 *  例如：
 * <pre>
 *      inst.incRef();
 *      try {
 *        // 使用 inst...
 *
 *      } finally {
 *          inst.decRef();
 *      }
 * </pre>
 */
public interface RefCounted {

    /**
     * 递增此实例的 refCount。
     *
     * @see #decRef
     * @see #tryIncRef（）
     * @throws IllegalStateException 如果引用计数器不能递增。
     */
    void incRef();

    /**
     * 尝试增加此实例的 refCount。如果 refCount 成功递增，此方法将返回 {@code true}。
     *
     * @see #decRef（）
     * @see #incRef（）
     */
    boolean tryIncRef();

    /**
     * 减少此实例的 refCount。如果 refCount 下降到 0，则此
     * 实例被视为已关闭，不应再使用。
     *
     * @see #incRef
     *
     * @return 如果调用此方法后 ref 计数下降到 0，则返回 {@code true}
     */
    boolean decRef();

    /**
     * 仅当调用方法时至少有一个活动引用时，才返回 {@code true};如果返回 {@code false}，则
     * 对象已关闭;将来尝试获取引用将失败。
     *
     * @return当前是否有对此对象的任何活动引用。
     */
    boolean hasReferences();

    /**
     * 类似于 {@link #incRef（）}，但它还声称它设法获取了 ref，以便在出现错误的情况下使用
     * 如果所有 ref 都已发布。
     */
    default void mustIncRef() {
        if (tryIncRef()) {
            return;
        }
        assert false : AbstractRefCounted.ALREADY_CLOSED_MESSAGE;
        incRef(); // throws an ISE
    }

    /**
     * A noop implementation that always behaves as if it is referenced and cannot be released.
     */
    RefCounted ALWAYS_REFERENCED = new RefCounted() {
        @Override
        public void incRef() {}

        @Override
        public boolean tryIncRef() {
            return true;
        }

        @Override
        public boolean decRef() {
            return false;
        }

        @Override
        public boolean hasReferences() {
            return true;
        }
    };
}
