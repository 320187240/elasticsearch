/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.query;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.FilterLeafCollector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TotalHitCountCollector;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * {@link TotalHitCountCollector} 的扩展，支持根据提供的阈值提前终止总点击计数。
 * 请注意，总命中计数可以从 {@link org.apache.lucene.search.Weight#count（LeafReaderContext）} 中检索。
 * 在这种情况下，提前终止仅适用于收集文档的 leaves。
 */
class PartialHitCountCollector extends TotalHitCountCollector {

    private final HitsThresholdChecker hitsThresholdChecker;
    private boolean earlyTerminated;

    PartialHitCountCollector(HitsThresholdChecker hitsThresholdChecker) {
        this.hitsThresholdChecker = hitsThresholdChecker;
    }

    @Override
    public ScoreMode scoreMode() {
        return hitsThresholdChecker.totalHitsThreshold == Integer.MAX_VALUE ? super.scoreMode() : ScoreMode.TOP_DOCS;
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
        if (hitsThresholdChecker.totalHitsThreshold == Integer.MAX_VALUE) {
            return super.getLeafCollector(context);
        }
        earlyTerminateIfNeeded();
        return new FilterLeafCollector(super.getLeafCollector(context)) {
            @Override
            public void collect(int doc) throws IOException {
                earlyTerminateIfNeeded();
                hitsThresholdChecker.incrementHitCount();
                super.collect(doc);
            }
        };
    }

    private void earlyTerminateIfNeeded() {
        if (hitsThresholdChecker.isThresholdReached()) {
            earlyTerminated = true;
            throw new CollectionTerminatedException();
        }
    }

    boolean hasEarlyTerminated() {
        return earlyTerminated;
    }

    static class HitsThresholdChecker {
        private final int totalHitsThreshold;
        private final AtomicInteger numCollected = new AtomicInteger();

        HitsThresholdChecker(int totalHitsThreshold) {
            this.totalHitsThreshold = totalHitsThreshold;
        }

        void incrementHitCount() {
            numCollected.incrementAndGet();
        }

        boolean isThresholdReached() {
            return numCollected.getAcquire() >= totalHitsThreshold;
        }
    }
}
