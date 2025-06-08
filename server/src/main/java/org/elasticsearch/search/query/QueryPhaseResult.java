/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.query;

import org.elasticsearch.common.lucene.search.TopDocsAndMaxScore;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.profile.query.CollectorResult;

/**
 * 包括搜索作作为查询阶段的一部分返回的结果。
 * @param topDocsAndMaxScore {@link org.apache.lucene.search.TopDocs} 作为可选的最高分数
 * @param sortValueFormats 对请求进行排序的字段
 * @param终止请求是否提前终止后，根据 <code>terminate_after</code> 功能
 * @param collectorResult 分析结果（启用分析时）
 */
public record QueryPhaseResult(
    TopDocsAndMaxScore topDocsAndMaxScore,
    DocValueFormat[] sortValueFormats,
    boolean terminatedAfter,
    CollectorResult collectorResult
) {}
