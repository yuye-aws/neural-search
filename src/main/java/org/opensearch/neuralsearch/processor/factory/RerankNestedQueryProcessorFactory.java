/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.factory;

import java.util.Map;

import org.opensearch.neuralsearch.processor.RerankNestedQueryProcessor;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.SearchResponseProcessor;

import lombok.AllArgsConstructor;

/**
 * Factory for rerank processors. Must:
 * - Instantiate the right kind of rerank processor
 * - Instantiate the appropriate context source fetchers
 */
@AllArgsConstructor
public class RerankNestedQueryProcessorFactory implements Processor.Factory<SearchResponseProcessor> {

    @Override
    public SearchResponseProcessor create(
        final Map<String, Processor.Factory<SearchResponseProcessor>> processorFactories,
        final String tag,
        final String description,
        final boolean ignoreFailure,
        final Map<String, Object> config,
        final Processor.PipelineContext pipelineContext
    ) {
        return new RerankNestedQueryProcessor(description, tag, ignoreFailure);
    }
}
