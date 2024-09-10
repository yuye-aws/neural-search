/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.search.pipeline.PipelineProcessingContext;
import org.opensearch.search.pipeline.SearchResponseProcessor;

import lombok.Getter;

@Getter
public class RerankNestedQueryProcessor implements SearchResponseProcessor {

    public static final String TYPE = "rerank_nested_query";

    private final String description;
    private final String tag;
    private final boolean ignoreFailure;

    public RerankNestedQueryProcessor(String description, String tag, boolean ignoreFailure) {
        this.description = description;
        this.tag = tag;
        this.ignoreFailure = ignoreFailure;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public SearchResponse processResponse(final SearchRequest request, final SearchResponse response) throws Exception {
        throw new UnsupportedOperationException("Use asyncProcessResponse unless you can guarantee to not deadlock yourself");
    }

    @Override
    public void processResponseAsync(
        final SearchRequest request,
        final SearchResponse response,
        final PipelineProcessingContext ctx,
        final ActionListener<SearchResponse> responseListener
    ) {
        try {
            responseListener.onResponse(response);
        } catch (Exception e) {
            responseListener.onFailure(e);
        }
    }
}
