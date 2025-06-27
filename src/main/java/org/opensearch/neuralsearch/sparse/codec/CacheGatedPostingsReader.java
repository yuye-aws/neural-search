/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import org.apache.lucene.util.BytesRef;
import org.opensearch.neuralsearch.sparse.algorithm.PostingClusters;
import org.opensearch.neuralsearch.sparse.common.InMemoryKey;

import java.io.IOException;
import java.util.Set;

public class CacheGatedPostingsReader {
    private final String field;
    // SparseTermsLuceneReader to read sparse terms from disk
    private final SparseTermsLuceneReader luceneReader;

    public CacheGatedPostingsReader(
        String field,
        SparseTermsLuceneReader luceneReader
    ) {
        this.field = field;
        this.luceneReader = luceneReader;
    }

    // we return terms from lucene as cache may not have all data due to memory constraint
    public Set<BytesRef> terms() throws IOException {
        return luceneReader.getTerms(field);
    }

    public long size() {
        return luceneReader.getTerms(field).size();
    }

    public PostingClusters read(BytesRef term) throws IOException {
        return luceneReader.read(field, term);
    }
}
