/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import lombok.extern.log4j.Log4j2;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.opensearch.neuralsearch.sparse.algorithm.DocumentCluster;
import org.opensearch.neuralsearch.sparse.algorithm.PostingClusters;
import org.opensearch.neuralsearch.sparse.common.DocFreq;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * DiskClusteredPostingReader reads posting clusters directly from disk
 */
@Log4j2
public class DiskClusteredPostingReader {
    private final LeafReader leafReader;
    private final String fieldName;

    public DiskClusteredPostingReader(LeafReader leafReader, String fieldName) {
        this.leafReader = leafReader;
        this.fieldName = fieldName;
    }

    public PostingClusters read(BytesRef term) throws IOException {
        Terms terms = Terms.getTerms(leafReader, fieldName);
        if (terms == null) {
            return null;
        }

        TermsEnum termsEnum = terms.iterator();
        if (!termsEnum.seekExact(term)) {
            return null;
        }

        PostingsEnum postingsEnum = termsEnum.postings(null, PostingsEnum.FREQS);
        if (postingsEnum instanceof SparsePostingsEnum sparsePostingsEnum) {
            return sparsePostingsEnum.getClusters();
        } else {
            // If we don't have SparsePostingsEnum, create a simple cluster from standard postings
            List<DocFreq> docs = new ArrayList<>();
            int docId;
            while ((docId = postingsEnum.nextDoc()) != PostingsEnum.NO_MORE_DOCS) {
                docs.add(new DocFreq(docId, (byte) postingsEnum.freq()));
            }

            if (!docs.isEmpty()) {
                List<DocumentCluster> clusters = new ArrayList<>();
                // Create a single cluster with all documents
                // Note: This is a simplified approach - in a real implementation,
                // you would need to compute a proper summary vector
                clusters.add(new DocumentCluster(null, docs, true));
                return new PostingClusters(clusters);
            }
        }

        return null;
    }
}
