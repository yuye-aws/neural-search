/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch;

import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.search.DocIdSetIterator;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class PartitionedDocIdSetIterator extends DocIdSetIterator {
    private final List<PostingsEnum> postingsEnums;
    private final NumericDocValues clusterIdValues;
    private final Set<Integer> validClusterIds;

    private int currentDoc = -1;
    private final int[] currentPositions;
    private final boolean[] termMatches;

    public PartitionedDocIdSetIterator(List<PostingsEnum> postingsEnums, NumericDocValues clusterIdValues, List<Integer> validClusterIds) {
        this.postingsEnums = postingsEnums;
        this.clusterIdValues = clusterIdValues;
        this.validClusterIds = new HashSet<>(validClusterIds);

        this.currentPositions = new int[postingsEnums.size()];
        this.termMatches = new boolean[postingsEnums.size()];
    }

    @Override
    public int docID() {
        return currentDoc;
    }

    @Override
    public int nextDoc() throws IOException {
        return advance(currentDoc + 1);
    }

    @Override
    public int advance(int target) throws IOException {
        // Implementation of the partitioned posting list algorithm
        while (true) {
            // Find the minimum docID across all posting lists that's >= target
            int minDoc = NO_MORE_DOCS;
            for (int i = 0; i < postingsEnums.size(); i++) {
                PostingsEnum postings = postingsEnums.get(i);

                // Advance this posting list if needed
                if (postings.docID() < target) {
                    currentPositions[i] = postings.advance(target);
                }

                // Track the minimum docID
                if (postings.docID() < minDoc) {
                    minDoc = postings.docID();
                }
            }

            // No more documents
            if (minDoc == NO_MORE_DOCS) {
                return currentDoc = NO_MORE_DOCS;
            }

            // Check if this document is in one of our valid clusters
            if (clusterIdValues.advanceExact(minDoc)) {
                int clusterId = (int) clusterIdValues.longValue();
                if (validClusterIds.contains(clusterId)) {
                    // This document is in a valid cluster, update term match information for scoring
                    for (int i = 0; i < postingsEnums.size(); i++) {
                        termMatches[i] = (postingsEnums.get(i).docID() == minDoc);
                    }
                    return currentDoc = minDoc;
                }
            }

            // If we get here, the document wasn't in a valid cluster, try the next one
            target = minDoc + 1;
        }
    }

    @Override
    public long cost() {
        // Estimate the cost of this iterator
        long cost = 0;
        for (PostingsEnum postings : postingsEnums) {
            cost += postings.cost();
        }
        // Adjust cost based on cluster filtering - this helps the query planner
        return cost / Math.max(1, validClusterIds.size());
    }

    // Helper method for scoring
    public float score(List<Float> weights) {
        float score = 0;
        for (int i = 0; i < termMatches.length; i++) {
            if (termMatches[i]) {
                score += weights.get(i);
            }
        }
        return score;
    }
}
