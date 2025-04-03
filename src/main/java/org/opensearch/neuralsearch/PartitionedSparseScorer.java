/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch;

public class PartitionedSparseScorer extends Scorer {
    private final PartitionedDocIdSetIterator iterator;
    private final List<Float> weights;

    public PartitionedSparseScorer(Weight weight, PartitionedDocIdSetIterator iterator, List<Float> weights) {
        super(weight);
        this.iterator = iterator;
        this.weights = weights;
    }

    @Override
    public int docID() {
        return iterator.docID();
    }

    @Override
    public float score() throws IOException {
        // Calculate score based on matching terms for the current document
        return iterator.score(weights);
    }

    @Override
    public DocIdSetIterator iterator() {
        return iterator;
    }

    @Override
    public float getMaxScore(int upTo) throws IOException {
        // Calculate maximum possible score
        float maxScore = 0;
        for (Float weight : weights) {
            maxScore += weight;
        }
        return maxScore;
    }
}
