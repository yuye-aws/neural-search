/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch;

public class PartitionedSparseWeight extends Weight {
    private final PartitionedSparseQuery query;
    private final IndexSearcher searcher;
    private final ScoreMode scoreMode;
    private final float boost;
    private final Map<Term, TermStates> termStates;

    public PartitionedSparseWeight(PartitionedSparseQuery query, IndexSearcher searcher, ScoreMode scoreMode, float boost)
        throws IOException {
        super(query);
        this.query = query;
        this.searcher = searcher;
        this.scoreMode = scoreMode;
        this.boost = boost;

        // Precompute term states for efficient term access
        this.termStates = new HashMap<>();
        for (String token : query.getQueryTokens().keySet()) {
            Term term = new Term(query.getFieldName(), token);
            termStates.put(term, TermStates.build(searcher.getTopReaderContext(), term, true));
        }
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
        Scorer scorer = scorer(context);
        if (scorer != null) {
            int newDoc = scorer.iterator().advance(doc);
            if (newDoc == doc) {
                return Explanation.match(scorer.score(), "Score based on partitioned sparse matching in clusters " + query.getClusterIds());
            }
        }
        return Explanation.noMatch("No matching terms in the right clusters");
    }

    @Override
    public Scorer scorer(LeafReaderContext context) throws IOException {
        // Get access to the cluster ID field
        NumericDocValues clusterIdValues = context.reader().getNumericDocValues("cluster_id");
        if (clusterIdValues == null) {
            return null; // No cluster IDs in this segment
        }

        // Create term-based iterators for each query token
        List<PostingsEnum> postingsEnums = new ArrayList<>();
        List<Float> weights = new ArrayList<>();

        for (Map.Entry<String, Float> entry : query.getQueryTokens().entrySet()) {
            Term term = new Term(query.getFieldName(), entry.getKey());
            TermState state = termStates.get(term).get(context.ord);
            if (state == null) {
                continue; // Term doesn't exist in this segment
            }

            TermsEnum termsEnum = context.reader().terms(query.getFieldName()).iterator();
            termsEnum.seekExact(entry.getKey(), state);

            PostingsEnum postings = termsEnum.postings(null, PostingsEnum.FREQS);
            if (postings != null) {
                postingsEnums.add(postings);
                weights.add(entry.getValue());
            }
        }

        if (postingsEnums.isEmpty()) {
            return null; // No matching terms in this segment
        }

        // Create our custom DocIdSetIterator that filters by cluster ID
        PartitionedDocIdSetIterator iterator = new PartitionedDocIdSetIterator(postingsEnums, clusterIdValues, query.getClusterIds());

        // Return a custom scorer that uses our iterator
        return new PartitionedSparseScorer(this, iterator, weights);
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
        return true;
    }
}
