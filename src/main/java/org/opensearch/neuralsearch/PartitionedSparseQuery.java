/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch;

public class PartitionedSparseQuery extends Query {
    private final String fieldName;
    private final Map<String, Float> queryTokens;
    private final List<Integer> clusterIds;

    public PartitionedSparseQuery(String fieldName, Map<String, Float> queryTokens, List<Integer> clusterIds) {
        this.fieldName = fieldName;
        this.queryTokens = queryTokens;
        this.clusterIds = clusterIds;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return new PartitionedSparseWeight(this, searcher, scoreMode, boost);
    }

    @Override
    public String toString(String field) {
        return "PartitionedSparseQuery(field=" + fieldName + ", clusters=" + clusterIds + ")";
    }

    @Override
    public boolean equals(Object obj) {
        // Implement equals method
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        PartitionedSparseQuery that = (PartitionedSparseQuery) obj;
        return Objects.equals(fieldName, that.fieldName)
            && Objects.equals(queryTokens, that.queryTokens)
            && Objects.equals(clusterIds, that.clusterIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldName, queryTokens, clusterIds);
    }

    public String getFieldName() {
        return fieldName;
    }

    public Map<String, Float> getQueryTokens() {
        return queryTokens;
    }

    public List<Integer> getClusterIds() {
        return clusterIds;
    }
}
