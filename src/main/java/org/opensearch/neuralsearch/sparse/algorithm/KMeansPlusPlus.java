/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.algorithm;

import lombok.AllArgsConstructor;
import org.opensearch.neuralsearch.sparse.common.DocFreq;
import org.opensearch.neuralsearch.sparse.common.SparseVector;
import org.opensearch.neuralsearch.sparse.common.SparseVectorReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.opensearch.neuralsearch.sparse.algorithm.PostingClustering.MINIMAL_DOC_SIZE_OF_CLUSTER;

/**
 * KMeans++ clustering algorithm
 */
@AllArgsConstructor
public class KMeansPlusPlus implements Clustering {
    private final float alpha;
    private final int beta;
    private final SparseVectorReader reader;

    /**
     * Assigns a document to the best cluster based on similarity.
     *
     * @param docFreq The document frequency object to assign
     * @param reader The document vector reader
     * @param denseCentroids The list of cluster centroids
     * @param clusterIds The list of cluster IDs to consider
     * @return The ID of the best cluster, or -1 if document couldn't be read
     */
    private int assignDocumentToCluster(
        DocFreq docFreq,
        SparseVectorReader reader,
        List<float[]> denseCentroids,
        List<Integer> clusterIds
    ) {
        SparseVector docVector = reader.read(docFreq.getDocID());
        if (docVector == null) {
            return -1;
        }

        int bestCluster = -1;
        float maxScore = Float.MIN_VALUE;

        for (int clusterId : clusterIds) {
            float[] center = denseCentroids.get(clusterId);
            if (center != null) {
                float score = docVector.dotProduct(center);
                if (score > maxScore) {
                    maxScore = score;
                    bestCluster = clusterId;
                }
            }
        }

        return bestCluster;
    }

    @Override
    public List<DocumentCluster> cluster(List<DocFreq> docFreqs) throws IOException {
        if (beta == 1) {
            DocumentCluster cluster = new DocumentCluster(null, docFreqs, true);
            return List.of(cluster);
        }
        int size = docFreqs.size();

        // Ensure at least one cluster
        int num_cluster = Math.min(beta, size);
        num_cluster = Math.max(1, num_cluster);

        // Generate beta unique random centers
        Random random = new Random();
        int[] centers = random.ints(0, size).distinct().limit(num_cluster).toArray();

        // Initialize centroids
        List<List<DocFreq>> docAssignments = new ArrayList<>(num_cluster);
        List<float[]> denseCentroids = new ArrayList<>();
        for (int i = 0; i < num_cluster; i++) {
            docAssignments.add(new ArrayList<>());
            SparseVector center = reader.read(docFreqs.get(centers[i]).getDocID());
            if (center == null) {
                denseCentroids.add(null);
            } else {
                denseCentroids.add(center.toDenseVector());
            }
        }

        // Create a list of all cluster indices
        List<Integer> allClusterIds = new ArrayList<>(num_cluster);
        for (int i = 0; i < num_cluster; i++) {
            allClusterIds.add(i);
        }

        // Assign documents to clusters
        for (DocFreq docFreq : docFreqs) {
            int bestCluster = assignDocumentToCluster(docFreq, reader, denseCentroids, allClusterIds);
            if (bestCluster >= 0) {
                docAssignments.get(bestCluster).add(docFreq);
            }
        }

        // Identify small clusters and collect their documents for reassignment
        List<DocFreq> docsToReassign = new ArrayList<>();
        List<Integer> validClusterIds = new ArrayList<>();
        for (int i = 0; i < num_cluster; i++) {
            if (docAssignments.get(i).size() <= MINIMAL_DOC_SIZE_OF_CLUSTER) {
                // This is a small cluster - collect its documents for reassignment
                docsToReassign.addAll(docAssignments.get(i));
                // Clear this cluster's document list
                docAssignments.get(i).clear();
            } else {
                // This is a valid cluster
                validClusterIds.add(i);
            }
        }

        // If there are documents to reassign and at least one valid cluster
        if (!docsToReassign.isEmpty() && !validClusterIds.isEmpty()) {
            // Reassign documents from small clusters
            for (DocFreq docFreq : docsToReassign) {
                int bestCluster = assignDocumentToCluster(docFreq, reader, denseCentroids, validClusterIds);
                if (bestCluster >= 0) {
                    docAssignments.get(bestCluster).add(docFreq);
                }
            }
        }

        List<DocumentCluster> clusters = new ArrayList<>();
        for (int i = 0; i < num_cluster; ++i) {
            if (docAssignments.get(i).isEmpty()) {
                continue;
            }
            DocumentCluster cluster = new DocumentCluster(null, docAssignments.get(i), false);
            PostingsProcessor.summarize(cluster, this.reader, this.alpha);
            clusters.add(cluster);
        }
        return clusters;
    }
}
