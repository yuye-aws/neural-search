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
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * KMeans++ clustering algorithm
 */
@AllArgsConstructor
public class KMeansPlusPlus implements Clustering {

    private final static int MINIMAL_CLUSTER_DOC_SIZE = 10;

    private final float alpha;
    private final int beta;
    private final SparseVectorReader reader;

    /**
     * Assigns a document to the best cluster based on similarity.
     *
     * @param docVector The document vector
     * @param denseCentroids The list of cluster centroids
     * @param clusterIds The list of cluster IDs to consider
     * @return The ID of the best cluster, or -1 if document couldn't be read
     */
    private int assignDocumentToCluster(SparseVector docVector, List<float[]> denseCentroids, List<Integer> clusterIds) {
        if (docVector == null) {
            return -1;
        }

        int bestCluster = 0;
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

        // Ensure cluster not exceed doc size
        int num_cluster = Math.min(beta, size);

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
        List<Integer> allClusterIds = IntStream.range(0, num_cluster).boxed().collect(Collectors.toList());

        // Assign documents to clusters
        for (DocFreq docFreq : docFreqs) {
            SparseVector docVector = reader.read(docFreq.getDocID());
            int bestCluster = assignDocumentToCluster(docVector, denseCentroids, allClusterIds);
            if (bestCluster >= 0) {
                docAssignments.get(bestCluster).add(docFreq);
            }
        }

        // Identify small clusters and collect their documents for reassignment
        List<DocFreq> docsToReassign = new ArrayList<>();
        List<Integer> validClusterIds = new ArrayList<>();

        // Identify valid clusters
        for (int i = 0; i < num_cluster; i++) {
            if (docAssignments.get(i).size() >= MINIMAL_CLUSTER_DOC_SIZE) {
                validClusterIds.add(i);
            }
        }

        // Only proceed with reassignment if we have valid clusters to reassign to
        if (!validClusterIds.isEmpty()) {
            // Collect documents from small clusters
            for (int i = 0; i < num_cluster; i++) {
                if (docAssignments.get(i).size() < MINIMAL_CLUSTER_DOC_SIZE) {
                    docsToReassign.addAll(docAssignments.get(i));
                    docAssignments.get(i).clear();
                }
            }

            // Reassign documents from small clusters
            for (DocFreq docFreq : docsToReassign) {
                SparseVector docVector = reader.read(docFreq.getDocID());
                int bestCluster = assignDocumentToCluster(docVector, denseCentroids, allClusterIds);
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
