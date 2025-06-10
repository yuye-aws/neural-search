/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.algorithm;

import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.opensearch.neuralsearch.sparse.common.DocFreq;
import org.opensearch.neuralsearch.sparse.common.Profiling;
import org.opensearch.neuralsearch.sparse.common.SparseVector;
import org.opensearch.neuralsearch.sparse.common.SparseVectorReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Random clustering algorithm
 */
@Log4j2
@AllArgsConstructor
public class RandomClustering implements Clustering {

    private final int lambda;
    private final float alpha;
    private final int beta;
    private final SparseVectorReader reader;

    @Override
    public List<DocumentCluster> cluster(List<DocFreq> docFreqs) throws IOException {
        long startRandomCluster = Profiling.INSTANCE.begin(Profiling.ItemId.RANDOMCLUSTER);
        if (beta == 1) {
            DocumentCluster cluster = new DocumentCluster(null, docFreqs, true);
            return List.of(cluster);
        }
        int size = docFreqs.size();
        // generate beta unique random centers
        long startRandomInitialize = Profiling.INSTANCE.begin(Profiling.ItemId.CLUSTERRANDOMINITIALIZE);
        Random random = new Random();
        int num_cluster = (int) Math.ceil((double) (size * beta) / lambda);
        int[] centers = random.ints(0, size).distinct().limit(num_cluster).toArray();
        List<List<DocFreq>> docAssignments = new ArrayList<>(num_cluster);
        List<byte[]> denseCentroids = new ArrayList<>();
        for (int i = 0; i < num_cluster; i++) {
            docAssignments.add(new ArrayList<>());
            SparseVector center = reader.read(docFreqs.get(centers[i]).getDocID());
            if (center == null) {
                denseCentroids.add(null);
            } else {
                long startToDense = Profiling.INSTANCE.begin(Profiling.ItemId.CLUSTERTODENSE);
                denseCentroids.add(center.toDenseVector());
                Profiling.INSTANCE.end(Profiling.ItemId.CLUSTERTODENSE, startToDense);
            }
        }
        Profiling.INSTANCE.end(Profiling.ItemId.CLUSTERRANDOMINITIALIZE, startRandomInitialize);

        long startTotalDP = Profiling.INSTANCE.begin(Profiling.ItemId.CLUSTERTOTALDP);
        for (DocFreq docFreq : docFreqs) {
            int centerIdx = 0;
            float maxScore = Float.MIN_VALUE;
            long startReadSparseVector = Profiling.INSTANCE.begin(Profiling.ItemId.CLUSTERREADSPARSEVECTOR);
            SparseVector docVector = reader.read(docFreq.getDocID());
            Profiling.INSTANCE.end(Profiling.ItemId.CLUSTERREADSPARSEVECTOR, startReadSparseVector);

            if (docVector == null) {
                continue;
            }

            long startDocumentTotalDP = Profiling.INSTANCE.begin(Profiling.ItemId.CLUSTERDOCUMENTTOTALDP);
            for (int i = 0; i < num_cluster; i++) {
                float score = Float.MIN_VALUE;
                long startGetDenseCentroid = Profiling.INSTANCE.begin(Profiling.ItemId.CLUSTERGETDENSECENTROID);
                byte[] center = denseCentroids.get(i);
                Profiling.INSTANCE.end(Profiling.ItemId.CLUSTERGETDENSECENTROID, startGetDenseCentroid);
                if (center != null) {
                    long startDP = Profiling.INSTANCE.begin(Profiling.ItemId.CLUSTERDP);
                    score = docVector.dotProduct(center);
                    Profiling.INSTANCE.end(Profiling.ItemId.CLUSTERDP, startDP);
                } else {
                    log.info("Null Center");
                }
                if (score > maxScore) {
                    long startUpdateCenterIdx = Profiling.INSTANCE.begin(Profiling.ItemId.CLUSTERUPDATECENTERIDX);
                    maxScore = score;
                    centerIdx = i;
                    Profiling.INSTANCE.end(Profiling.ItemId.CLUSTERUPDATECENTERIDX, startUpdateCenterIdx);
                }
            }
            Profiling.INSTANCE.end(Profiling.ItemId.CLUSTERDOCUMENTTOTALDP, startDocumentTotalDP);

            long startUpdateAssignment = Profiling.INSTANCE.begin(Profiling.ItemId.CLUSTERUPDATEASSIGNMENT);
            docAssignments.get(centerIdx).add(docFreq);
            Profiling.INSTANCE.end(Profiling.ItemId.CLUSTERUPDATEASSIGNMENT, startUpdateAssignment);
        }
        Profiling.INSTANCE.end(Profiling.ItemId.CLUSTERTOTALDP, startTotalDP);

        long startTotalSummarize = Profiling.INSTANCE.begin(Profiling.ItemId.CLUSTERTOTALSUMMARIZE);
        List<DocumentCluster> clusters = new ArrayList<>();
        for (int i = 0; i < num_cluster; ++i) {
            if (docAssignments.get(i).isEmpty()) continue;
            DocumentCluster cluster = new DocumentCluster(null, docAssignments.get(i), false);
            long startSummarize = Profiling.INSTANCE.begin(Profiling.ItemId.CLUSTERSUMMARIZE);
            PostingsProcessor.summarize(cluster, this.reader, this.alpha);
            Profiling.INSTANCE.end(Profiling.ItemId.CLUSTERSUMMARIZE, startSummarize);
            clusters.add(cluster);
        }
        Profiling.INSTANCE.end(Profiling.ItemId.CLUSTERTOTALSUMMARIZE, startTotalSummarize);

        Profiling.INSTANCE.end(Profiling.ItemId.RANDOMCLUSTER, startRandomCluster);
        return clusters;
    }
}
