/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.algorithm;

import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.lucene.util.BytesRef;
import org.opensearch.neuralsearch.sparse.common.DocFreq;
import org.opensearch.neuralsearch.sparse.common.SparseVector;
import org.opensearch.neuralsearch.sparse.common.SparseVectorReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

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
    public List<DocumentCluster> cluster(List<DocFreq> docFreqs, BytesRef term) throws IOException {
        if (beta == 1) {
            DocumentCluster cluster = new DocumentCluster(null, docFreqs, true);
            return List.of(cluster);
        }
        int size = docFreqs.size();
        docFreqs.sort((o2, o1) -> ByteQuantizer.compareUnsignedByte(o1.getFreq(), o2.getFreq()));
        // generate beta unique random centers
        int num_cluster = (int) Math.ceil((double) (size * beta) / lambda);
        int[] centers = IntStream.range(0, num_cluster).toArray();
        List<List<DocFreq>> docAssignments = new ArrayList<>(num_cluster);
        List<SparseVector> sparseVectors = new ArrayList<>();
        for (int i = 0; i < num_cluster; i++) {
            docAssignments.add(new ArrayList<>());
            SparseVector center = reader.read(docFreqs.get(centers[i]).getDocID());
            sparseVectors.add(center);
        }

        for (DocFreq docFreq : docFreqs) {
            int centerIdx = 0;
            float maxScore = Float.MIN_VALUE;
            SparseVector docVector = reader.read(docFreq.getDocID());
            if (docVector == null) {
                continue;
            }
            byte[] denseDocVector = docVector.toDenseVector();
            for (int i = 0; i < num_cluster; i++) {
                float score = Float.MIN_VALUE;
                SparseVector center = sparseVectors.get(i);
                if (center != null) {
                    score = center.dotProduct(denseDocVector);
                }
                if (score > maxScore) {
                    maxScore = score;
                    centerIdx = i;
                }
            }
            docAssignments.get(centerIdx).add(docFreq);
        }
        List<DocumentCluster> clusters = new ArrayList<>();
        for (int i = 0; i < num_cluster; ++i) {
            if (docAssignments.get(i).isEmpty()) continue;
            DocumentCluster cluster = new DocumentCluster(null, docAssignments.get(i), false);
            PostingsProcessor.summarize(cluster, this.reader, this.alpha);
            if (term.bytesEquals(new BytesRef("13723"))) {
                log.info("{}th cluster on term 13723", i);
                cluster.show();
                List<DocFreq> docAssignment = docAssignments.get(i);
                int idx = 0;
                for (DocFreq docFreq : docAssignment) {
                    SparseVector docVector = reader.read(docFreq.getDocID());
                    log.info("doc {}th vector on term 13723", idx++);
                    docVector.show();
                }
            }
            clusters.add(cluster);
        }
        return clusters;
    }
}
