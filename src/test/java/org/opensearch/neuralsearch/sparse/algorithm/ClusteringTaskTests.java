/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.algorithm;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.lucene.util.BytesRef;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.common.DocFreq;
import org.opensearch.neuralsearch.sparse.common.InMemoryKey;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.doThrow;

public class ClusteringTaskTests extends AbstractSparseTestBase {

    public void testConstructor_withValidInputs_createsTask() {
        BytesRef term = new BytesRef("test_term");
        List<DocFreq> docs = Arrays.asList(new DocFreq(1, (byte) 1), new DocFreq(2, (byte) 2));
        InMemoryKey.IndexKey key = mock(InMemoryKey.IndexKey.class);
        PostingClustering postingClustering = mock(PostingClustering.class);

        ClusteringTask task = new ClusteringTask(term, docs, key, postingClustering);

        assertNotNull(task);
    }

    public void testConstructor_withEmptyDocs_createsTask() {
        BytesRef term = new BytesRef("test_term");
        List<DocFreq> docs = Collections.emptyList();
        InMemoryKey.IndexKey key = mock(InMemoryKey.IndexKey.class);
        PostingClustering postingClustering = mock(PostingClustering.class);

        ClusteringTask task = new ClusteringTask(term, docs, key, postingClustering);

        assertNotNull(task);
    }

    public void testGet_withValidClustering_returnsPostingClusters() throws IOException {
        BytesRef term = new BytesRef("test_term");
        List<DocFreq> docs = Arrays.asList(new DocFreq(1, (byte) 1), new DocFreq(2, (byte) 2));
        InMemoryKey.IndexKey key = mock(InMemoryKey.IndexKey.class);
        PostingClustering postingClustering = mock(PostingClustering.class);

        List<DocumentCluster> expectedClusters = Arrays.asList(mock(DocumentCluster.class), mock(DocumentCluster.class));
        when(postingClustering.cluster(any())).thenReturn(expectedClusters);

        ClusteringTask task = new ClusteringTask(term, docs, key, postingClustering);
        PostingClusters result = task.get();

        assertNotNull(result);
        assertNotNull(result.getClusters());
        assertEquals(2, result.getClusters().size());
        verify(postingClustering, times(1)).cluster(any());
    }

    public void testGet_withEmptyDocs_returnsPostingClusters() throws IOException {
        BytesRef term = new BytesRef("test_term");
        List<DocFreq> docs = Collections.emptyList();
        InMemoryKey.IndexKey key = mock(InMemoryKey.IndexKey.class);
        PostingClustering postingClustering = mock(PostingClustering.class);

        List<DocumentCluster> expectedClusters = Collections.emptyList();
        when(postingClustering.cluster(any())).thenReturn(expectedClusters);

        ClusteringTask task = new ClusteringTask(term, docs, key, postingClustering);
        PostingClusters result = task.get();

        assertNotNull(result);
        assertNotNull(result.getClusters());
        assertEquals(0, result.getClusters().size());
        verify(postingClustering, times(1)).cluster(any());
    }

    public void testGet_withIOException_throwsRuntimeException() throws IOException {
        BytesRef term = new BytesRef("test_term");
        List<DocFreq> docs = Arrays.asList(new DocFreq(1, (byte) 1));
        InMemoryKey.IndexKey key = mock(InMemoryKey.IndexKey.class);
        PostingClustering postingClustering = mock(PostingClustering.class);

        doThrow(new IOException("Test exception")).when(postingClustering).cluster(any());

        ClusteringTask task = new ClusteringTask(term, docs, key, postingClustering);

        try {
            task.get();
            fail("Expected RuntimeException to be thrown");
        } catch (RuntimeException e) {
            assertTrue(e.getCause() instanceof IOException);
            assertEquals("Test exception", e.getCause().getMessage());
        }
    }

    public void testGet_callsClusteringWithCorrectDocs() throws IOException {
        BytesRef term = new BytesRef("test_term");
        List<DocFreq> docs = Arrays.asList(new DocFreq(1, (byte) 1), new DocFreq(2, (byte) 2), new DocFreq(3, (byte) 3));
        InMemoryKey.IndexKey key = mock(InMemoryKey.IndexKey.class);
        PostingClustering postingClustering = mock(PostingClustering.class);

        List<DocumentCluster> expectedClusters = Arrays.asList(mock(DocumentCluster.class));
        when(postingClustering.cluster(any())).thenReturn(expectedClusters);

        ClusteringTask task = new ClusteringTask(term, docs, key, postingClustering);
        task.get();

        verify(postingClustering, times(1)).cluster(docs);
    }

    public void testGet_withSingleDoc_returnsPostingClusters() throws IOException {
        BytesRef term = new BytesRef("single_term");
        List<DocFreq> docs = Arrays.asList(new DocFreq(42, (byte) 5));
        InMemoryKey.IndexKey key = mock(InMemoryKey.IndexKey.class);
        PostingClustering postingClustering = mock(PostingClustering.class);

        List<DocumentCluster> expectedClusters = Arrays.asList(mock(DocumentCluster.class));
        when(postingClustering.cluster(any())).thenReturn(expectedClusters);

        ClusteringTask task = new ClusteringTask(term, docs, key, postingClustering);
        PostingClusters result = task.get();

        assertNotNull(result);
        assertNotNull(result.getClusters());
        assertEquals(1, result.getClusters().size());
        verify(postingClustering, times(1)).cluster(docs);
    }

    public void testGet_preservesOriginalTermBytes() throws IOException {
        BytesRef originalTerm = new BytesRef("original_term");
        List<DocFreq> docs = Arrays.asList(new DocFreq(1, (byte) 1));
        InMemoryKey.IndexKey key = mock(InMemoryKey.IndexKey.class);
        PostingClustering postingClustering = mock(PostingClustering.class);

        List<DocumentCluster> expectedClusters = Arrays.asList(mock(DocumentCluster.class));
        when(postingClustering.cluster(any())).thenReturn(expectedClusters);

        ClusteringTask task = new ClusteringTask(originalTerm, docs, key, postingClustering);

        // Modify original term to ensure deep copy was made
        originalTerm.bytes[0] = (byte) 'X';

        PostingClusters result = task.get();

        assertNotNull(result);
        assertNotNull(result.getClusters());
        assertEquals(1, result.getClusters().size());
    }
}
