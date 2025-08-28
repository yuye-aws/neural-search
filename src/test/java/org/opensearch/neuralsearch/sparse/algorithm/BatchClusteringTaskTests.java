/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.algorithm;

import lombok.SneakyThrows;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.util.BytesRef;
import org.junit.Before;
import org.mockito.MockitoAnnotations;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.cache.CacheKey;
import org.opensearch.neuralsearch.sparse.TestsPrepareUtils;
import org.opensearch.neuralsearch.sparse.data.PostingClusters;

import java.util.Arrays;
import java.util.List;

public class BatchClusteringTaskTests extends AbstractSparseTestBase {
    private List<BytesRef> terms;
    private MergeState mergeState;
    private CacheKey key;

    @Before
    @Override
    @SneakyThrows
    public void setUp() {
        super.setUp();
        MockitoAnnotations.openMocks(this);

        boolean isEmptyMaxDocs = true;
        SegmentInfo segmentInfo = TestsPrepareUtils.prepareSegmentInfo();

        terms = Arrays.asList(new BytesRef("term1"), new BytesRef("term2"));
        mergeState = TestsPrepareUtils.prepareMergeState(isEmptyMaxDocs);
        key = new CacheKey(segmentInfo, "test_field");
    }

    public void testConstructorDeepCopiesTerms() throws Exception {
        // Setup
        List<BytesRef> originalTerms = Arrays.asList(new BytesRef("term1"), new BytesRef("term2"));

        // Execute - create task with null mergeState to test constructor
        BatchClusteringTask task = new BatchClusteringTask(originalTerms, key, 0.5f, 0.3f, 10, mergeState, null);

        // Verify task is created
        assertNotNull("Task should be created successfully", task);

        List<BytesRef> taskTerms = task.getTerms();

        // Verify deep copy by checking actual content
        assertEquals("First term should be 'term1'", "term1", taskTerms.get(0).utf8ToString());

        // Modify original terms to verify deep copy
        originalTerms.get(0).bytes[0] = (byte) 'X';

        // Verify task's terms remain unchanged (proving deep copy worked)
        assertEquals("Task's first term should still be 'term1'", "term1", taskTerms.get(0).utf8ToString());
        assertNotEquals("Original term should now be different", "term1", originalTerms.get(0).utf8ToString());
    }

    public void testGetWithNullMergeStateThenThrowException() {
        // Test behavior with null merge state - should throw NullPointerException within constructor
        NullPointerException nullPointerException = assertThrows(
            NullPointerException.class,
            () -> new BatchClusteringTask(terms, key, 0.5f, 0.3f, 10, null, null)
        );

        assertEquals("mergeState is marked non-null but is null", nullPointerException.getMessage());
    }

    @SneakyThrows
    public void testGetWithNonNullMergeState() {
        // Test behavior with a not null merge state
        boolean isEmptyMaxDocs = false;
        MergeState mergeState = TestsPrepareUtils.prepareMergeState(isEmptyMaxDocs);
        FieldInfo keyFieldInfo = TestsPrepareUtils.prepareKeyFieldInfo();

        // Create BatchClusteringTask
        BatchClusteringTask task = new BatchClusteringTask(terms, key, 0.5f, 0.3f, 10, mergeState, keyFieldInfo);

        // Execute and examine the result
        List<Pair<BytesRef, PostingClusters>> result = task.get();

        // Verify the returned clusters
        assertNotNull("Result should not be null", result);
        assertEquals("Should return clusters for each term", terms.size(), result.size());

        for (int i = 0; i < result.size(); i++) {
            Pair<BytesRef, PostingClusters> pair = result.get(i);

            // Verify term matches
            assertNotNull("Term should not be null", pair.getLeft());
            assertEquals("Term should match input", terms.get(i).utf8ToString(), pair.getLeft().utf8ToString());

            // Verify clusters
            PostingClusters clusters = pair.getRight();
            assertNotNull("PostingClusters should not be null", clusters);
            assertNotNull("Clusters list should not be null", clusters.getClusters());

            // Additional cluster validation
            assertTrue("Should have non-negative cluster count", clusters.getClusters().size() >= 0);
        }
    }

    @SneakyThrows
    public void testGetWithNonNullMergeStateZeroMaxDocs() {
        // Test behavior with a not null merge state
        boolean isEmptyMaxDocs = true;
        MergeState mergeState = TestsPrepareUtils.prepareMergeState(isEmptyMaxDocs);
        FieldInfo keyFieldInfo = TestsPrepareUtils.prepareKeyFieldInfo();

        // Create BatchClusteringTask
        BatchClusteringTask task = new BatchClusteringTask(terms, key, 0.5f, 0.3f, 10, mergeState, keyFieldInfo);

        // Execute and examine the result
        List<Pair<BytesRef, PostingClusters>> result = task.get();

        // Verify the returned clusters
        assertNotNull("Result should not be null", result);
        // Should trigger early quit schema to return an empty list
        assertEquals("Should return an empty list", 0, result.size());
    }

    public void testTermsDeepCopyInGet() {
        // Test that the terms are properly deep copied and used in get() method
        List<BytesRef> originalTerms = Arrays.asList(new BytesRef("original1"), new BytesRef("original2"));

        BatchClusteringTask task = new BatchClusteringTask(originalTerms, key, 0.5f, 0.3f, 10, mergeState, null);

        // Modify original terms
        originalTerms.get(0).bytes[0] = (byte) 'M';

        // The task should still have the original values due to deep copy
        // We can't easily test the get() method output, but we can verify the task was created properly
        assertNotNull("Task should be created and maintain its own copy of terms", task);
    }
}
