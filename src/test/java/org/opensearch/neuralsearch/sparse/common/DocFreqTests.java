/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.common;

import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;

public class DocFreqTests extends AbstractSparseTestBase {

    public void testConstructorAndGetters() {
        int docID = 42;
        byte freq = 5;
        DocFreq docFreq = new DocFreq(docID, freq);

        assertEquals(docID, docFreq.getDocID());
        assertEquals(freq, docFreq.getFreq());
    }

    public void testGetIntFreq() {
        // Test positive byte value
        DocFreq docFreq1 = new DocFreq(1, (byte) 5);
        assertEquals(5, docFreq1.getIntFreq());

        // Test byte value that would be negative in signed interpretation
        DocFreq docFreq2 = new DocFreq(2, (byte) 0xFF); // -1 as signed byte, 255 as unsigned
        assertEquals(255, docFreq2.getIntFreq());

        // Test zero value
        DocFreq docFreq3 = new DocFreq(3, (byte) 0);
        assertEquals(0, docFreq3.getIntFreq());

        // Test max positive signed byte value
        DocFreq docFreq4 = new DocFreq(4, (byte) 127);
        assertEquals(127, docFreq4.getIntFreq());
    }

    public void testCompareTo() {
        DocFreq docFreq1 = new DocFreq(1, (byte) 10);
        DocFreq docFreq2 = new DocFreq(2, (byte) 5);
        DocFreq docFreq3 = new DocFreq(1, (byte) 20);

        // Test less than
        assertTrue(docFreq1.compareTo(docFreq2) < 0);

        // Test greater than
        assertTrue(docFreq2.compareTo(docFreq1) > 0);

        // Test equal docIDs (frequency should not affect comparison)
        assertEquals(0, docFreq1.compareTo(docFreq3));
    }

    public void testEqualsAndHashCode() {
        DocFreq docFreq1 = new DocFreq(1, (byte) 10);
        DocFreq docFreq2 = new DocFreq(1, (byte) 10);
        DocFreq docFreq3 = new DocFreq(1, (byte) 20);
        DocFreq docFreq4 = new DocFreq(2, (byte) 10);

        // Test equality with same values
        assertEquals(docFreq1, docFreq2);
        assertEquals(docFreq1.hashCode(), docFreq2.hashCode());

        // Test inequality with different freq
        assertNotEquals(docFreq1, docFreq3);

        // Test inequality with different docID
        assertNotEquals(docFreq1, docFreq4);

        // Test inequality with null
        assertNotEquals(docFreq1, null);

        // Test inequality with different type
        assertNotEquals(docFreq1, "not a DocFreq");
    }

    public void testCompareToEdgeCases() {
        DocFreq minDocFreq = new DocFreq(Integer.MIN_VALUE, (byte) 0);
        DocFreq maxDocFreq = new DocFreq(Integer.MAX_VALUE, (byte) 0);

        // Test min vs max
        assertTrue(minDocFreq.compareTo(maxDocFreq) < 0);

        // Test max vs min
        assertTrue(maxDocFreq.compareTo(minDocFreq) > 0);

        // Test self comparison
        assertEquals(0, minDocFreq.compareTo(minDocFreq));
        assertEquals(0, maxDocFreq.compareTo(maxDocFreq));
    }
}
