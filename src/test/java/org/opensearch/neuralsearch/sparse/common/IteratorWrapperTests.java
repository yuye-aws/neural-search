/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.common;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;

public class IteratorWrapperTests extends AbstractSparseTestBase {

    public void testIteratorWrapperWithNullIterator() {
        IteratorWrapper<String> wrapper = new IteratorWrapper<>(null);
        expectThrows(NullPointerException.class, wrapper::hasNext);
    }

    public void testNextWhenHasNext() {
        Iterator<Integer> iterator = Arrays.asList(1, 2, 3).iterator();
        IteratorWrapper<Integer> wrapper = new IteratorWrapper<>(iterator);

        Integer result = wrapper.next();

        assertEquals(Integer.valueOf(1), result);
        assertEquals(Integer.valueOf(1), wrapper.getCurrent());
    }

    public void testNextWhenNoMoreElements() {
        Iterator<String> emptyIterator = Collections.emptyIterator();
        IteratorWrapper<String> wrapper = new IteratorWrapper<>(emptyIterator);

        assertNull(wrapper.next());
    }

    public void testNextWithEmptyIterator() {
        Iterator<String> emptyIterator = Collections.emptyIterator();
        IteratorWrapper<String> wrapper = new IteratorWrapper<>(emptyIterator);

        assertNull(wrapper.next());
    }

    public void testIteratorWrapperInitialization() {
        Iterator<Integer> iterator = Arrays.asList(1, 2, 3).iterator();
        IteratorWrapper<Integer> wrapper = new IteratorWrapper<>(iterator);

        assertNotNull(wrapper);
        assertNull(wrapper.getCurrent());
        assertTrue(wrapper.hasNext());
    }

    public void testHasNextReturnsTrueWhenUnderlyingIteratorHasMoreElements() {
        Iterator<Integer> mockIterator = Arrays.asList(1, 2, 3).iterator();
        IteratorWrapper<Integer> wrapper = new IteratorWrapper<>(mockIterator);

        assertTrue(wrapper.hasNext());
    }

}
