/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.junit.Before;
import org.mockito.MockitoAnnotations;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import org.opensearch.neuralsearch.sparse.algorithm.BatchClusteringTaskTests;

import static org.opensearch.neuralsearch.sparse.SparseTokensField.SPARSE_FIELD;

public class SparseTokensFieldTests extends AbstractSparseTestBase {

    private String fieldName;
    private byte[] testValue;
    private IndexableFieldType mockType;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);

        fieldName = "testField";
        testValue = new byte[] { 1, 2, 3 };
        mockType = prepareMockIndexableFieldType();
    }

    public static IndexableFieldType prepareMockIndexableFieldType() {
        return new IndexableFieldType() {
            @Override
            public boolean stored() {
                return false;
            }

            @Override
            public boolean tokenized() {
                return false;
            }

            @Override
            public boolean storeTermVectors() {
                return false;
            }

            @Override
            public boolean storeTermVectorOffsets() {
                return false;
            }

            @Override
            public boolean storeTermVectorPositions() {
                return false;
            }

            @Override
            public boolean storeTermVectorPayloads() {
                return false;
            }

            @Override
            public boolean omitNorms() {
                return false;
            }

            @Override
            public IndexOptions indexOptions() {
                return IndexOptions.DOCS_AND_FREQS;
            }

            @Override
            public DocValuesType docValuesType() {
                return DocValuesType.NUMERIC;
            }

            @Override
            public DocValuesSkipIndexType docValuesSkipIndexType() {
                return DocValuesSkipIndexType.NONE;
            }

            @Override
            public Map<String, String> getAttributes() {
                return new HashMap<>();
            }

            @Override
            public int pointDimensionCount() {
                return 0;
            }

            @Override
            public int pointIndexDimensionCount() {
                return 0;
            }

            @Override
            public int pointNumBytes() {
                return 0;
            }

            @Override
            public int vectorDimension() {
                return 0;
            }

            @Override
            public VectorEncoding vectorEncoding() {
                return VectorEncoding.FLOAT32;
            }

            @Override
            public VectorSimilarityFunction vectorSimilarityFunction() {
                return VectorSimilarityFunction.EUCLIDEAN;
            }
        };
    }

    public void testSparseTokensFieldConstructor() {
        SparseTokensField field = new SparseTokensField(fieldName, testValue, mockType);

        assertNotNull("Field should be created successfully", field);
        assertEquals("Field name should match", fieldName, field.name());
        assertArrayEquals("Binary value should match", testValue, field.binaryValue().bytes);
        assertEquals("Field type should match", mockType, field.fieldType());
    }

    public void testIsSparseFieldReturnsFalseWhenFieldIsNull() {
        FieldInfo field = null;
        assertFalse("Should return false for null field", SparseTokensField.isSparseField(field));
    }

    public void testIsSparseFieldWhenFieldContainsSparseAttribute() throws Exception {
        FieldInfo mockField = BatchClusteringTaskTests.prepareKeyFieldInfo();
        Map<String, String> attributes = new HashMap<>();
        attributes.put(SPARSE_FIELD, "true");

        Field attributesField = FieldInfo.class.getDeclaredField("attributes");
        attributesField.setAccessible(true);
        attributesField.set(mockField, attributes);

        boolean result = SparseTokensField.isSparseField(mockField);

        assertTrue("Should return true for field with sparse attribute", result);
    }

    public void testIsSparseFieldWithNullField() {
        assertFalse("Should return false for null field", SparseTokensField.isSparseField(null));
    }

}
