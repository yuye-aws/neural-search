/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import lombok.extern.log4j.Log4j2;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.util.BytesRef;
import org.opensearch.neuralsearch.sparse.common.SparseVector;

import java.io.IOException;

/**
 * DiskSparseVectorForwardIndex reads sparse vectors directly from disk
 */
@Log4j2
public class DiskSparseVectorForwardIndex implements SparseVectorForwardIndex {

    private final LeafReader leafReader;
    private final String fieldName;

    public DiskSparseVectorForwardIndex(LeafReader leafReader, String fieldName) {
        this.leafReader = leafReader;
        this.fieldName = fieldName;
    }

    @Override
    public SparseVectorForwardIndexReader getForwardIndexReader() {
        return new DiskSparseVectorForwardIndexReader();
    }

    @Override
    public SparseVectorForwardIndexWriter getForwardIndexWriter() {
        throw new UnsupportedOperationException("Writing to disk-based forward index is not supported directly");
    }

    private class DiskSparseVectorForwardIndexReader implements SparseVectorForwardIndexReader {
        private BinaryDocValues docValues;

        private DiskSparseVectorForwardIndexReader() {
            try {
                this.docValues = leafReader.getBinaryDocValues(fieldName);
            } catch (IOException e) {
                log.error("Failed to get binary doc values for field {}", fieldName, e);
                this.docValues = null;
            }
        }

        @Override
        public SparseVector readSparseVector(int docId) {
            try {
                if (docValues != null && docValues.advanceExact(docId)) {
                    BytesRef bytesRef = docValues.binaryValue();
                    return new SparseVector(bytesRef);
                }
            } catch (IOException e) {
                log.error("Failed to read sparse vector for docId {}", docId, e);
            }
            return null;
        }

        @Override
        public BytesRef read(int docId) {
            try {
                if (docValues != null && docValues.advanceExact(docId)) {
                    return docValues.binaryValue();
                }
            } catch (IOException e) {
                log.error("Failed to read bytes for docId {}", docId, e);
            }
            return null;
        }
    }
}
