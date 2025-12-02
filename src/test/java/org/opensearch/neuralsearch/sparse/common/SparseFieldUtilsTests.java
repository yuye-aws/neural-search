/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.common;

<<<<<<< HEAD
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
=======
>>>>>>> 6f499f5b (Fix two phase and seismic)
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
<<<<<<< HEAD
import org.opensearch.index.mapper.MapperService;
import org.opensearch.neuralsearch.sparse.TestsPrepareUtils;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
=======
import org.opensearch.neuralsearch.util.NeuralSearchClusterUtil;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
>>>>>>> 6f499f5b (Fix two phase and seismic)
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SparseFieldUtilsTests extends OpenSearchTestCase {

    private static final String TEST_INDEX_NAME = "test_index";
    private static final String TEST_SPARSE_FIELD_NAME = "test_sparse_field";
<<<<<<< HEAD
    private static final String TEST_PARENT_FIELD_NAME = "test_parent_field";

    @Mock
    private IndexMetadata indexMetadata;
    @Mock
    private ClusterService clusterService;
    @Mock
    private Metadata metadata;
    @Mock
    private ClusterState clusterState;
=======

    private IndexMetadata indexMetadata;
    private ClusterService clusterService;
>>>>>>> 6f499f5b (Fix two phase and seismic)

    @Override
    public void setUp() throws Exception {
        super.setUp();
<<<<<<< HEAD
        MockitoAnnotations.openMocks(this);
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.index(anyString())).thenReturn(indexMetadata);
    }

    public void testGetSparseAnnFields_whenNullSparseIndex_thenReturnEmptySet() {
        assertEquals(0, SparseFieldUtils.getSparseAnnFields(null, clusterService).size());
    }

    public void testGetSparseAnnFields_whenNullIndexMetadata_thenReturnEmptySet() {
        configureSparseIndexSetting(true);
        when(metadata.index(anyString())).thenReturn(null);
        assertEquals(0, SparseFieldUtils.getSparseAnnFields(TEST_INDEX_NAME, clusterService).size());
=======
        clusterService = mock(ClusterService.class);
        NeuralSearchClusterUtil.instance().initialize(clusterService);
    }

    public void testGetSparseAnnFields_whenNullSparseIndex_thenReturnEmptySet() {
        assertEquals(0, SparseFieldUtils.getSparseAnnFields(null).size());
>>>>>>> 6f499f5b (Fix two phase and seismic)
    }

    public void testGetSparseAnnFields_whenNonSparseIndex_thenReturnEmptySet() {
        // Setup mock cluster service with non-sparse index
        configureSparseIndexSetting(false);

<<<<<<< HEAD
        assertEquals(0, SparseFieldUtils.getSparseAnnFields(TEST_INDEX_NAME, clusterService).size());
=======
        assertEquals(0, SparseFieldUtils.getSparseAnnFields(TEST_INDEX_NAME).size());
>>>>>>> 6f499f5b (Fix two phase and seismic)
    }

    public void testGetSparseAnnFields_whenNullMappingMetaData_thenReturnEmptySet() {
        // Setup mock cluster service with null mapping metadata
<<<<<<< HEAD
        configureSparseIndexSetting(true);
        when(indexMetadata.mapping()).thenReturn(null);

        assertEquals(0, SparseFieldUtils.getSparseAnnFields(TEST_INDEX_NAME, clusterService).size());
    }

    public void testGetSparseAnnFields_whenNullSourceAsMap_thenReturnEmptySet() {
        // Setup mock cluster service with null mapping metadata
        configureSparseIndexSetting(true);
        MappingMetadata mappingMetadata = mock(MappingMetadata.class);
        when(indexMetadata.mapping()).thenReturn(mappingMetadata);
        when(mappingMetadata.sourceAsMap()).thenReturn(null);

        assertEquals(0, SparseFieldUtils.getSparseAnnFields(TEST_INDEX_NAME, clusterService).size());
=======
        configureIndexMapping(null);

        assertEquals(0, SparseFieldUtils.getSparseAnnFields(TEST_INDEX_NAME).size());
>>>>>>> 6f499f5b (Fix two phase and seismic)
    }

    public void testGetSparseAnnFields_whenEmptyProperties_thenReturnEmptySet() {
        // Setup mock cluster service with empty properties
        configureIndexMappingProperties(Map.of());

<<<<<<< HEAD
        assertEquals(0, SparseFieldUtils.getSparseAnnFields(TEST_INDEX_NAME, clusterService).size());
=======
        assertEquals(0, SparseFieldUtils.getSparseAnnFields(TEST_INDEX_NAME).size());
>>>>>>> 6f499f5b (Fix two phase and seismic)
    }

    public void testGetSparseAnnFields_whenNonSeismicField_thenReturnEmptySet() {
        // Setup mock cluster service with non-seismic field
<<<<<<< HEAD
        Map<String, Object> properties = TestsPrepareUtils.createFieldMappingProperties(
            false,
            Collections.singletonList(TEST_SPARSE_FIELD_NAME)
        );
        configureIndexMappingProperties(properties);

        assertEquals(0, SparseFieldUtils.getSparseAnnFields(TEST_INDEX_NAME, clusterService).size());
=======
        Map<String, Object> properties = createFieldMappingProperties(false);
        configureIndexMappingProperties(properties);

        assertEquals(0, SparseFieldUtils.getSparseAnnFields(TEST_INDEX_NAME).size());
>>>>>>> 6f499f5b (Fix two phase and seismic)
    }

    public void testGetSparseAnnFields_whenSeismicField_thenReturnField() {
        // Setup mock cluster service with seismic field
<<<<<<< HEAD
        Map<String, Object> properties = TestsPrepareUtils.createFieldMappingProperties(
            true,
            Collections.singletonList(TEST_SPARSE_FIELD_NAME)
        );
        configureIndexMappingProperties(properties);

        assertEquals(Set.of(TEST_SPARSE_FIELD_NAME), SparseFieldUtils.getSparseAnnFields(TEST_INDEX_NAME, clusterService));
    }

    public void testGetSparseAnnFields_whenNestedSeismicField_thenReturnField() {
        // Setup mock cluster service with nested seismic field
        Map<String, Object> properties = createNestedFieldMappingProperties(
            true,
            TEST_PARENT_FIELD_NAME,
            Collections.singletonList(TEST_SPARSE_FIELD_NAME)
        );
        configureIndexMappingProperties(properties);

        assertEquals(
            Set.of(TEST_PARENT_FIELD_NAME + "." + TEST_SPARSE_FIELD_NAME),
            SparseFieldUtils.getSparseAnnFields(TEST_INDEX_NAME, clusterService)
        );
    }

    public void testGetSparseAnnFields_whenNestedSeismicField_andExceedMapDepth_thenThrowException() {
        // Setup mock cluster service with deeply nested seismic field that exceeds maxDepth
        Map<String, Object> properties = createNestedFieldMappingProperties(
            true,
            TEST_PARENT_FIELD_NAME,
            Collections.singletonList(TEST_SPARSE_FIELD_NAME)
        );
        configureIndexMappingProperties(properties);

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            SparseFieldUtils.getSparseAnnFields(TEST_INDEX_NAME, clusterService, 1);
        });

        assertTrue(exception.getMessage().contains("exceeds maximum mapping depth limit"));
    }

    public void testGetMaxDepth_whenNullIndex_thenReturnDefaultDepth() {
        long defaultDepth = MapperService.INDEX_MAPPING_DEPTH_LIMIT_SETTING.getDefault(Settings.EMPTY);

        assertEquals(defaultDepth, SparseFieldUtils.getMaxDepth(null, clusterService));
    }

    public void testGetMaxDepth_whenNullClusterService_thenReturnDefaultDepth() {
        long defaultDepth = MapperService.INDEX_MAPPING_DEPTH_LIMIT_SETTING.getDefault(Settings.EMPTY);

        assertEquals(defaultDepth, SparseFieldUtils.getMaxDepth(TEST_INDEX_NAME, null));
    }

    public void testGetMaxDepth_whenIndexNotFound_thenReturnDefaultDepth() {
        when(metadata.index(TEST_INDEX_NAME)).thenReturn(null);

        long defaultDepth = MapperService.INDEX_MAPPING_DEPTH_LIMIT_SETTING.getDefault(Settings.EMPTY);

        assertEquals(defaultDepth, SparseFieldUtils.getMaxDepth(TEST_INDEX_NAME, clusterService));
    }

    public void testGetMaxDepth_whenCustomDepthConfigured_thenReturnCustomDepth() {
        long customDepth = 50L;
        Settings settings = Settings.builder().put(MapperService.INDEX_MAPPING_DEPTH_LIMIT_SETTING.getKey(), customDepth).build();

        when(indexMetadata.getSettings()).thenReturn(settings);

        assertEquals(customDepth, SparseFieldUtils.getMaxDepth(TEST_INDEX_NAME, clusterService));
    }

    public void testGetMaxDepth_whenNoDepthConfigured_thenReturnDefaultDepth() {
        Settings settings = Settings.builder().build();
        when(indexMetadata.getSettings()).thenReturn(settings);

        long defaultDepth = MapperService.INDEX_MAPPING_DEPTH_LIMIT_SETTING.getDefault(Settings.EMPTY);

        assertEquals(defaultDepth, SparseFieldUtils.getMaxDepth(TEST_INDEX_NAME, clusterService));
    }

    private void configureSparseIndexSetting(boolean isSparseIndex) {
=======
        Map<String, Object> properties = createFieldMappingProperties(true);
        configureIndexMappingProperties(properties);

        assertEquals(Set.of(TEST_SPARSE_FIELD_NAME), SparseFieldUtils.getSparseAnnFields(TEST_INDEX_NAME).size());
    }

    private void initializeMockClusterService() {
        Metadata metadata = mock(Metadata.class);
        ClusterState clusterState = mock(ClusterState.class);

        indexMetadata = mock(IndexMetadata.class);

        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.index(anyString())).thenReturn(indexMetadata);
    }

    private void configureSparseIndexSetting(boolean isSparseIndex) {
        initializeMockClusterService();
>>>>>>> 6f499f5b (Fix two phase and seismic)
        Settings settings = Settings.builder().put("index.sparse", isSparseIndex).build();
        when(indexMetadata.getSettings()).thenReturn(settings);
    }

<<<<<<< HEAD
    private void configureIndexMappingProperties(Map<String, Object> properties) {
        MappingMetadata mappingMetadata = new MappingMetadata("_doc", properties);
=======
    private void configureIndexMapping(MappingMetadata mappingMetadata) {
>>>>>>> 6f499f5b (Fix two phase and seismic)
        configureSparseIndexSetting(true);
        when(indexMetadata.mapping()).thenReturn(mappingMetadata);
    }

<<<<<<< HEAD
    private Map<String, Object> createNestedFieldMappingProperties(boolean isSeismicField, String parentField, List<String> sparseFields) {
        Map<String, Object> properties = new HashMap<>();
        Map<String, Object> nestedFieldMapping = new HashMap<>();
        Map<String, Object> sparseFieldMapping = new HashMap<>();
        for (String sparseField : sparseFields) {
            Map<String, Object> sparseFieldProperties = new HashMap<>();
            sparseFieldProperties.put("type", isSeismicField ? "sparse_vector" : "rank_features");

            sparseFieldMapping.put(sparseField, sparseFieldProperties);
        }
        nestedFieldMapping.put("properties", sparseFieldMapping);
        properties.put("properties", Map.of(parentField, nestedFieldMapping));
=======
    private void configureIndexMappingProperties(Map<String, Object> properties) {
        MappingMetadata mappingMetadata = new MappingMetadata("_doc", properties);
        configureIndexMapping(mappingMetadata);
    }

    private Map<String, Object> createFieldMappingProperties(boolean isSeismicField) {
        Map<String, Object> sparseFieldMapping = new HashMap<>();
        Map<String, Object> sparseFieldProperties = new HashMap<>();
        sparseFieldProperties.put("type", isSeismicField ? "sparse_tokens" : "rank_features");
        sparseFieldMapping.put(TEST_SPARSE_FIELD_NAME, sparseFieldProperties);

        Map<String, Object> properties = new HashMap<>();
        properties.put("properties", sparseFieldMapping);
>>>>>>> 6f499f5b (Fix two phase and seismic)
        return properties;
    }
}
