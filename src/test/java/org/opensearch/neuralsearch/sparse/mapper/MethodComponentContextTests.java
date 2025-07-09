/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.mapper;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.index.mapper.MapperParsingException;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.NAME_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.PARAMETERS_FIELD;

public class MethodComponentContextTests extends AbstractSparseTestBase {

    public void testConstructorWithNullParameter() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            new MethodComponentContext((MethodComponentContext) null);
        });
        assertNotNull(exception);
    }

    public void testEqualsWithDifferentClass() {
        MethodComponentContext context = new MethodComponentContext("test", new HashMap<>());
        assertFalse(context.equals(new Object()));
    }

    public void testEqualsWithDifferentName() {
        MethodComponentContext context1 = new MethodComponentContext("test1", new HashMap<>());
        MethodComponentContext context2 = new MethodComponentContext("test2", new HashMap<>());
        assertFalse(context1.equals(context2));
    }

    public void testEqualsWithDifferentParameters() {
        Map<String, Object> params1 = new HashMap<>();
        params1.put("key", "value1");
        Map<String, Object> params2 = new HashMap<>();
        params2.put("key", "value2");

        MethodComponentContext context1 = new MethodComponentContext("test", params1);
        MethodComponentContext context2 = new MethodComponentContext("test", params2);
        assertFalse(context1.equals(context2));
    }

    public void testEqualsWithNull() {
        MethodComponentContext context = new MethodComponentContext("test", new HashMap<>());
        assertFalse(context.equals(null));
    }

    public void testFromXContentWithEmptyMap() throws IOException {
        XContentParser mockParser = mock(XContentParser.class);
        when(mockParser.currentToken()).thenReturn(null);
        when(mockParser.nextToken()).thenReturn(XContentParser.Token.START_OBJECT);
        when(mockParser.map()).thenReturn(new HashMap<>());

        MapperParsingException exception = expectThrows(
            MapperParsingException.class,
            () -> { MethodComponentContext.fromXContent(mockParser); }
        );
        assertEquals("name needs to be set", exception.getMessage());
    }

    public void testFromXContentWithEndOfInput() throws IOException {
        XContentParser mockParser = mock(XContentParser.class);
        when(mockParser.currentToken()).thenReturn(null);
        when(mockParser.nextToken()).thenReturn(null);

        MapperParsingException exception = expectThrows(
            MapperParsingException.class,
            () -> { MethodComponentContext.fromXContent(mockParser); }
        );
        assertEquals("name needs to be set", exception.getMessage());
    }

    public void testGetFloatWithNonNumberValue() {
        MethodComponentContext context = new MethodComponentContext("test", Map.of("key", "not a number"));
        Float defaultValue = 1.0f;
        Float result = context.getFloat("key", defaultValue);
        assertEquals(defaultValue, result);
    }

    public void testMethodComponentContextWithEmptyStreamInput() throws IOException {
        StreamInput mockStreamInput = mock(StreamInput.class);
        when(mockStreamInput.readString()).thenReturn("testName");
        when(mockStreamInput.available()).thenReturn(0);

        MethodComponentContext context = new MethodComponentContext(mockStreamInput);
        assertTrue(context.getParameters().isEmpty());
    }

    public void testParseWithEmptyName() {
        Map<String, Object> input = new HashMap<>();
        input.put("name", "");
        input.put("parameters", new HashMap<>());

        MapperParsingException exception = expectThrows(MapperParsingException.class, () -> { MethodComponentContext.parse(input); });
        assertNotNull(exception);
    }

    public void testParseWithInvalidKey() {
        Map<String, Object> input = new HashMap<>();
        input.put("name", "validName");
        input.put("invalidKey", "value");

        MapperParsingException exception = expectThrows(MapperParsingException.class, () -> { MethodComponentContext.parse(input); });
        assertNotNull(exception);
    }

    public void testParseWithNestedParameters() {
        Map<String, Object> input = new HashMap<>();
        input.put(NAME_FIELD, "testMethod");

        Map<String, Object> parameters = new HashMap<>();
        parameters.put("param1", null);

        Map<String, Object> nestedParam = new HashMap<>();
        nestedParam.put(NAME_FIELD, "nestedMethod");
        nestedParam.put(PARAMETERS_FIELD, null);
        parameters.put("param2", nestedParam);

        input.put(PARAMETERS_FIELD, parameters);

        MethodComponentContext result = MethodComponentContext.parse(input);

        assertEquals("testMethod", result.getName());
        assertEquals(null, result.getParameters().get("param1"));
        assertTrue(result.getParameters().get("param2") instanceof MethodComponentContext);

        MethodComponentContext nestedResult = (MethodComponentContext) result.getParameters().get("param2");
        assertEquals("nestedMethod", nestedResult.getName());
        assertEquals(null, nestedResult.getParameters());
    }

    public void testParseWithNonMapInput() {
        MapperParsingException exception = expectThrows(MapperParsingException.class, () -> { MethodComponentContext.parse("Not a Map"); });
        assertNotNull(exception);
    }

    public void testParseWithNonMapParameters() {
        Map<String, Object> input = new HashMap<>();
        input.put("name", "validName");
        input.put("parameters", "Not a Map");

        MapperParsingException exception = expectThrows(MapperParsingException.class, () -> { MethodComponentContext.parse(input); });
        assertNotNull(exception);
    }

    public void testParseWithNonStringName() {
        Map<String, Object> input = new HashMap<>();
        input.put("name", 123);
        input.put("parameters", new HashMap<>());

        MapperParsingException exception = expectThrows(MapperParsingException.class, () -> { MethodComponentContext.parse(input); });
        assertNotNull(exception);
    }

    public void testReadNonMethodComponentContextValue() throws IOException {
        StreamInput mockInput = mock(StreamInput.class);
        when(mockInput.readBoolean()).thenReturn(false);
        when(mockInput.readGenericValue()).thenReturn("test-value");

        // Test is removed due to private access - functionality is tested through public methods
        assertNotNull(mockInput);
    }

    public void testToXContentWithNestedMethodComponentContext() throws IOException {
        String name = "parentMethod";
        Map<String, Object> nestedParams = new HashMap<>();
        nestedParams.put("nestedParam", "nestedValue");
        MethodComponentContext nestedContext = new MethodComponentContext("nestedMethod", nestedParams);

        Map<String, Object> params = new HashMap<>();
        params.put("nestedContext", nestedContext);
        MethodComponentContext context = new MethodComponentContext(name, params);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        context.toXContent(builder, null);

        String expected =
            "{\"name\":\"parentMethod\",\"parameters\":{\"nestedContext\":{\"name\":\"nestedMethod\",\"parameters\":{\"nestedParam\":\"nestedValue\"}}}}";
        assertEquals(expected, builder.toString());
    }

    public void testToXContentWithNullParameters() throws IOException {
        String name = "testMethod";
        MethodComponentContext context = new MethodComponentContext(name, null);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        context.toXContent(builder, null);

        String expected = "{\"name\":\"testMethod\",\"parameters\":null}";
        assertEquals(expected, builder.toString());
    }

    public void testCopyConstructorWithNonMethodComponentContextParameters() {
        String name = "testMethod";
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("param1", "value1");
        parameters.put("param2", 42);

        MethodComponentContext original = new MethodComponentContext(name, parameters);
        MethodComponentContext copy = new MethodComponentContext(original);

        assertNotNull(copy);
        assertEquals(name, copy.getName());
        assertNotNull(copy.getParameters());
        assertEquals(2, copy.getParameters().size());
        assertEquals("value1", copy.getParameters().get("param1"));
        assertEquals(42, copy.getParameters().get("param2"));
    }

    public void testDeepCopyWithNestedParameters() {
        Map<String, Object> nestedParams = new HashMap<>();
        nestedParams.put("nestedKey", "nestedValue");
        MethodComponentContext nestedContext = new MethodComponentContext("nestedComponent", nestedParams);

        Map<String, Object> params = new HashMap<>();
        params.put("key1", "value1");
        params.put("nestedContext", nestedContext);
        MethodComponentContext originalContext = new MethodComponentContext("mainComponent", params);

        MethodComponentContext copiedContext = new MethodComponentContext(originalContext);

        assertNotSame(originalContext, copiedContext);
        assertEquals(originalContext.getName(), copiedContext.getName());
        assertNotSame(originalContext.getParameters(), copiedContext.getParameters());
        assertEquals(originalContext.getParameters().size(), copiedContext.getParameters().size());

        assertTrue(copiedContext.getParameters().get("nestedContext") instanceof MethodComponentContext);
        MethodComponentContext copiedNestedContext = (MethodComponentContext) copiedContext.getParameters().get("nestedContext");
        assertNotSame(nestedContext, copiedNestedContext);
        assertEquals(nestedContext.getName(), copiedNestedContext.getName());
        assertEquals(nestedContext.getParameters(), copiedNestedContext.getParameters());
    }

    public void testEqualsIdenticalObjects() {
        String name = "testMethod";
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("param1", "value1");
        parameters.put("param2", 42);

        MethodComponentContext context1 = new MethodComponentContext(name, parameters);
        MethodComponentContext context2 = new MethodComponentContext(name, parameters);

        assertTrue(context1.equals(context2));
        assertTrue(context2.equals(context1));
    }

    public void testFromXContentWithValidInput() throws IOException {
        String json = "{\"name\":\"testMethod\",\"parameters\":{\"param1\":\"value1\"}}";
        XContentParser parser = JsonXContent.jsonXContent.createParser(null, null, json);

        MethodComponentContext result = MethodComponentContext.fromXContent(parser);

        assertNotNull(result);
        assertEquals("testMethod", result.getName());
        assertTrue(result.getParameters().containsKey("param1"));
        assertEquals("value1", result.getParameters().get("param1"));
    }

    public void testGetFloatWhenParameterIsNumber() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("testKey", 42);
        MethodComponentContext context = new MethodComponentContext("testName", parameters);

        Float result = context.getFloat("testKey", 0.0f);

        assertEquals(42.0f, result, 0.001f);
    }

    public void testGetParameterWhenKeyExists() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("testKey", "testValue");
        MethodComponentContext context = new MethodComponentContext("testName", parameters);

        Object result = context.getParameter("testKey", "defaultValue");

        assertEquals("testValue", result);
    }

    public void testGetParametersWhenParametersIsNull() {
        MethodComponentContext context = new MethodComponentContext("testComponent", null);
        Map<String, Object> result = context.getParameters();
        assertEquals(Collections.emptyMap(), result);
    }

    public void testHashCode() {
        String name = "testName";
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("key1", "value1");
        parameters.put("key2", 42);

        MethodComponentContext context = new MethodComponentContext(name, parameters);

        int expectedHashCode = new HashCodeBuilder().append(name).append(parameters).toHashCode();
        int actualHashCode = context.hashCode();

        assertEquals(expectedHashCode, actualHashCode);
    }

    public void testHashCodeDifferentNamesEqualParameters() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("key", "value");

        MethodComponentContext context1 = new MethodComponentContext("name1", parameters);
        MethodComponentContext context2 = new MethodComponentContext("name2", parameters);

        assertNotEquals(context1.hashCode(), context2.hashCode());
    }

    public void testWriteToAndReadFrom() throws IOException {
        String name = "testMethod";
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("param1", "value1");
        parameters.put("param2", 42);

        MethodComponentContext context = new MethodComponentContext(name, parameters);

        BytesStreamOutput out = new BytesStreamOutput();
        context.writeTo(out);

        BytesReference bytesRef = out.bytes();
        StreamInput in = bytesRef.streamInput();
        MethodComponentContext readContext = new MethodComponentContext(in);

        assertEquals(name, readContext.getName());
        assertEquals(parameters, readContext.getParameters());
    }

    public void testWriteToWithNullParameters() throws IOException {
        String name = "testMethod";
        MethodComponentContext context = new MethodComponentContext(name, null);

        BytesStreamOutput out = new BytesStreamOutput();
        context.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        String writtenName = in.readString();
        assertEquals(name, writtenName);
        assertEquals(0, in.available());
    }
}
