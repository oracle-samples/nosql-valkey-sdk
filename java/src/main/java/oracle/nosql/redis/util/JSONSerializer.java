/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.redis.util;

import java.io.IOException;
import java.math.BigDecimal;
import com.fasterxml.jackson.core.io.CharTypes;
import oracle.nosql.driver.values.FieldValueEventHandler;
import oracle.nosql.driver.values.TimestampValue;

public class JSONSerializer implements FieldValueEventHandler {

    private static String NULL_VAL = "null";

    private final StringBuilder sb;
    private final String indent;
    private final String newLine;
    private final String space;
    private int depth;

    public JSONSerializer(String indent, String newLine, String space) {
        sb = new StringBuilder();
        this.indent = indent;
        this.newLine = newLine;
        this.space = space;
    }

    private void addIndent() {
        if (indent != null && depth > 0) {
            for(int i = 0; i < depth; i++) {
                sb.append(indent);
            }
        }
    }

    private void complexValFinish() {
        depth--;
        assert depth >= 0;

        int lastIdx = sb.length() - 1;
        if (lastIdx > 0 && sb.charAt(lastIdx) == ',') {
            sb.setLength(lastIdx);
        }

        if (newLine != null) {
            sb.append(newLine);
        }

        addIndent();
    }

    @Override
    public void startMap(int size) {
        sb.append('{');
        depth++;
    }

    @Override
    public void startArray(int size) {
        sb.append('[');
        depth++;
    }

    @Override
    public void endMap(int size) {
        complexValFinish();
        sb.append('}');
    }

    @Override
    public void endArray(int size) {
        complexValFinish();
        sb.append(']');
    }

    @Override
    public boolean startMapField(String key) {
        if (newLine != null) {
            sb.append(newLine);
        }
        addIndent();
        sb.append('"');
        CharTypes.appendQuoted(sb, key);
        sb.append("\":");
        if (space != null) {
            sb.append(space);
        }
        return false;
    }

    @Override
    public boolean startArrayField(int idx) {
        if (newLine != null) {
            sb.append(newLine);
        }
        addIndent();
        return false;
    }

    @Override
    public void endMapField(String key) {
        sb.append(',');
    }

    @Override
    public void endArrayField(int index) {
        sb.append(',');
    }

    @Override
    public void binaryValue(byte[] byteArray) throws IOException {
        this.binaryValue(byteArray, 0, byteArray.length);
    }

    // We should not have BinaryValue in the field that stores Redis JSON
    // value, having this indicates data corruption.
    @Override
    public void binaryValue(byte[] byteArray, int offset, int length)
        throws IOException {
        throw new IOException("Encountered BinaryValue in JSON");
    }

    // Same as for binary, this should not be encountered in Redis JSON data.
    @Override
    public void timestampValue(TimestampValue timestamp) throws IOException {
        throw new IOException("Encountered TimestampValue in JSON");
    }

    @Override
    public void numberValue(BigDecimal value) throws IOException {
        sb.append(value);
    }

    @Override
    public void booleanValue(boolean value) {
        sb.append(value);
    }

    @Override
    public void stringValue(String value) {
        sb.append('"');
        CharTypes.appendQuoted(sb, value);
        sb.append('"');
    }

    @Override
    public void integerValue(int value) {
        sb.append(value);
    }

    @Override
    public void longValue(long value) {
        sb.append(value);
    }

    @Override
    public void doubleValue(double value) throws IOException {
        sb.append(value);
    }

    @Override
    public void jsonNullValue() {
        sb.append(NULL_VAL);
    }


    // We should not really encounter NullValue or EmptyValue, adding just in
    // case.
    @Override
    public void nullValue() {
        sb.append(NULL_VAL);
    }

    @Override
    public void emptyValue() {
        sb.append(NULL_VAL);
    }

    @Override
    public String toString() {
        return sb.toString();
    }
}
