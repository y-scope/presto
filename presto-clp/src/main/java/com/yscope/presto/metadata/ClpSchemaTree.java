package com.yscope.presto.metadata;

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.PrestoException;
import com.yscope.presto.ClpColumnHandle;
import com.yscope.presto.ClpErrorCode;
import com.yscope.presto.ClpExpression;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class ClpSchemaTree {
    static class ClpNode {
        Type type; // Only non-null for leaf nodes
        Map<String, ClpNode> children = new HashMap<>();
        Set<String> conflictingBaseNames = new HashSet<>();

        boolean isLeaf() {
            return children.isEmpty();
        }
    }

    private final ClpNode root;
    private final boolean polymorphicTypeEnabled;
    ClpSchemaTree(boolean polymorphicTypeEnabled)
    {
        this.polymorphicTypeEnabled = polymorphicTypeEnabled;
        this.root = new ClpNode();
    }

    private Type mapColumnType(byte type)
    {
        switch (ClpNodeType.fromType(type)) {
            case Integer:
                return BigintType.BIGINT;
            case Float:
                return DoubleType.DOUBLE;
            case ClpString:
            case VarString:
            case DateString:
            case NullValue:
                return VarcharType.VARCHAR;
            case UnstructuredArray:
                return new ArrayType(VarcharType.VARCHAR);
            case Boolean:
                return BooleanType.BOOLEAN;
            default:
                throw new PrestoException(ClpErrorCode.CLP_UNSUPPORTED_TYPE, "Unsupported type: " + type);
        }
    }

    public void addColumn(String fullName, byte type) {
        Type prestoType = mapColumnType(type);
        String[] path = fullName.split("\\.");
        ClpNode current = root;

        for (int i = 0; i < path.length - 1; i++) {
            String segment = path[i];
            current.children.putIfAbsent(segment, new ClpNode());
            current = current.children.get(segment);
        }

        String leafName = path[path.length - 1];
        String finalLeafName = leafName;

        if (polymorphicTypeEnabled) {
            boolean conflictDetected = false;

            if (current.children.containsKey(leafName)) {
                ClpNode existing = current.children.get(leafName);

                if (existing.type != null && !existing.type.equals(prestoType)) {
                    String existingSuffix = existing.type.getDisplayName().toLowerCase();
                    String renamedExisting = leafName + "_" + existingSuffix;

                    current.children.remove(leafName);
                    current.children.put(renamedExisting, existing);

                    current.conflictingBaseNames.add(leafName);
                    conflictDetected = true;
                }
            } else if (current.conflictingBaseNames.contains(leafName)) {
                conflictDetected = true;
            }

            if (conflictDetected) {
                String newSuffix = prestoType.getDisplayName().toLowerCase();
                finalLeafName = leafName + "_" + newSuffix;
            }
        }

        ClpNode leaf = new ClpNode();
        leaf.type = prestoType;
        current.children.put(finalLeafName, leaf);
    }

    public List<ClpColumnHandle> collectColumnHandles() {
        List<ClpColumnHandle> columns = new ArrayList<>();
        for (Map.Entry<String, ClpNode> entry : root.children.entrySet()) {
            String name = entry.getKey();
            ClpNode child = entry.getValue();
            if (child.isLeaf()) {
                columns.add(new ClpColumnHandle(name, child.type, true));
            } else {
                Type rowType = buildRowType(child);
                columns.add(new ClpColumnHandle(name, rowType, true));
            }
        }
        return columns;
    }

    private Type buildRowType(ClpNode node) {
        List<RowType.Field> fields = new ArrayList<>();
        for (Map.Entry<String, ClpNode> entry : node.children.entrySet()) {
            String name = entry.getKey();
            ClpNode child = entry.getValue();
            Type fieldType = child.isLeaf() ? child.type : buildRowType(child);
            fields.add(new RowType.Field(Optional.of(name), fieldType));
        }
        return RowType.from(fields);
    }
}
