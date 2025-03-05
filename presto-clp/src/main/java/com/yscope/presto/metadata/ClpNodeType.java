package com.yscope.presto.metadata;

public enum ClpNodeType
{
    Integer((byte) 0),
    Float((byte) 1),
    ClpString((byte) 2),
    VarString((byte) 3),
    Boolean((byte) 4),
    Object((byte) 5),
    UnstructuredArray((byte) 6),
    NullValue((byte) 7),
    DateString((byte) 8),
    StructuredArray((byte) 9);

    private final byte type;
    private static final ClpNodeType[] LOOKUP_TABLE;

    static {
        byte maxType = 0;
        for (ClpNodeType nodeType : values()) {
            if (nodeType.type > maxType) {
                maxType = nodeType.type;
            }
        }

        ClpNodeType[] lookup = new ClpNodeType[maxType + 1];
        for (ClpNodeType nodeType : values()) {
            lookup[nodeType.type] = nodeType;
        }

        LOOKUP_TABLE = lookup;
    }

    ClpNodeType(byte type) {
        this.type = type;
    }

    public static ClpNodeType fromType(byte type) {
        if (type < 0 || type >= LOOKUP_TABLE.length || LOOKUP_TABLE[type] == null) {
            throw new IllegalArgumentException("Invalid type code: " + type);
        }
        return LOOKUP_TABLE[type];
    }

    public byte getType()
    {
        return type;
    }
}
