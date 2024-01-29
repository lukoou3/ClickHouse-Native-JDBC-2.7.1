package com.github.housepower.data.type.complex;

import com.github.housepower.data.IDataType;
import com.github.housepower.misc.SQLLexer;
import com.github.housepower.serde.BinaryDeserializer;
import com.github.housepower.serde.BinarySerializer;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Types;

public class DataTypeQuantileTDigestAggregate implements IDataType<byte[], byte[]> {

    public static DataTypeCreator<byte[], byte[]> CREATOR = (lexer, serverContext) -> new DataTypeQuantileTDigestAggregate();

    public DataTypeQuantileTDigestAggregate() {
    }

    @Override
    public String name() {
        return "AggregateFunction(quantileTDigest, Int64)";
    }

    @Override
    public int sqlTypeId() {
        return Types.BINARY;
    }

    @Override
    public byte[] defaultValue() {
        return new byte[]{0};
    }

    @Override
    public Class<byte[]> javaType() {
        return byte[].class;
    }

    @Override
    public Class<byte[]> jdbcJavaType() {
        return byte[].class;
    }

    @Override
    public int getPrecision() {
        return 0;
    }

    @Override
    public int getScale() {
        return 0;
    }

    @Override
    public void serializeBinary(byte[] data, BinarySerializer serializer) throws SQLException, IOException {
        serializer.writeBytes(data);
    }

    /**
     * deserializeBinary will always returns String
     * for getBytes(idx) method, we encode the String again
     */
    @Override
    public byte[] deserializeBinary(BinaryDeserializer deserializer) throws SQLException, IOException {
        return null;
    }

    @Override
    public byte[] deserializeText(SQLLexer lexer) throws SQLException {
        return null;
    }

    @Override
    public String[] getAliases() {
        return new String[]{
                "LONGBLOB",
                "MEDIUMBLOB",
                "TINYBLOB",
                "MEDIUMTEXT",
                "CHAR",
                "VARCHAR",
                "TEXT",
                "TINYTEXT",
                "LONGTEXT",
                "BLOB"};
    }
}
