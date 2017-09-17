package io.dgraph.entity;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.time.Duration;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import com.google.protobuf.ByteString;

import io.dgraph.entity.CommonConstants.TypeID;
import io.dgraph.exception.DGraphException;
import io.dgraph.proto.Value;

public class Conversions {

    private static ZonedDateTime startTime;

    static {
        startTime = ZonedDateTime.of(1, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
    }

    public static Value ObjectValue(TypeID type, Object obj) {

        ByteString b;
        switch (type) {
            case StringID:
                if (obj instanceof String) {
                    return Value.newBuilder().setStrVal((String) obj).build();
                } else {
                    throw new DGraphException("Invalid Conversion");
                }
            case DefaultId:
                if (obj instanceof String) {
                    return Value.newBuilder().setDefaultVal((String) obj).build();
                } else {
                    throw new DGraphException("Invalid Conversion");
                }
            case IntID:
                if (obj instanceof Long) {
                    return Value.newBuilder().setIntVal((Long) obj).build();
                } else {
                    throw new DGraphException("Invalid Conversion");
                }
            case FloatID:
                if (obj instanceof Float) {
                    return Value.newBuilder().setDoubleVal((Float) obj).build();
                } else {
                    throw new DGraphException("Invalid Conversion");
                }
            case BoolID:
                if (obj instanceof Boolean) {
                    return Value.newBuilder().setBoolVal((Boolean) obj).build();
                } else {
                    throw new DGraphException("Invalid Conversion");
                }
            case PasswordId:
                if (obj instanceof String) {
                    return Value.newBuilder().setStrVal((String) obj).build();
                } else {
                    throw new DGraphException("Invalid Conversion");
                }
            case DateTimeID:
                b = toBinary(obj, type);
                return Value.newBuilder().setDatetimeVal(b).build();
            case BinaryID:
                if (obj instanceof ByteString) {
                    return Value.newBuilder().setBytesVal((ByteString) obj).build();
                } else {
                    throw new DGraphException("Invalid Conversion");
                }
            default:
                throw new DGraphException("Invalid Conversion due to missing conversion logic");

        }

        // return null;
    }

    private static ByteString toBinary(Object obj, TypeID type) {

        if (type.getTypeVal() == TypeID.DateTimeID.getTypeVal()) {
            byte[] byteArr = marshallToBinary((ZonedDateTime) obj);
            return ByteString.copyFrom(byteArr);
        }
        return null;

    }

    // Picked custom encoding logic from
    // https://golang.org/src/time/time.go?s=34661:34710#L1144
    private static byte[] marshallToBinary(ZonedDateTime date) {

        Duration d = Duration.between(startTime, date);
        long sec = d.getSeconds();

        int nano = date.getNano();
        ZoneOffset offset = date.getOffset();
        // Get offset WRT UTC

        int offsetMin = (offset.getTotalSeconds() / 60);

        ByteArrayOutputStream out = new ByteArrayOutputStream();

        out.write(CommonConstants.timeBinaryVersion);
        out.write((byte) (sec >> 56));
        out.write((byte) (sec >> 48));
        out.write((byte) (sec >> 40));
        out.write((byte) (sec >> 32));
        out.write((byte) (sec >> 24));
        out.write((byte) (sec >> 16));
        out.write((byte) (sec >> 8));
        out.write((byte) (sec));
        out.write((byte) (nano >> 24));
        out.write((byte) (nano >> 16));
        out.write((byte) (nano >> 8));
        out.write((byte) (nano));
        out.write((byte) (offsetMin >> 8));
        out.write((byte) (offsetMin));

        return out.toByteArray();
    }

    public static byte[] GetBytes(Object yourObject) throws IOException {

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = null;
        try {
            out = new ObjectOutputStream(bos);
            out.writeObject(yourObject);
            out.flush();
            byte[] yourBytes = bos.toByteArray();
            return yourBytes;
        } finally {
            bos.close();

        }
    }

}
