package io.dgraph.entity;

import java.io.IOException;
import java.nio.ByteOrder;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.Base64;
import java.util.Date;

import org.apache.commons.lang3.StringUtils;

import com.google.protobuf.ByteString;

import io.dgraph.entity.CommonConstants.TypeID;
import io.dgraph.exception.DGraphException;
import io.dgraph.proto.NQuad;
import io.dgraph.proto.Value;
import mil.nga.wkb.geom.Geometry;
import mil.nga.wkb.io.ByteWriter;
import mil.nga.wkb.io.WkbGeometryWriter;

public class Edge {

    NQuad nq;

    Edge(NQuad nq) {
        Edge e = new Edge();
        e.nq = nq;
    }

    public Edge() {
        this.nq = NQuad.newBuilder().build();
    }

    public void setSubject(Node n) {

        if (StringUtils.isNotBlank(n.getVarName())) {
            this.nq = nq.toBuilder().setSubjectVar(n.String()).build();
        } else {
            this.nq = nq.toBuilder().setSubject(n.String()).build();
        }
    }

    public Edge DeletePredicate(String pred) {
        Edge e = new Edge();
        e.nq = e.nq.toBuilder().setSubject(CommonConstants.X_STAR).setPredicate(pred).setObjectValue(Conversions.ObjectValue(TypeID.DefaultId, CommonConstants.X_STAR)).build();
        return e;
    }

    public void validate() {
        if (StringUtils.isBlank(this.nq.getSubject()) && StringUtils.isBlank(this.nq.getSubjectVar())) {
            throw new DGraphException("ErrInvalidSubject");

        }

        if (StringUtils.isBlank(this.nq.getPredicate())) {
            throw new DGraphException("ErrEmptyPredicate");
        }

        if ((nq.getObjectValue() != null) || StringUtils.isNotBlank(nq.getObjectId()) || StringUtils.isNotBlank(nq.getObjectVar())) {
            return;
        }

        throw new DGraphException("ErrNotConnected");

    }

    public void setValueString(String val) {
        if (StringUtils.isNotBlank(nq.getObjectId())) {
            throw new DGraphException("ErrConnected");
        }
        validateStr(val);
        Value v = Conversions.ObjectValue(TypeID.StringID, val);

        nq = nq.toBuilder().setObjectValue(v).setObjectType(TypeID.StringID.getTypeVal()).build();
    }

    public void setValueStringWithLang(String val, String lang) {
        checkConnections();
        validateStr(val);
        Value v = Conversions.ObjectValue(TypeID.StringID, val);

        nq = nq.toBuilder().setObjectValue(v).setObjectType(TypeID.StringID.getTypeVal()).setLang(lang).build();
    }

    private void checkConnections() {
        if (StringUtils.isNotBlank(nq.getObjectId())) {
            throw new DGraphException("ErrConnected");
        }
    }

    private void setData(Object obj, TypeID type) {
        Value v = Conversions.ObjectValue(type, obj);
        nq = nq.toBuilder().setObjectValue(v).setObjectType(type.getTypeVal()).build();

    }

    public void setValueFloat(Float f) {
        checkConnections();
        setData(f, TypeID.FloatID);
    }

    public void setValueBool(Boolean b) {
        checkConnections();
        setData(b, TypeID.BoolID);
    }

    public void SetValuePassword(String pass) {
        checkConnections();
        setData(pass, TypeID.PasswordId);
    }

    public void SetValueInt(Long pass) {
        checkConnections();
        setData(pass, TypeID.IntID);
    }

    public void SetValueDefault(String str) {
        checkConnections();

        Value v = Conversions.ObjectValue(TypeID.DefaultId, str);
        nq = nq.toBuilder().setObjectValue(v).setObjectType(TypeID.StringID.getTypeVal()).build();
    }

    public void addFacet(String key, String value) {
        // nq.getFacetsOrBuilderList().add
        // TODO
    }

    public void SetValueDatetime(ZonedDateTime date) {
        checkConnections();
        setData(date, TypeID.DateTimeID);
    }

    private void validateStr(String s) {
        // TODO
        // func validateStr(val string) error {
        // for idx, c := range val {
        // if c == '"' && (idx == 0 || val[idx-1] != '\\') {
        // return fmt.Errorf(`" must be preceded by a \ in object value`)
        // }
        // }
        // return nil
        // }

    }

    public void SetValueGeoGeometry(Geometry geom) {

        checkConnections();
        ByteWriter writer = new ByteWriter();
        writer.setByteOrder(ByteOrder.LITTLE_ENDIAN);
        byte[] bytes = null;
        try {
            WkbGeometryWriter.writeGeometry(writer, geom);
            bytes = writer.getBytes();
            writer.close();

        } catch (IOException e) {

            new DGraphException("Failed to marshall the given geometry");
        }

        ByteString bs = ByteString.copyFrom(bytes);

        Value v = Value.newBuilder().setGeoVal(bs).build();
        nq = nq.toBuilder().setObjectValue(v).setObjectType(TypeID.GeoID.getTypeVal()).build();

    }
    
    public void SetValueBytes(byte[] byteArr) {
        checkConnections();
        byte[] dst = Base64.getEncoder().encode(byteArr);
        Value v = Conversions.ObjectValue(TypeID.BinaryID, dst);
        
        nq = nq.toBuilder().setObjectValue(v).setObjectType(TypeID.BinaryID.getTypeVal()).build();
    }
    
    
    

    public void SetValueGeoJson(String json) {
        checkConnections();
        
    }
    
    
    public void connectTo(Node conn) {

        if (nq.getObjectType() > 0) {
            throw new DGraphException("ErrConnected");
        }

        if (StringUtils.isNotBlank(conn.getVarName())) {
            nq = nq.toBuilder().setObjectVar(conn.String()).build();
        } else {
            nq = nq.toBuilder().setObjectId(conn.String()).build();
        }

    }

}
