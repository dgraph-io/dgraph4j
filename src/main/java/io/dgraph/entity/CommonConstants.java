package io.dgraph.entity;

public class CommonConstants {

    public static final String OPTYPE_SET = "SET";

    public static final String OPTYPE_DEL = "DEL";
    
    public static final String X_STAR = "*";

    public static final String VAR_PREFIX = "_:";
    
    public static byte timeBinaryVersion = 1;

    public enum TypeID {
        

        BinaryID("binary",1),IntID("int",2), FloatID("float",3), StringID("string",9), 
        BoolID("bool",4), DateTimeID("datetime",5), GeoID("geo",6), 
        PasswordId("password",8), DefaultId("default",0);

        private String value;
        private int typeVal;
        
        private TypeID(String value, int typeVal) {
            this.value = value;
            this.typeVal = typeVal;
        }

        public String getValue() {
            return value;
        }
        
        public int getTypeVal() {
            return typeVal;
        }
        
    }
}
