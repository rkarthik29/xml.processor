package com.hortonworks.nifi.xml.util;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

public class Element {
     private final String name;
     private final String type;
     private final boolean required;
     private final boolean isArray;
     private final ComplexType complexType;
     
     public Element(String name,String type,boolean required,boolean isArray){
    	 this.isArray=isArray;
    	 this.name=name;
    	 this.type=getAvroType(type);
    	 this.required=required;
    	 if(this.type.equals("complex")){
    		 this.complexType=ComplexType.newInstace(type);
    	 }else{
    		 this.complexType=null;
    	 }
    	 
     }
     
    private String getAvroType(String name){
    	switch(name){
    	case "string":
    		return "string";
    	case "integer":
    	case "int":
    		return "int";
    	case "long":
    		return "long";
    	default:
    		return "complex";
    	
    	}
    	
    }
     
    public void getAvroField(SchemaBuilder.FieldAssembler<Schema> assembler){
    	assembler.name(this.name).type(this.type);
    }

	public String getName() {
		return name;
	}

	public String getType() {
		return type;
	}

	public boolean isRequired() {
		return required;
	}

	public boolean isArray() {
		return isArray;
	}

	public ComplexType getComplexType() {
		return complexType;
	}
	
	
    
}
