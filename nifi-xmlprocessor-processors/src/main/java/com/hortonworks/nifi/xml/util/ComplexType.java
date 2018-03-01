package com.hortonworks.nifi.xml.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.BaseTypeBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.avro.SchemaBuilder.RecordBuilder;
import org.apache.avro.SchemaBuilder.UnionAccumulator;

public class ComplexType {
     private final String name;
     private String displayName;
     private boolean isProcessed;
     private final List<Element> elements = new ArrayList<Element>();
     private static final Map<String, ComplexType> typeMap = new HashMap<String,ComplexType>();
     
     private ComplexType(String name){
    	 this.name=name;
    	 this.displayName=name;
    	 this.isProcessed=false;
     }
     
     public static ComplexType newInstace(String name){
    	 if(typeMap.containsKey(name)){
    		 return typeMap.get(name);
    	 }else{
    		 ComplexType cType = new ComplexType(name);
    		 typeMap.put(name, cType);
    		 return cType;
    	 }
     }
     
     public void addElement(Element element){
    	 this.elements.add(element);
     }
     
     public Schema getSchema(){
    	BaseTypeBuilder<UnionAccumulator<Schema>> uBuilder = SchemaBuilder.unionOf();
    	List<Schema> schemas = new ArrayList<Schema>();
    	//RecordBuilder<Schema> builder = SchemaBuilder.record(displayName);
    	FieldAssembler<UnionAccumulator<Schema>> assembler=null;
 		for(Element element:elements){
 			if("complex".equals(element.getType())){
 				if(element.getComplexType()!=null)
 					if(!element.getComplexType().isProcessed)
 						uBuilder = element.getComplexType().getSchema(uBuilder).and();
 			}
 		}for(Element element:elements){
 			if(assembler==null){
 				assembler = uBuilder.record(displayName).fields();
 			}
 			if("complex".equals(element.getType())){
 			assembler.name(element.getName()).type(element.getComplexType().displayName).withDefault(null);
 			}else{
 				assembler.name(element.getName()).type(element.getType()).noDefault();
 			}
 		}
 		isProcessed=true;
 		return assembler.endRecord().endUnion();
     }

	private UnionAccumulator<Schema> getSchema(BaseTypeBuilder<UnionAccumulator<Schema>> uBuilder) {
		// TODO Auto-generated method stub
		
		FieldAssembler<UnionAccumulator<Schema>> assembler=null;
 		for(Element element:elements){
 			if("complex".equals(element.getType())){
 				if(element.getComplexType()!=null)
 					if(!element.getComplexType().isProcessed)
 						assembler = element.getComplexType().getSchema(uBuilder).and().record(displayName).fields();
 			}
 		}for(Element element:elements){
 			if(assembler==null){
 				RecordBuilder<UnionAccumulator<Schema>> builder = uBuilder.record(displayName);
 				assembler = builder.fields();
 			}
 			if("complex".equals(element.getType())){
 			assembler.name(element.getName()).type(element.getComplexType().displayName).withDefault(null);
 			}else{
 				assembler.name(element.getName()).type(element.getType()).noDefault();
 			}
 		}
 		isProcessed=true;
 		return assembler.endRecord();
	}

	public String getDisplayName() {
		return displayName;
	}

	public void setDisplayName(String displayName) {
		this.displayName = displayName;
	}
     
}
