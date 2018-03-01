package com.hortonworks.nifi.xml.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.events.XMLEvent;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.BaseTypeBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.avro.SchemaBuilder.TypeBuilder;
import org.apache.avro.SchemaBuilder.UnionAccumulator;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;

import com.google.gson.JsonObject;

public class XSD2AVRO3 {
	static HashMap<String, SchemaBuilder.FieldAssembler<Schema>> compMap= new HashMap<String,SchemaBuilder.FieldAssembler<Schema>>();
	private static List<String> types = new ArrayList<String>();
	static HashMap<String, SchemaBuilder.FieldAssembler<Schema>> parentMap= new HashMap<String,SchemaBuilder.FieldAssembler<Schema>>();
	
	public static void main(String[] args){
		try{
		FileInputStream fis = new FileInputStream(new File("/Users/knarayanan/RmrkType.xsd"));
		xsd2avro(fis);
		 
		
//		ComplexType purchase = ComplexType.newInstace("Purchase");
//		purchase.addElement(new Element("item","string",false,false,null));
//		purchase.addElement(new Element("price","int",false,false,null));
//		ComplexType state = ComplexType.newInstace("State");
//		state.addElement(new Element("stateName","string",false,false,null));
//		state.addElement(new Element("stateCode","string",false,false,null));
//		purchase.addElement(new Element("state","complex",false,false,state));
//		purchase.addElement(new Element("state2","complex",false,false,state));
//		DataFileWriter<Object> writer =
//		        new DataFileWriter<>(new GenericDatumWriter<>());
//	    writer.create(purchase.getSchema(),new File("/Users/knarayanan/purchase1.avsc"));
//	    writer.close();
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	public static void xsd2avro(InputStream fis) throws NumberFormatException, XMLStreamException, IOException{
		XMLStreamReader reader = XMLInputFactory.newInstance().createXMLStreamReader(fis);
    	ComplexType record=null;
		while(reader.hasNext()){
			switch(reader.next()){
			case XMLEvent.START_ELEMENT:
				if(reader.getLocalName().equals("element")){
					int attributes = reader.getAttributeCount();
					String name = attributes >= 0?reader.getAttributeValue(0):null;
					String type = attributes >= 1?reader.getAttributeValue(1):null;
					if(type==null){
						record = ComplexType.newInstace(name);
					}else{
						if(type.contains(":"))
							type=type.split(":")[1];
						record = ComplexType.newInstace(type);
						record.setDisplayName(name);
					}
					break;
				}if(reader.getLocalName().equals("complexType")){
					int attributes = reader.getAttributeCount();
					String name = attributes >= 0?reader.getAttributeValue(0):null;
					if(name==null){
						addComplexType(reader, record);
					}else{
						record = ComplexType.newInstace(name);
						addComplexType(reader, record);
					}
					
				}
			}
				
		}
		//assembler.nullableString("empl", null);
		DataFileWriter<Object> writer =
		        new DataFileWriter<>(new GenericDatumWriter<>());
	    writer.create(record.getSchema(),new File("/Users/knarayanan/delta.avsc"));
	    writer.close();
	}
	
	public static void addComplexType(XMLStreamReader reader, ComplexType record) throws XMLStreamException{
		while(reader.hasNext()){
			switch(reader.next()){
			case XMLEvent.START_ELEMENT:
				if(reader.getLocalName().equals("element")||reader.getLocalName().equals("attribute"))
						addChildElement(reader, record);
			case XMLEvent.END_ELEMENT:
				if(reader.getLocalName().equals("complexType"))
					return;
			}
		}
	}
	
	public static void xsd2avroschema(InputStream fis) throws FactoryConfigurationError, NumberFormatException, Exception{
		//InputStream fis = new FileInputStream(is);
		XMLStreamReader reader = XMLInputFactory.newInstance().createXMLStreamReader(fis);
		ComplexType record=null;
		while(reader.hasNext()){
			switch(reader.next()){
			case XMLEvent.START_ELEMENT:
				if(reader.getLocalName().equals("element")){
					int attributes = reader.getAttributeCount();
					String name = attributes >= 0?reader.getAttributeValue(0):null;
					String type = attributes >= 1?reader.getAttributeValue(1):null;
					if(type==null){
						record = ComplexType.newInstace(name);
						readComplexType(reader,name,record);
						break;
					}else{
						if(type.contains(":")){
						type=type.split(":")[1];
						}
					
						int minOccurs = reader.getAttributeValue("", "minOccurs")==null?0:Integer.parseInt(reader.getAttributeValue("", "minOccurs"));
						int maxOccurs = reader.getAttributeValue("", "maxOccurs")==null?0:Integer.parseInt(reader.getAttributeValue("", "maxOccurs"));
						String use = reader.getAttributeValue("", "use")==null?"optional":reader.getAttributeValue("", "use");
						record = ComplexType.newInstace(type);
						record.setDisplayName(name);
					}
					break;
				}if(reader.getLocalName().equals("complexType")){
					int attributes = reader.getAttributeCount();
					String name = attributes >= 0?reader.getAttributeValue(0):null;
					record = ComplexType.newInstace(name);
					addChildElements(reader, record);
				}
			}
				
		}
		//assembler.nullableString("empl", null);
		DataFileWriter<Object> writer =
		        new DataFileWriter<>(new GenericDatumWriter<>());
	    writer.create(record.getSchema(),new File("/Users/knarayanan/purchase.avsc"));
	    writer.close();
	}
	
	public static void readComplexType(XMLStreamReader reader,String name,ComplexType record) throws XMLStreamException{
		boolean found_complex_type=false;
		while(reader.hasNext()){
			switch(reader.next()){
			case XMLEvent.START_ELEMENT:
				if(reader.getLocalName().equals("complexType")){
					found_complex_type=true;
				}
				if(!found_complex_type){
					return;
				}else{
					if(reader.getLocalName().equals("element"))
						addChildElement(reader, record);
				}
				break;
			case XMLEvent.END_ELEMENT:
				if(reader.getLocalName().equals("complexType"))
					return;
			}
		}
		
	}
	
	public static ComplexType addElement(String name,String type,boolean required,boolean isArray){
		Element element = new Element(name,type,required,isArray);
		return element.getComplexType();
	}

    public static void addChildElements(XMLStreamReader reader, ComplexType record) throws NumberFormatException, XMLStreamException{
    	while(reader.hasNext()){
			switch(reader.next()){
			case XMLEvent.START_ELEMENT:
				if(reader.getLocalName().equals("element")){
					int attributes = reader.getAttributeCount();
					String name = attributes >= 0?reader.getAttributeValue(0):null;
					String type = attributes >= 1?reader.getAttributeValue(1):null;
					if(type==null){
						ComplexType cType = ComplexType.newInstace(name);
						int minOccurs = reader.getAttributeValue("", "minOccurs")==null?0:Integer.parseInt(reader.getAttributeValue("", "minOccurs"));
						int maxOccurs = reader.getAttributeValue("", "maxOccurs")==null?0:Integer.parseInt(reader.getAttributeValue("", "maxOccurs"));
						String use = reader.getAttributeValue("", "use")==null?"optional":reader.getAttributeValue("", "use");
						readComplexType(reader,name,cType);
						record.addElement(new Element(name,cType.getDisplayName(),"required".equals(use),false));
						break;
					}else{
						if(type.contains(":")){
							type=type.split(":")[1];
						}
					
						int minOccurs = reader.getAttributeValue("", "minOccurs")==null?0:Integer.parseInt(reader.getAttributeValue("", "minOccurs"));
						int maxOccurs = reader.getAttributeValue("", "maxOccurs")==null?0:Integer.parseInt(reader.getAttributeValue("", "maxOccurs"));
						String use = reader.getAttributeValue("", "use")==null?"optional":reader.getAttributeValue("", "use");
						record.addElement(new Element(name,type,"required".equals(use),false));
					}
					break;
				}if(reader.getLocalName().equals("complexType")){
					int attributes = reader.getAttributeCount();
					String name = attributes >= 0?reader.getAttributeValue(0):null;
					ComplexType complexType = ComplexType.newInstace(name);
					addChildElements(reader, complexType);
				}
			}
				
		}
    }
    
    public static XMLStreamReader detecNextStartElement(XMLStreamReader reader, String element) throws XMLStreamException{
    	while(reader.hasNext()){
    		int type=reader.next();
    		if (type==XMLEvent.START_ELEMENT && reader.getLocalName().equals(element))
    			return reader;
    	}
    		
    	return reader;
    }
    
    public static void addChildElement(XMLStreamReader reader, ComplexType record) throws XMLStreamException{
			int attributes = reader.getAttributeCount();
			String name = attributes >= 0?reader.getAttributeValue(0):null;
			String type = attributes >= 1?reader.getAttributeValue(1):null;
			if(type==null){
				ComplexType cType = ComplexType.newInstace(name);
				record.addElement(new Element(name,name,false,false));
				addComplexType(detecNextStartElement(reader,"complexType"), cType);
			}else{
				if(type.contains(":")){
				type=type.split(":")[1];
				}
				//int minOccurs = reader.getAttributeValue("", "minOccurs")==null?0:Integer.parseInt(reader.getAttributeValue("", "minOccurs"));
				//int maxOccurs = reader.getAttributeValue("", "maxOccurs")==null?0:Integer.parseInt(reader.getAttributeValue("", "maxOccurs"));
				//String use = reader.getAttributeValue("", "use")==null?"optional":reader.getAttributeValue("", "use");
				record.addElement(new Element(name,type,false,false));
			}
    }

}
