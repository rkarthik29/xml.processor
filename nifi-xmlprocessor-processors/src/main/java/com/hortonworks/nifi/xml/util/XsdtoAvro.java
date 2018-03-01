package com.hortonworks.nifi.xml.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.events.XMLEvent;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

public class XsdtoAvro {
	static HashMap<String, SchemaBuilder.FieldAssembler<Schema>> compMap= new HashMap<String,SchemaBuilder.FieldAssembler<Schema>>();
	Map<String,String> simpleTypeMap = new HashMap<String,String>();
	static JsonArray avroObject = new JsonArray();
	static String path="/Users/knarayanan/Downloads/ode_pids_messages_dotnet_w2k3/xsd/";
	static Set<String> processed = new HashSet<String>();
	public static void main(String[] args){
		try{
		String fileName="BkngSvrStbyListPutbackPsgrType.xsd";
		XsdtoAvro avro = new XsdtoAvro();
		avro.xsd2avro(fileName);
		System.out.println(avroObject);
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	public void xsd2avro(String filename) throws NumberFormatException, XMLStreamException, IOException{
		handleIncludes(new FileInputStream(path+filename));
		handleSimpleTypes(new FileInputStream(path+filename));
		handleComplexTypes(new FileInputStream(path+filename));
	}
	
    public void handleIncludes(InputStream fis) throws XMLStreamException, FactoryConfigurationError, NumberFormatException, IOException{
    	XMLStreamReader reader = XMLInputFactory.newInstance().createXMLStreamReader(fis);
    	ComplexType record=null;
		while(reader.hasNext()){
			switch(reader.next()){
			case XMLEvent.START_ELEMENT:
				if(reader.getLocalName().equals("include")){
					String location = reader.getAttributeValue(0);
					if(!processed.contains(location)){
						XsdtoAvro avro = new XsdtoAvro();
						avro.xsd2avro(location);
					processed.add(location);
					}
				}
			}
				
		}
    }
	
	
	public void handleSimpleTypes(InputStream fis) throws XMLStreamException, NumberFormatException, IOException{
		XMLStreamReader reader = XMLInputFactory.newInstance().createXMLStreamReader(fis);
    	ComplexType record=null;
		while(reader.hasNext()){
			switch(reader.next()){
			case XMLEvent.START_ELEMENT:
				if(reader.getLocalName().equals("include")){
					String location = reader.getAttributeValue(0);
					if(!processed.contains(location)){
						XsdtoAvro avro = new XsdtoAvro();
						avro.xsd2avro(location);
					processed.add(location);
					}
				}
			   if(reader.getLocalName().equals("simpleType")){
				   String name = reader.getAttributeValue(0);
				   handleSimpleType(reader,name);
			   }
			}
				
		}
	}
	
	public void handleComplexType(XMLStreamReader reader,String name) throws XMLStreamException{
		JsonObject jsonObject = new JsonObject();
		jsonObject.addProperty("type", "record");
		jsonObject.addProperty("name", name);
		JsonArray fields = new JsonArray();
		jsonObject.add("field", fields);
		while(reader.hasNext()){
			switch(reader.next()){
			case XMLEvent.START_ELEMENT:
			if(reader.getLocalName().equals("attribute")){
				JsonObject field = new JsonObject();
				String fname = reader.getAttributeValue("", "name");
				String type = reader.getAttributeValue("", "type");
				field.addProperty("name", fname);
				String simpleType = simpleTypeMap.get(type);
				if(simpleType!=null)
					type=simpleType;
				if(type!=null){
					if(type.contains(":")){
					type= type.split(":")[1];
					}
				}
				field.addProperty("type",getAvroType(type));
				fields.add(field);
			   }
			
			break;
			case XMLEvent.END_ELEMENT :
				if(reader.getLocalName().equals("complexType")){
					avroObject.add(jsonObject);
					return;
				}
				
			}
		}
	}
	
	public void handleComplexTypes(InputStream fis) throws XMLStreamException{
		XMLStreamReader reader = XMLInputFactory.newInstance().createXMLStreamReader(fis);
    	ComplexType record=null;
		while(reader.hasNext()){
			switch(reader.next()){
			case XMLEvent.START_ELEMENT:
			   if(reader.getLocalName().equals("complexType")){
				   String name = reader.getAttributeValue(0);
				   handleComplexType(reader,name);
			   }
			}
				
		}
	}
	
	public void handleSimpleType(XMLStreamReader reader,String name) throws XMLStreamException{
		while(reader.hasNext()){
			switch(reader.next()){
			case XMLEvent.START_ELEMENT:
			if(reader.getLocalName().equals("restriction")){
				String type = reader.getAttributeValue("", "base");
				simpleTypeMap.put(name, type);
				return;
			   }
			}
		}
	}
	
	public static String getAvroType(String type){
		type=type.toLowerCase();
		if(type.contains("string"))
			return "string";
		if(type.contains("int"))
			return "int";
		if(type.contains("long"))
			return "long";
		if(type.contains("short"))
			return "int";
		if(type.contains("decimal"))
			return "double";
		if(type.contains("boolean"))
			return "boolean";
		return "string";
	}
	
	
}
