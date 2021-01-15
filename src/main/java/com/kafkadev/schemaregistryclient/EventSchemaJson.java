package com.dassault_systemes.schemaregistryclient;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Event type sample schema with 3 identic fields [String,String,String] used to
 * produce messages in JsonSchema using Schema Registry
 * 
 * @author SKI44
 *
 */
public class EventSchemaJson {

	@JsonProperty
	public String input1;

	@JsonProperty
	public String input2;

	@JsonProperty
	public String input3;

	public EventSchemaJson() {

	}

	public EventSchemaJson(String input1, String input2, String input3) {
		this.input1 = input1;
		this.input2 = input2;
		this.input3 = input3;

	}
}
