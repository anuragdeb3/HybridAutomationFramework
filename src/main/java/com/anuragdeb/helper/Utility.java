package com.anuragdeb.helper;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.json.FlattenMap;
 

public class Utility {
	
	

	public static void compareJsons(String actualJson, String expectedJson) throws JsonMappingException, JsonProcessingException {
		
		ObjectMapper mapper = new ObjectMapper();
		
		TypeReference<Map<String,Object>> type = new TypeReference<Map<String,Object>>(){};
		
		Map<String, Object> actualMap= mapper.readValue(actualJson, type);
		Map<String, Object> expectedMap= mapper.readValue(expectedJson, type);
		
		
		System.out.println("Left Json "+actualMap);
		System.out.println("Right Json "+expectedMap);
		
		/*Assert.assertEquals(actualMap,expectedMap);
		System.out.println("Both Jsons are perfect match");
		
		MapDifference<String, Object> difference = Maps.difference(actualMap, expectedMap);
		
		difference.entriesDiffering().forEach((key,value) -> System.out.println("key:"+key+"---> Value"+value));
		
		*/
		
		Map<String, Object> leftFlatMap = FlattenMap.flatten(actualMap);
		Map<String, Object> rightFlatMap = FlattenMap.flatten(expectedMap);
		
		MapDifference<String, Object> difference = Maps.difference(leftFlatMap, rightFlatMap);
		//System.out.println("Same Key but different Values :");
		
		Map<String,Object> entryDiff = new HashMap<>();
		Map<String,Object> leftDiff = new HashMap<>();
		Map<String,Object> rightDiff = new HashMap<>();
		
		difference.entriesDiffering().forEach((key,value) -> {
			//System.out.println("key:"+key+"---> Value"+value);
			entryDiff.put(key, value);
		});
		//System.out.println("Entries that are present in left but not in right ");
		
		difference.entriesOnlyOnLeft().forEach((key,value) -> {
			//System.out.println("key:"+key+"---> Value"+value);
			leftDiff.put(key, value);
		});
		//System.out.println("Entries that are present in right but not in left ");
		difference.entriesDiffering().forEach((key,value) -> {
			//System.out.println("key:"+key+"---> Value"+value);
			rightDiff.put(key, value);
		});
		
		
		System.out.println("================================================");
		if (!isNullOrEmpty(entryDiff)) {
			System.out.println("Entries where Key is having different Value");
			entryDiff.forEach((key,value) -> System.out.println("key:"+key+"---> Value"+value));
		}
		System.out.println("================================================");
		if (!isNullOrEmpty(leftDiff)) {
			System.out.println("Entries that are present in right but not in left ");
			leftDiff.forEach((key,value) -> System.out.println("key:"+key+"---> Value"+value));
		}
		System.out.println("================================================");
		if (!isNullOrEmpty(rightDiff)) {
			System.out.println("Entries that are present in right but not in left ");
			rightDiff.forEach((key,value) -> System.out.println("key:"+key+"---> Value"+value));
		}
		System.out.println("================================================");
		
		
		
		
		
	}
	
	
	public static boolean isNullOrEmpty( Map< ?,? > map ) {
	    return map == null || map.isEmpty();
	}
	

}
