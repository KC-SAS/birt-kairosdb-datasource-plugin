package com.kratos.birt.report.data.oda.kairosdb.json;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import javax.validation.ConstraintViolation;
import javax.validation.Path;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.metadata.ConstraintDescriptor;

import org.apache.bval.jsr303.ApacheValidationProvider;
import org.eclipse.datatools.connectivity.oda.OdaException;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;


public class QueryParser {
	@SuppressWarnings("rawtypes")
	private Map<Class, Map<String, PropertyDescriptor>> m_descriptorMap;
	private static final Validator VALIDATOR = Validation.byProvider(ApacheValidationProvider.class).configure().buildValidatorFactory().getValidator();
	private Gson m_gson;


	@SuppressWarnings("rawtypes")
	public QueryParser() {
		GsonBuilder builder = new GsonBuilder();
		builder.registerTypeAdapterFactory(new LowercaseEnumTypeAdapterFactory());
		builder.registerTypeAdapter(TimeUnit.class, new TimeUnitDeserializer());
		//builder.registerTypeAdapter(Metric.class, new MetricDeserializer());
		m_descriptorMap = new HashMap<Class, Map<String, PropertyDescriptor>>();
		m_gson = builder.create();
	}

	public boolean displayMetricColumn(String queryText) throws OdaException{
		JsonParser parser = new JsonParser();
		JsonObject obj = parser.parse(queryText).getAsJsonObject();
		JsonArray metricsArray = obj.getAsJsonArray("metrics");
		if (metricsArray == null) 
			throw new OdaException("metric[] must have a size of at least 1");
		return metricsArray.size() > 1;
	}

	public boolean validateQuery(String queryText){
		GroupByFactory m_groupByFactory = new GroupByFactory();
		JsonParser parser = new JsonParser();
		JsonObject obj = parser.parse(queryText).getAsJsonObject();
		JsonArray metricsArray = obj.getAsJsonArray("metrics");
		if (metricsArray == null) 
			return false;
		for (int I = 0; I < metricsArray.size(); I++)
		{
			String context = "query.metric[" + I + "]";
			JsonObject jsMetric = metricsArray.get(I).getAsJsonObject();
			JsonElement group_by = jsMetric.get("group_by");
			if (group_by != null)
			{
				JsonArray groupBys = group_by.getAsJsonArray();
				for (int J = 0; J < groupBys.size(); J++)
				{
					String groupContext = "group_by[" + J + "]";
					JsonObject jsGroupBy = groupBys.get(J).getAsJsonObject();

					JsonElement nameElement = jsGroupBy.get("name");
					if (nameElement == null || nameElement.getAsString().isEmpty())
						return false;

					String name = nameElement.getAsString();

					GroupBy groupBy = m_groupByFactory.createGroupBy(name);
					if (groupBy == null)
						return false;

					try {
						deserializeProperties(context + "." + groupContext, jsGroupBy, name, groupBy);
						validateObject(groupBy, context + "." + groupContext);
					} catch (BeanValidationException e) {
						return false;
					} catch (OdaException e) {
						return false;
					}
				}
			}
		}
		return true;
	}

	
	public String changeStartDate(String queryText,long timestamp) throws IOException{
		JsonParser parser = new JsonParser();
		JsonObject obj = parser.parse(queryText).getAsJsonObject();
		StringWriter writer = new StringWriter();
		JsonWriter m_jsonWriter = new JsonWriter(writer);
		m_jsonWriter.beginObject();
		for(Map.Entry<String,JsonElement> entry : obj.entrySet()){
			if(!entry.getKey().equals("start_relative") && !entry.getKey().equals("start_absolute")){
				m_jsonWriter.name(entry.getKey());
				writeRecursively(entry.getValue(),m_jsonWriter);
			}
		}
		m_jsonWriter.name("start_absolute");
		m_jsonWriter.value(timestamp);
		m_jsonWriter.endObject();
		writer.flush();
		return writer.toString();
	}
	
	public String changeEndDate(String queryText,long timestamp) throws IOException{
		JsonParser parser = new JsonParser();
		JsonObject obj = parser.parse(queryText).getAsJsonObject();
		StringWriter writer = new StringWriter();
		JsonWriter m_jsonWriter = new JsonWriter(writer);
		m_jsonWriter.beginObject();
		for(Map.Entry<String,JsonElement> entry : obj.entrySet()){
			if(!entry.getKey().equals("end_relative") && !entry.getKey().equals("end_absolute")){
				m_jsonWriter.name(entry.getKey());
				writeRecursively(entry.getValue(),m_jsonWriter);
			}
		}
		m_jsonWriter.name("end_absolute");
		m_jsonWriter.value(timestamp);
		m_jsonWriter.endObject();
		writer.flush();
		return writer.toString();
	}
	
	
    private void writeRecursively(JsonElement queryElement, JsonWriter m_jsonWriter) throws IOException{
        if(queryElement.isJsonArray()){
            m_jsonWriter.beginArray();
            for(JsonElement element : queryElement.getAsJsonArray()){
                writeRecursively(element,m_jsonWriter);
            }
            m_jsonWriter.endArray();
        }else if(queryElement.isJsonPrimitive()){
            if(queryElement.getAsJsonPrimitive().isNumber())
                m_jsonWriter.value(queryElement.getAsJsonPrimitive().getAsNumber());
            else if(queryElement.getAsJsonPrimitive().isString())
                m_jsonWriter.value(queryElement.getAsJsonPrimitive().getAsString());
            else if(queryElement.getAsJsonPrimitive().isBoolean())
                m_jsonWriter.value(queryElement.getAsJsonPrimitive().getAsBoolean());
        }else if(queryElement.isJsonObject()){
            m_jsonWriter.beginObject();
            for(Map.Entry<String,JsonElement> entry : queryElement.getAsJsonObject().entrySet()){
                m_jsonWriter.name(entry.getKey());
                writeRecursively(entry.getValue(),m_jsonWriter);
            }
            m_jsonWriter.endObject();
        }
        
    }
	
	
	public boolean validateJson(String queryText){
		try {
			JsonParser parser = new JsonParser();
			parser.parse(queryText).getAsJsonObject();
		} catch (Exception ex) {
			return false;
		}
		return true;
	}
	
	public void parse(String queryText,TreeSet<String> tagList,TreeSet<Integer>  valueList,TreeSet<Duration>  timeList) throws OdaException{
		GroupByFactory m_groupByFactory = new GroupByFactory();
		JsonParser parser = new JsonParser();
		JsonObject obj = parser.parse(queryText).getAsJsonObject();
		JsonArray metricsArray = obj.getAsJsonArray("metrics");
		if (metricsArray == null) 
			throw new OdaException("metric[] must have a size of at least 1");
		for (int I = 0; I < metricsArray.size(); I++)
		{
			String context = "query.metric[" + I + "]";
			JsonObject jsMetric = metricsArray.get(I).getAsJsonObject();
			JsonElement group_by = jsMetric.get("group_by");
			if (group_by != null)
			{
				JsonArray groupBys = group_by.getAsJsonArray();
				for (int J = 0; J < groupBys.size(); J++)
				{
					String groupContext = "group_by[" + J + "]";
					JsonObject jsGroupBy = groupBys.get(J).getAsJsonObject();

					JsonElement nameElement = jsGroupBy.get("name");
					if (nameElement == null || nameElement.getAsString().isEmpty())
						throw new OdaException(new BeanValidationException(new SimpleConstraintViolation(groupContext, "must have a name"), context).getMessage());

					String name = nameElement.getAsString();

					GroupBy groupBy = m_groupByFactory.createGroupBy(name);
					if (groupBy == null)
						throw new OdaException(new BeanValidationException(new SimpleConstraintViolation(groupContext + "." + name, "invalid group_by name"), context).getMessage());

					try {
						deserializeProperties(context + "." + groupContext, jsGroupBy, name, groupBy);
						validateObject(groupBy, context + "." + groupContext);
					} catch (BeanValidationException e) {
						throw new OdaException(e.getMessage());
					}

					groupBy.addToSet(tagList, valueList, timeList);
				}
			}
		}
	}















	private static class SimpleConstraintViolation implements ConstraintViolation<Object>
	{
		private String message;
		private String context;

		public SimpleConstraintViolation(String context, String message)
		{
			this.message = message;
			this.context = context;
		}

		@Override
		public String getMessage()
		{
			return message;
		}

		@Override
		public String getMessageTemplate()
		{
			return null;
		}

		@Override
		public Object getRootBean()
		{
			return null;
		}

		@Override
		public Class<Object> getRootBeanClass()
		{
			return null;
		}

		@Override
		public Object getLeafBean()
		{
			return null;
		}

		@Override
		public Path getPropertyPath()
		{
			return new SimplePath(context);
		}

		@Override
		public Object getInvalidValue()
		{
			return null;
		}

		@Override
		public ConstraintDescriptor<?> getConstraintDescriptor()
		{
			return null;
		}
	}

	private static class SimplePath implements Path
	{
		private String context;

		private SimplePath(String context)
		{
			this.context = context;
		}

		@Override
		public Iterator<Node> iterator()
		{
			return null;
		}

		@Override
		public String toString()
		{
			return context;
		}
	}



	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void deserializeProperties(String context, JsonObject jsonObject, String name, Object object) throws OdaException, BeanValidationException
	{
		Set<Map.Entry<String, JsonElement>> props = jsonObject.entrySet();
		for (Map.Entry<String, JsonElement> prop : props)
		{
			String property = prop.getKey();
			if (property.equals("name"))
				continue;

			PropertyDescriptor pd = null;
			try
			{
				pd = getPropertyDescriptor(object.getClass(), property);
			}
			catch (IntrospectionException e)
			{

			}

			if (pd == null)
			{
				String msg = "Property '" + property + "' was specified for object '" + name +
						"' but no matching setter was found on '" + object.getClass() + "'";

				throw new OdaException(msg);
			}

			Class propClass = pd.getPropertyType();

			Object propValue;
			try
			{
				propValue = m_gson.fromJson(prop.getValue(), propClass);
				validateObject(propValue, context + "." + property);
			}
			catch (ContextualJsonSyntaxException e)
			{
				throw new BeanValidationException(new SimpleConstraintViolation(e.getContext(), e.getMessage()), context);
			}
			catch(NumberFormatException e)
			{
				throw new BeanValidationException(new SimpleConstraintViolation(property, e.getMessage()), context);
			}

			Method method = pd.getWriteMethod();
			if (method == null)
			{
				String msg = "Property '" + property + "' was specified for object '" + name +
						"' but no matching setter was found on '" + object.getClass().getName() + "'";

				throw new OdaException(msg);
			}

			try
			{
				method.invoke(object, propValue);
			}
			catch (Exception e)
			{
				String msg = "Call to " + object.getClass().getName() + ":" + method.getName() +
						" failed with message: " + e.getMessage();

				throw new OdaException(msg);
			}
		}
	}


	// GROUP BY DESERIALIZATION METHODS (should be refactored into a deserializer class/package)

	private PropertyDescriptor getPropertyDescriptor(@SuppressWarnings("rawtypes") Class objClass, String property) throws IntrospectionException
	{

		Map<String, PropertyDescriptor> propMap = m_descriptorMap.get(objClass);

		if (propMap == null)
		{
			propMap = new HashMap<String, PropertyDescriptor>();
			m_descriptorMap.put(objClass, propMap);

			BeanInfo beanInfo = Introspector.getBeanInfo(objClass);
			PropertyDescriptor[] descriptors = beanInfo.getPropertyDescriptors();
			for (PropertyDescriptor descriptor : descriptors)
			{
				propMap.put(getUnderscorePropertyName(descriptor.getName()), descriptor);
			}
		}

		return (propMap.get(property));

	}

	private void validateObject(Object object, String context) throws BeanValidationException
	{
		// validate object using the bean validation framework
		Set<ConstraintViolation<Object>> violations = VALIDATOR.validate(object);
		if (!violations.isEmpty())
		{
			throw new BeanValidationException(violations, context);
		}
	}

	@SuppressWarnings("serial")
	private static class ContextualJsonSyntaxException extends RuntimeException
	{
		private String context;

		private ContextualJsonSyntaxException(String context, String msg)
		{
			super(msg);
			this.context = context;
		}

		private String getContext()
		{
			return context;
		}
	}

	public static String getUnderscorePropertyName(String camelCaseName)
	{
		StringBuilder sb = new StringBuilder();

		for (char c : camelCaseName.toCharArray())
		{
			if (Character.isUpperCase(c))
				sb.append('_').append(Character.toLowerCase(c));
			else
				sb.append(c);
		}

		return (sb.toString());
	}

	private class TimeUnitDeserializer implements JsonDeserializer<TimeUnit>
	{
		public TimeUnit deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
				throws JsonParseException
		{
			String unit = json.getAsString();
			TimeUnit tu;

			try
			{
				tu = TimeUnit.from(unit);
			}
			catch (IllegalArgumentException e)
			{
				throw new ContextualJsonSyntaxException(unit,
						"is not a valid time unit, must be one of " + TimeUnit.toValueNames());
			}

			return tu;
		}
	}	

	private static class LowercaseEnumTypeAdapterFactory implements TypeAdapterFactory
	{
		public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> type)

		{
			@SuppressWarnings("unchecked")
			Class<T> rawType = (Class<T>) type.getRawType();
			if (!rawType.isEnum())
			{
				return null;
			}

			final Map<String, T> lowercaseToConstant = new HashMap<String, T>();
			for (T constant : rawType.getEnumConstants())
			{
				lowercaseToConstant.put(toLowercase(constant), constant);
			}

			return new TypeAdapter<T>()
					{
				public void write(JsonWriter out, T value) throws IOException
				{
					if (value == null)
					{
						out.nullValue();
					}
					else
					{
						out.value(toLowercase(value));
					}
				}

				public T read(JsonReader reader) throws IOException
				{
					if (reader.peek() == JsonToken.NULL)
					{
						reader.nextNull();
						return null;
					}
					else
					{
						return lowercaseToConstant.get(reader.nextString());
					}
				}
					};
		}

		private String toLowercase(Object o)
		{
			return o.toString().toLowerCase(Locale.US);
		}
	}
}
