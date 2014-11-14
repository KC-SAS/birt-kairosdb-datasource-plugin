/*
 *************************************************************************
 * Copyright (c) 2014 <<Your Company Name here>>
 *  
 *************************************************************************
 */

package com.kratos.birt.report.data.oda.kairosdb.impl;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
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
import org.eclipse.datatools.connectivity.oda.IParameterMetaData;
import org.eclipse.datatools.connectivity.oda.IQuery;
import org.eclipse.datatools.connectivity.oda.IResultSet;
import org.eclipse.datatools.connectivity.oda.IResultSetMetaData;
import org.eclipse.datatools.connectivity.oda.OdaException;
import org.eclipse.datatools.connectivity.oda.SortSpec;
import org.eclipse.datatools.connectivity.oda.spec.QuerySpecification;
import org.kairosdb.client.HttpClient;
import org.kairosdb.client.builder.QueryBuilder;
import org.kairosdb.client.response.QueryResponse;
import org.kairosdb.client.response.Results;

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
import com.kratos.birt.report.data.oda.kairosdb.util.BeanValidationException;
import com.kratos.birt.report.data.oda.kairosdb.util.Duration;
import com.kratos.birt.report.data.oda.kairosdb.util.GroupBy;
import com.kratos.birt.report.data.oda.kairosdb.util.GroupByFactory;
import com.kratos.birt.report.data.oda.kairosdb.util.TimeUnit;

/**
 * Implementation class of IQuery for an ODA runtime driver.
 * <br>
 * For demo purpose, the auto-generated method stubs have
 * hard-coded implementation that returns a pre-defined set
 * of meta-data and query results.
 * A custom ODA driver is expected to implement own data source specific
 * behavior in its place. 
 */
public class Query implements IQuery
{
	private int m_maxRows;
    private String m_preparedText;
    private String hostName;
    private QueryBuilder builder;
    private TreeSet<String> tagList;
    private TreeSet<Integer> valueList;
    private TreeSet<Duration> timeList;
    private Map<Class, Map<String, PropertyDescriptor>> m_descriptorMap;
    private static final Validator VALIDATOR = Validation.byProvider(ApacheValidationProvider.class).configure().buildValidatorFactory().getValidator();
    private Gson m_gson;
    
	public Query(String hostName) {
		this.hostName = hostName;
		GsonBuilder builder = new GsonBuilder();
		builder.registerTypeAdapterFactory(new LowercaseEnumTypeAdapterFactory());
		builder.registerTypeAdapter(TimeUnit.class, new TimeUnitDeserializer());
		//builder.registerTypeAdapter(Metric.class, new MetricDeserializer());
		m_descriptorMap = new HashMap<Class, Map<String, PropertyDescriptor>>();
		m_gson = builder.create();
	}
    
	/*
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#prepare(java.lang.String)
	 */
	@Override
	public void prepare( String queryText ) throws OdaException
	{
		tagList = new TreeSet<String>();
		timeList = new TreeSet<Duration>();
		valueList = new TreeSet<Integer>();
		
		
        m_preparedText = queryText;
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
//					JsonElement nameElement = groupBys.get(J).getAsJsonObject().get("name");
//					if (nameElement == null || nameElement.getAsString().isEmpty())
//						throw new OdaException("Group_by must have a name");
//					String name = nameElement.getAsString();
//					if(name.equals("tag")){
//						JsonArray tagsArray = groupBys.get(J).getAsJsonObject().get("tags").getAsJsonArray();
//						if (tagsArray == null || tagsArray.size()==0)
//							throw new OdaException("Group_by tag must have a 'tags' parameter");	
//						for(JsonElement element :tagsArray)
//							tagList.add(element.getAsString());
//					} else if(name.equals("time")){
//						JsonObject timeGrouping = groupBys.get(J).getAsJsonObject().get("tags").getAsJsonObject();
//						if (timeGrouping == null || timeGrouping.get("range_size")==null)
//							throw new OdaException("Group_by time must have a 'range_size' parameter");	
//						timeList.add(timeGrouping.get("range_size").);
//					} else if(name.equals("value")){
//						
//					} else if(name.equals("type")){
//						
//					}
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
        System.out.println("queryText: "+queryText);
        
//    	builder = QueryBuilder.getInstance();
//    	builder.setStart(1, TimeUnit.DAYS)
//    	       .addMetric("kairosdb.protocol.http_request_count")
//    	       .addAggregator(AggregatorFactory.createSumAggregator(5, TimeUnit.MINUTES))
//    	       .addGrouper(new TagGrouper("method"));
//    	tagList.add("method");
	}
	
	/*
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#setAppContext(java.lang.Object)
	 */
	@Override
	public void setAppContext( Object context ) throws OdaException
	{
	    // do nothing; assumes no support for pass-through context
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#close()
	 */
	@Override
	public void close() throws OdaException
	{
        // TODO Auto-generated method stub
        m_preparedText = null;
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#getMetaData()
	 */
	@Override
	public IResultSetMetaData getMetaData() throws OdaException
	{
        /* TODO Auto-generated method stub
         * Replace with implementation to return an instance 
         * based on this prepared query.
         */
		return new ResultSetMetaData(tagList,timeList,valueList);
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#executeQuery()
	 */
	@Override
	public IResultSet executeQuery() throws OdaException
	{
		QueryResponse response = null;
		try {
			HttpClient client = new HttpClient(hostName);
			if(m_preparedText!=null){
				response = client.query(m_preparedText);
			} else {
				response = client.query(builder);
			}
			client.shutdown();
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		IResultSet resultSet = new ResultSet(response,tagList,timeList,valueList);
		int nbFetched = 0;
		for(Results result : response.getQueries().get(0).getResults()){
			nbFetched += result.getDataPoints().size();
		}
		System.out.println("Nb points fetched: "+nbFetched);
		resultSet.setMaxRows(nbFetched);
		return resultSet;
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#setProperty(java.lang.String, java.lang.String)
	 */
	@Override
	public void setProperty( String name, String value ) throws OdaException
	{
		// do nothing; assumes no data set query property
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#setMaxRows(int)
	 */
	@Override
	public void setMaxRows( int max ) throws OdaException
	{
	    m_maxRows = max;
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#getMaxRows()
	 */
	@Override
	public int getMaxRows() throws OdaException
	{
		return m_maxRows;
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#clearInParameters()
	 */
	@Override
	public void clearInParameters() throws OdaException
	{
        // TODO Auto-generated method stub
		// only applies to input parameter
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#setInt(java.lang.String, int)
	 */
	@Override
	public void setInt( String parameterName, int value ) throws OdaException
	{
        // TODO Auto-generated method stub
		// only applies to named input parameter
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#setInt(int, int)
	 */
	@Override
	public void setInt( int parameterId, int value ) throws OdaException
	{
        // TODO Auto-generated method stub
		// only applies to input parameter
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#setDouble(java.lang.String, double)
	 */
	@Override
	public void setDouble( String parameterName, double value ) throws OdaException
	{
        // TODO Auto-generated method stub
		// only applies to named input parameter
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#setDouble(int, double)
	 */
	@Override
	public void setDouble( int parameterId, double value ) throws OdaException
	{
        // TODO Auto-generated method stub
		// only applies to input parameter
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#setBigDecimal(java.lang.String, java.math.BigDecimal)
	 */
	@Override
	public void setBigDecimal( String parameterName, BigDecimal value ) throws OdaException
	{
        // TODO Auto-generated method stub
		// only applies to named input parameter
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#setBigDecimal(int, java.math.BigDecimal)
	 */
	@Override
	public void setBigDecimal( int parameterId, BigDecimal value ) throws OdaException
	{
        // TODO Auto-generated method stub
		// only applies to input parameter
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#setString(java.lang.String, java.lang.String)
	 */
	@Override
	public void setString( String parameterName, String value ) throws OdaException
	{
        // TODO Auto-generated method stub
		// only applies to named input parameter
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#setString(int, java.lang.String)
	 */
	@Override
	public void setString( int parameterId, String value ) throws OdaException
	{
        // TODO Auto-generated method stub
		// only applies to input parameter
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#setDate(java.lang.String, java.sql.Date)
	 */
	@Override
	public void setDate( String parameterName, Date value ) throws OdaException
	{
        // TODO Auto-generated method stub
		// only applies to named input parameter
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#setDate(int, java.sql.Date)
	 */
	@Override
	public void setDate( int parameterId, Date value ) throws OdaException
	{
        // TODO Auto-generated method stub
		// only applies to input parameter
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#setTime(java.lang.String, java.sql.Time)
	 */
	@Override
	public void setTime( String parameterName, Time value ) throws OdaException
	{
        // TODO Auto-generated method stub
		// only applies to named input parameter
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#setTime(int, java.sql.Time)
	 */
	@Override
	public void setTime( int parameterId, Time value ) throws OdaException
	{
        // TODO Auto-generated method stub
		// only applies to input parameter
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#setTimestamp(java.lang.String, java.sql.Timestamp)
	 */
	@Override
	public void setTimestamp( String parameterName, Timestamp value ) throws OdaException
	{
        // TODO Auto-generated method stub
		// only applies to named input parameter
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#setTimestamp(int, java.sql.Timestamp)
	 */
	@Override
	public void setTimestamp( int parameterId, Timestamp value ) throws OdaException
	{
        // TODO Auto-generated method stub
		// only applies to input parameter
	}

    /* (non-Javadoc)
     * @see org.eclipse.datatools.connectivity.oda.IQuery#setBoolean(java.lang.String, boolean)
     */
    @Override
	public void setBoolean( String parameterName, boolean value )
            throws OdaException
    {
        // TODO Auto-generated method stub
        // only applies to named input parameter
    }

    /* (non-Javadoc)
     * @see org.eclipse.datatools.connectivity.oda.IQuery#setBoolean(int, boolean)
     */
    @Override
	public void setBoolean( int parameterId, boolean value )
            throws OdaException
    {
        // TODO Auto-generated method stub       
        // only applies to input parameter
    }

    /* (non-Javadoc)
     * @see org.eclipse.datatools.connectivity.oda.IQuery#setObject(java.lang.String, java.lang.Object)
     */
    @Override
	public void setObject( String parameterName, Object value )
            throws OdaException
    {
        // TODO Auto-generated method stub
        // only applies to named input parameter
    }
    
    /* (non-Javadoc)
     * @see org.eclipse.datatools.connectivity.oda.IQuery#setObject(int, java.lang.Object)
     */
    @Override
	public void setObject( int parameterId, Object value ) throws OdaException
    {
        // TODO Auto-generated method stub
        // only applies to input parameter
    }
    
    /* (non-Javadoc)
     * @see org.eclipse.datatools.connectivity.oda.IQuery#setNull(java.lang.String)
     */
    @Override
	public void setNull( String parameterName ) throws OdaException
    {
        // TODO Auto-generated method stub
        // only applies to named input parameter
    }

    /* (non-Javadoc)
     * @see org.eclipse.datatools.connectivity.oda.IQuery#setNull(int)
     */
    @Override
	public void setNull( int parameterId ) throws OdaException
    {
        // TODO Auto-generated method stub
        // only applies to input parameter
    }

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#findInParameter(java.lang.String)
	 */
	@Override
	public int findInParameter( String parameterName ) throws OdaException
	{
        // TODO Auto-generated method stub
		// only applies to named input parameter
		return 0;
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#getParameterMetaData()
	 */
	@Override
	public IParameterMetaData getParameterMetaData() throws OdaException
	{
        /* TODO Auto-generated method stub
         * Replace with implementation to return an instance 
         * based on this prepared query.
         */
		return new ParameterMetaData();
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#setSortSpec(org.eclipse.datatools.connectivity.oda.SortSpec)
	 */
	@Override
	public void setSortSpec( SortSpec sortBy ) throws OdaException
	{
		// only applies to sorting, assumes not supported
        throw new UnsupportedOperationException();
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#getSortSpec()
	 */
	@Override
	public SortSpec getSortSpec() throws OdaException
	{
		// only applies to sorting
		return null;
	}

    /* (non-Javadoc)
     * @see org.eclipse.datatools.connectivity.oda.IQuery#setSpecification(org.eclipse.datatools.connectivity.oda.spec.QuerySpecification)
     */
    @Override
	public void setSpecification( QuerySpecification querySpec )
            throws OdaException, UnsupportedOperationException
    {
        // assumes no support
        throw new UnsupportedOperationException();
    }

    /* (non-Javadoc)
     * @see org.eclipse.datatools.connectivity.oda.IQuery#getSpecification()
     */
    @Override
	public QuerySpecification getSpecification()
    {
        // assumes no support
        return null;
    }

    /* (non-Javadoc)
     * @see org.eclipse.datatools.connectivity.oda.IQuery#getEffectiveQueryText()
     */
    @Override
	public String getEffectiveQueryText()
    {
        // TODO Auto-generated method stub
        return m_preparedText;
    }

    /* (non-Javadoc)
     * @see org.eclipse.datatools.connectivity.oda.IQuery#cancel()
     */
    @Override
	public void cancel() throws OdaException, UnsupportedOperationException
    {
        // assumes unable to cancel while executing a query
        throw new UnsupportedOperationException();
    }
    
    
    
    public static class SimpleConstraintViolation implements ConstraintViolation<Object>
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
    
    private PropertyDescriptor getPropertyDescriptor(Class objClass, String property) throws IntrospectionException
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
    
	private void validateObject(Object object) throws BeanValidationException
	{
		validateObject(object, null);
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
