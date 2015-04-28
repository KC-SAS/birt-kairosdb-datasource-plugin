/*
 *************************************************************************
 * Copyright (c) 2014 <Kratos Integral Systems Europe>
 *  
 *************************************************************************
 */

package com.kratos.birt.report.data.oda.kairosdb.impl;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.util.List;
import java.util.TreeSet;

import org.eclipse.datatools.connectivity.oda.IParameterMetaData;
import org.eclipse.datatools.connectivity.oda.IQuery;
import org.eclipse.datatools.connectivity.oda.IResultSet;
import org.eclipse.datatools.connectivity.oda.IResultSetMetaData;
import org.eclipse.datatools.connectivity.oda.OdaException;
import org.eclipse.datatools.connectivity.oda.SortSpec;
import org.eclipse.datatools.connectivity.oda.spec.QuerySpecification;
import org.kairosdb.client.HttpClient;
import org.kairosdb.client.response.Queries;
import org.kairosdb.client.response.QueryResponse;
import org.kairosdb.client.response.Results;
import org.kairosdb.client.util.ResponseSizeException;

import com.google.gson.JsonIOException;
import com.joestelmach.natty.DateGroup;
import com.joestelmach.natty.Parser;
import com.kratos.birt.report.data.oda.kairosdb.json.Duration;
import com.kratos.birt.report.data.oda.kairosdb.json.QueryParser;
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
	public static final String START_TIME_PARAMETER_NAME = "startTimeParameterName";
	public static final String END_TIME_PARAMETER_NAME = "endTimeParameterName";
	public static final String VALUE_TYPE = "valueType";

	public static final String TEXT_ONLY_VALUE = "textOnlyValue";
	public static final String NUMBER_ONLY_VALUE = "numberOnlyValue";
	public static final String EVERYTHING_TEXT_VALUE = "everythingTextValue";	

	private int m_maxRows;
	private String m_preparedText;
	private String hostName;

	// These sets represent group bys
	private TreeSet<String> tagList;
	private TreeSet<Integer> valueList;
	private TreeSet<Duration> timeList;

	private boolean displayMetricNameColumn;
	private QueryParser parser;
	private ResultSetMetaData resultSetMetaData;
	private String startTimeParameterName;
	private String endTimeParameterName;
	private double maxSizeMB;
	private HttpClient client;

	private String valueType;


	public Query(String hostName, double maxSizeMB) {
		this.hostName = hostName;
		this.parser = new QueryParser();
		this.maxSizeMB = maxSizeMB;
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

		displayMetricNameColumn = parser.displayMetricColumn(queryText);
		m_preparedText = queryText;
		parser.parse(queryText, tagList, valueList, timeList);
		resultSetMetaData = new ResultSetMetaData(tagList,timeList,valueList,displayMetricNameColumn);
	}


	//	public void setTimeParameters(String startTimeParameterName, String endTimeParameterName){
	//		this.startTimeParameterName = startTimeParameterName;
	//		this.endTimeParameterName = endTimeParameterName;
	//	}
	//	
	//	public void setTypeAsNumber(boolean isNumber){
	//		resultSetMetaData.setValueAsText(!isNumber);
	//	}


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
		return resultSetMetaData;
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
				///response = client.query(m_preparedText,(long)(maxSizeMB*1000000.));
				response =client.query(m_preparedText,(long)(maxSizeMB*1000000.));
			} 
			client.shutdown();
		} catch (MalformedURLException e) {
			throw new OdaException(e);
		} catch (URISyntaxException e) {
			throw new OdaException(e);
		}  catch (JsonIOException e){
			if(e.getCause() instanceof ResponseSizeException)
				throw new OdaException("The reponse is larger than the limit set by the datasource ("+maxSizeMB+"MB)");
			else
				throw new OdaException(e);
		}
		catch (IOException e) {
			throw new OdaException(e);
		}

		int nbFetched = 0;
		for(Queries query:response.getQueries()){
			for(Results result : query.getResults()){	
				nbFetched += result.getDataPoints().size();
				//				if(nbFetched>MAX_NUMBER_POINT)
				//					throw new OdaException("Error: this query is too big, it represents more than "+MAX_NUMBER_POINT+" points.");
			}
		}		
		IResultSet resultSet = new ResultSet(response,tagList,timeList,valueList, resultSetMetaData, valueType);

		resultSet.setMaxRows(nbFetched);
		return resultSet;
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#setProperty(java.lang.String, java.lang.String)
	 */
	@Override
	public void setProperty( String name, String value ) throws OdaException
	{
		if(name.equals(START_TIME_PARAMETER_NAME)){
			this.startTimeParameterName = value;
		} else if(name.equals(END_TIME_PARAMETER_NAME)){
			this.endTimeParameterName = value;
		} else if(name.equals(VALUE_TYPE)){
			this.resultSetMetaData.setValueAsText(!value.equals(NUMBER_ONLY_VALUE));
			valueType = value;
		}
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
		Parser parser = new Parser();
		List<DateGroup> groups = parser.parse(value);
		if(groups.size()!=1)
			throw new OdaException("could not parse date");
		
		DateGroup dateGroup = groups.get(0);
		if(dateGroup.getDates().size()!=1)
			throw new OdaException("could not parse date");
		setTimestamp(parameterName, new Timestamp(dateGroup.getDates().get(0).getTime()));
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#setString(int, java.lang.String)
	 */
	@Override
	public void setString( int parameterId, String value ) throws OdaException
	{
		Parser parser = new Parser();
		List<DateGroup> groups = parser.parse(value);
		if(groups.size()!=1)
			throw new OdaException("could not parse date");
		
		DateGroup dateGroup = groups.get(0);
		if(dateGroup.getDates().size()!=1)
			throw new OdaException("could not parse date");
		setTimestamp(parameterId, new Timestamp(dateGroup.getDates().get(0).getTime()));
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
		DateFormat dateFormatter = DateFormat.getDateTimeInstance();
		System.out.println("parameter"+parameterId+": "+dateFormatter.format(new Date(value.getTime())));
		if(parameterId==1){
			try {
				m_preparedText = parser.changeStartDate(m_preparedText, value.getTime());
			} catch (IOException e) {
				throw new OdaException("Could not use parameter to set start date");
			}
		}
		else if(parameterId==2){
			try{
				m_preparedText = parser.changeEndDate(m_preparedText, value.getTime());
			} catch (IOException e) {
				throw new OdaException("Could not use parameter to set start date");
			}
		}
		else
			throw new OdaException("Unknown timestamp parameter");
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
		// not allowed
	}

	/* (non-Javadoc)
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#setNull(int)
	 */
	@Override
	public void setNull( int parameterId ) throws OdaException
	{
		// not allowed
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#findInParameter(java.lang.String)
	 */
	@Override
	public int findInParameter( String parameterName ) throws OdaException
	{
		if(parameterName.equals(startTimeParameterName))
			return 1;
		if(parameterName.equals(endTimeParameterName))
			return 2;
		else
			return 0;
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#getParameterMetaData()
	 */
	@Override
	public IParameterMetaData getParameterMetaData() throws OdaException
	{
		System.out.println("getParameterMetaData");
		return new ParameterMetaData(startTimeParameterName,endTimeParameterName);
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
		System.out.println("cancel()");
		try {
			client.shutdown();
		} catch (IOException e) {
			throw new OdaException();
		}
	}






}
