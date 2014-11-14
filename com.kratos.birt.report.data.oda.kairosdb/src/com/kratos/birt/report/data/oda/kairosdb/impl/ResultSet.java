/*
 *************************************************************************
 * Copyright (c) 2014 <<Your Company Name here>>
 *  
 *************************************************************************
 */

package com.kratos.birt.report.data.oda.kairosdb.impl;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.TreeSet;

import org.eclipse.datatools.connectivity.oda.IBlob;
import org.eclipse.datatools.connectivity.oda.IClob;
import org.eclipse.datatools.connectivity.oda.IResultSet;
import org.eclipse.datatools.connectivity.oda.IResultSetMetaData;
import org.eclipse.datatools.connectivity.oda.OdaException;
import org.kairosdb.client.builder.DataFormatException;
import org.kairosdb.client.response.QueryResponse;

import com.kratos.birt.report.data.oda.kairosdb.util.Duration;

/**
 * Implementation class of IResultSet for an ODA runtime driver.
 * <br>
 * For demo purpose, the auto-generated method stubs have
 * hard-coded implementation that returns a pre-defined set
 * of meta-data and query results.
 * A custom ODA driver is expected to implement own data source specific
 * behavior in its place. 
 */
public class ResultSet implements IResultSet
{
	private int m_maxRows;
    private int m_currentRowId = -1;
    private QueryResponse response;
    private int currentSeries = 0;
    private int indexInCurrentSeries = -1;
    private ArrayList<String> tagList;
    private ArrayList<Integer> valueList;
    private ArrayList<Duration> timeList;
    private ResultSetMetaData resultSetMeta;
	
    public ResultSet(QueryResponse response,TreeSet<String> tagList,TreeSet<Duration> timeList,TreeSet<Integer> valueList){
    	this.response = response;
    	
		this.tagList = new ArrayList<String>(tagList);
		this.valueList = new ArrayList<Integer>(valueList);
		this.timeList = new ArrayList<Duration>(timeList);	
		
		resultSetMeta = new ResultSetMetaData(tagList, timeList, valueList);
    }
    
	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSet#getMetaData()
	 */
	@Override
	public IResultSetMetaData getMetaData() throws OdaException
	{
		
		return resultSetMeta;
		
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSet#setMaxRows(int)
	 */
	@Override
	public void setMaxRows( int max ) throws OdaException
	{
		m_maxRows = max;
	}
	
	/**
	 * Returns the maximum number of rows that can be fetched from this result set.
	 * @return the maximum number of rows to fetch.
	 */
	protected int getMaxRows()
	{
		return m_maxRows;
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSet#next()
	 */
	@Override
	public boolean next() throws OdaException
	{
		// TODO replace with data source specific implementation
        
        
        if( m_currentRowId < m_maxRows-1 )
        {
            m_currentRowId++;
            while(response.getQueries().get(0).getResults().get(currentSeries).getDataPoints().size()==0){
            	currentSeries++;
            }
            if(indexInCurrentSeries<response.getQueries().get(0).getResults().get(currentSeries).getDataPoints().size()-1){
            	indexInCurrentSeries++;
            } else {
            	indexInCurrentSeries = 0;
                do { 
                	currentSeries++;
                } while(response.getQueries().get(0).getResults().get(currentSeries).getDataPoints().size()==0);

            }
            return true;
        }
        
        return false;        
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSet#close()
	 */
	@Override
	public void close() throws OdaException
	{   
        m_currentRowId = -1;     // reset row counter
        currentSeries = 0;
        indexInCurrentSeries = -1;
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSet#getRow()
	 */
	@Override
	public int getRow() throws OdaException
	{
		return m_currentRowId;
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSet#getString(int)
	 */
	@Override
	public String getString( int index ) throws OdaException
	{
		if(index<=tagList.size()){
			return response.getQueries().get(0).getResults().get(currentSeries).getTags().get(tagList.get(index-1)).get(0);
		}
        throw new OdaException("Illegal column index for getString");
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSet#getString(java.lang.String)
	 */
	@Override
	public String getString( String columnName ) throws OdaException
	{
	    return getString( findColumn( columnName ) );
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSet#getInt(int)
	 */
	@Override
	public int getInt( int index ) throws OdaException
	{
        // TODO replace with data source specific implementation
        
        // hard-coded for demo purpose
        return getRow();
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSet#getInt(java.lang.String)
	 */
	@Override
	public int getInt( String columnName ) throws OdaException
	{
	    return getInt( findColumn( columnName ) );
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSet#getDouble(int)
	 */
	@Override
	public double getDouble( int index ) throws OdaException
	{
        try {
			return response.getQueries().get(0).getResults().get(currentSeries).getDataPoints().get(indexInCurrentSeries).doubleValue();
		} catch (DataFormatException e) {
			throw new OdaException("Illegal column index for getDouble");
		}
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSet#getDouble(java.lang.String)
	 */
	@Override
	public double getDouble( String columnName ) throws OdaException
	{
	    return getDouble( findColumn( columnName ) );
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSet#getBigDecimal(int)
	 */
	@Override
	public BigDecimal getBigDecimal( int index ) throws OdaException
	{
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSet#getBigDecimal(java.lang.String)
	 */
	@Override
	public BigDecimal getBigDecimal( String columnName ) throws OdaException
	{
	    return getBigDecimal( findColumn( columnName ) );
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSet#getDate(int)
	 */
	@Override
	public Date getDate( int index ) throws OdaException
	{
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSet#getDate(java.lang.String)
	 */
	@Override
	public Date getDate( String columnName ) throws OdaException
	{
	    return getDate( findColumn( columnName ) );
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSet#getTime(int)
	 */
	@Override
	public Time getTime( int index ) throws OdaException
	{
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSet#getTime(java.lang.String)
	 */
	@Override
	public Time getTime( String columnName ) throws OdaException
	{
	    return getTime( findColumn( columnName ) );
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSet#getTimestamp(int)
	 */
	@Override
	public Timestamp getTimestamp( int index ) throws OdaException
	{
        return new Timestamp(response.getQueries().get(0).getResults().get(currentSeries).getDataPoints().get(indexInCurrentSeries).getTimestamp());
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSet#getTimestamp(java.lang.String)
	 */
	@Override
	public Timestamp getTimestamp( String columnName ) throws OdaException
	{
	    return getTimestamp( findColumn( columnName ) );
	}

    /* 
     * @see org.eclipse.datatools.connectivity.oda.IResultSet#getBlob(int)
     */
    @Override
	public IBlob getBlob( int index ) throws OdaException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }

    /* 
     * @see org.eclipse.datatools.connectivity.oda.IResultSet#getBlob(java.lang.String)
     */
    @Override
	public IBlob getBlob( String columnName ) throws OdaException
    {
        return getBlob( findColumn( columnName ) );
    }

    /* 
     * @see org.eclipse.datatools.connectivity.oda.IResultSet#getClob(int)
     */
    @Override
	public IClob getClob( int index ) throws OdaException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }

    /* 
     * @see org.eclipse.datatools.connectivity.oda.IResultSet#getClob(java.lang.String)
     */
    @Override
	public IClob getClob( String columnName ) throws OdaException
    {
        return getClob( findColumn( columnName ) );
    }

    /* (non-Javadoc)
     * @see org.eclipse.datatools.connectivity.oda.IResultSet#getBoolean(int)
     */
    @Override
	public boolean getBoolean( int index ) throws OdaException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }

    /* (non-Javadoc)
     * @see org.eclipse.datatools.connectivity.oda.IResultSet#getBoolean(java.lang.String)
     */
    @Override
	public boolean getBoolean( String columnName ) throws OdaException
    {
        return getBoolean( findColumn( columnName ) );
    }
    
    /* (non-Javadoc)
     * @see org.eclipse.datatools.connectivity.oda.IResultSet#getObject(int)
     */
    @Override
	public Object getObject( int index ) throws OdaException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }

    /* (non-Javadoc)
     * @see org.eclipse.datatools.connectivity.oda.IResultSet#getObject(java.lang.String)
     */
    @Override
	public Object getObject( String columnName ) throws OdaException
    {
        return getObject( findColumn( columnName ) );
    }

    /*
     * @see org.eclipse.datatools.connectivity.oda.IResultSet#wasNull()
     */
    @Override
	public boolean wasNull() throws OdaException
    {
        // TODO Auto-generated method stub
        
        // hard-coded for demo purpose
        return false;
    }


    @Override
	public int findColumn( String columnName ) throws OdaException
    {

    	if(columnName.equals(ResultSetMetaData.TIMESTAMP)){
    		return tagList.size()+1;
    	}
    	else if(columnName.equals(ResultSetMetaData.VALUE)){
    		return tagList.size()+2;
    	}
    	else if(tagList.contains(columnName)){
    		return tagList.indexOf(columnName)+1;
    	}
    	throw new OdaException("Column not found");
    }
    
}
