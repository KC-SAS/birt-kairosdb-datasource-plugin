/*
 *************************************************************************
 * Copyright (c) 2014 <Kratos Integral Systems Europe>
 *  
 *************************************************************************
 */

package com.kratos.birt.report.data.oda.kairosdb.impl;

import java.util.ArrayList;
import java.util.TreeSet;

import org.eclipse.datatools.connectivity.oda.IResultSetMetaData;
import org.eclipse.datatools.connectivity.oda.OdaException;

import com.kratos.birt.report.data.oda.kairosdb.json.Duration;

/**
 * Implementation class of IResultSetMetaData for an ODA runtime driver.
 * <br>
 * For demo purpose, the auto-generated method stubs have
 * hard-coded implementation that returns a pre-defined set
 * of meta-data and query results.
 * A custom ODA driver is expected to implement own data source specific
 * behavior in its place. 
 */
public class ResultSetMetaData implements IResultSetMetaData
{
	public static final String TIMESTAMP = "Timestamp";
	public static final String VALUE = "Values";
	
    private ArrayList<String> tagList;
    private ArrayList<Integer> valueList;
    private ArrayList<Duration> timeList;
    private boolean displayMetricNameColumn;
    private boolean isText;
	
    
	public ResultSetMetaData(TreeSet<String> tagList,TreeSet<Duration> timeList,TreeSet<Integer> valueList, boolean displayMetricName){
		this.tagList = new ArrayList<String>(tagList);
		this.valueList = new ArrayList<Integer>(valueList);
		this.timeList = new ArrayList<Duration>(timeList);
		this.displayMetricNameColumn = displayMetricName;
		this.isText = false;
	}
	
	public void setValueAsText(boolean isText){
		this.isText = isText;
	}

    
	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSetMetaData#getColumnCount()
	 */
	@Override
	public int getColumnCount() throws OdaException
	{
		if(displayMetricNameColumn)
			return 3+tagList.size()+valueList.size()+timeList.size();
        return 2+tagList.size()+valueList.size()+timeList.size();
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSetMetaData#getColumnName(int)
	 */
	@Override
	public String getColumnName( int index ) throws OdaException
	{
		if(displayMetricNameColumn){
			index--;
			if(index==0)
				return "MetricName";
		}
		int groupingSize = tagList.size()+valueList.size()+timeList.size();
        if(index<=tagList.size()){
        	return "tag:"+tagList.get(index-1);
        } else if(index <= tagList.size() + valueList.size()){
        	return "value:"+valueList.get(index-tagList.size()-1);
        } else if(index <= groupingSize){
        	Duration duration = timeList.get(index-tagList.size()-valueList.size()-1);
        	return "time:"+duration.getValue()+duration.getUnit().toString();
        } else if(index == groupingSize+1){
        	return TIMESTAMP;
        }
        else if(index == groupingSize+2){
        	return VALUE;
        }
        throw new OdaException("Illegal column index for getColumnName");
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSetMetaData#getColumnLabel(int)
	 */
	@Override
	public String getColumnLabel( int index ) throws OdaException
	{
		if(displayMetricNameColumn){
			index--;
			if(index==0)
				return "Metric Name";
		}
		int groupingSize = tagList.size()+valueList.size()+timeList.size();
        if(index<=tagList.size()){
        	return tagList.get(index-1);
        } else if(index <= tagList.size() + valueList.size()){
        	return valueList.get(index-tagList.size()-1)+" range value group";
        } else if(index <= groupingSize){
        	Duration duration = timeList.get(index-tagList.size()-valueList.size()-1);
        	return duration.getValue()+duration.getUnit().toString()+" time group";
        } else if(index == groupingSize+1){
        	return TIMESTAMP;
        }
        else if(index == groupingSize+2){
        	return VALUE;
        }
        throw new OdaException("Illegal column index for getColumnLabel");
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSetMetaData#getColumnType(int)
	 */
	@Override
	public int getColumnType( int index ) throws OdaException
	{
		if(displayMetricNameColumn){
			index--;
		}
		
		int groupingSize = tagList.size()+valueList.size()+timeList.size();
        if(index<=tagList.size()){
        	return java.sql.Types.VARCHAR;
        }
        else if(index <= groupingSize){
        	return java.sql.Types.INTEGER;
        }
        else if(index == groupingSize+1){
        	return java.sql.Types.TIMESTAMP;
        }
        else if(index == groupingSize+2){
        	if(isText)
        		return java.sql.Types.VARCHAR;
        	else
        		return java.sql.Types.DOUBLE;
        }
        else
        	throw new OdaException("Illegal column index for getColumnType");
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSetMetaData#getColumnTypeName(int)
	 */
	@Override
	public String getColumnTypeName( int index ) throws OdaException
	{
        int nativeTypeCode = getColumnType( index );
        return Driver.getNativeDataTypeName( nativeTypeCode );
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSetMetaData#getColumnDisplayLength(int)
	 */
	@Override
	public int getColumnDisplayLength( int index ) throws OdaException
	{
        // TODO replace with data source specific implementation

        // hard-coded for demo purpose
		return 8;
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSetMetaData#getPrecision(int)
	 */
	@Override
	public int getPrecision( int index ) throws OdaException
	{
        // TODO Auto-generated method stub
		return -1;
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSetMetaData#getScale(int)
	 */
	@Override
	public int getScale( int index ) throws OdaException
	{
        // TODO Auto-generated method stub
		return -1;
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSetMetaData#isNullable(int)
	 */
	@Override
	public int isNullable( int index ) throws OdaException
	{
		if(displayMetricNameColumn){
			index--;
		}
		if(index == tagList.size()+valueList.size()+timeList.size()+1){
        	return IResultSetMetaData.columnNoNulls;
        }
		return IResultSetMetaData.columnNullable;
	}
    
}
