/*
 *************************************************************************
 * Copyright (c) 2014 <Kratos Integral Systems Europe>
 *  
 *************************************************************************
 */

package com.kratos.birt.report.data.oda.kairosdb.impl;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.TreeSet;

import org.eclipse.datatools.connectivity.oda.IBlob;
import org.eclipse.datatools.connectivity.oda.IClob;
import org.eclipse.datatools.connectivity.oda.IResultSet;
import org.eclipse.datatools.connectivity.oda.IResultSetMetaData;
import org.eclipse.datatools.connectivity.oda.OdaException;
import org.kairosdb.client.builder.DataFormatException;
import org.kairosdb.client.builder.DataPoint;
import org.kairosdb.client.response.GroupResult;
import org.kairosdb.client.response.QueryResponse;
import org.kairosdb.client.response.grouping.TimeGroupResult;
import org.kairosdb.client.response.grouping.ValueGroupResult;

import com.kratos.birt.report.data.oda.kairosdb.json.Duration;
import static com.kratos.birt.report.data.oda.kairosdb.impl.Query.*;
/**
 * Implementation class of IResultSet for an ODA runtime driver. <br>
 * For demo purpose, the auto-generated method stubs have hard-coded
 * implementation that returns a pre-defined set of meta-data and query results.
 * A custom ODA driver is expected to implement own data source specific
 * behavior in its place.
 */
public class ResultSet implements IResultSet {
	private int m_maxRows;
	private int m_currentRowId = -1;
	private QueryResponse response;
	private int currentQuery = 0;
	private int currentSeries = 0;
	private int indexInCurrentSeries = -1;
	private ArrayList<String> tagList;
	private ArrayList<Integer> valueList;
	private ArrayList<Duration> timeList;
	private ResultSetMetaData resultSetMeta;
	private boolean wasNull;
	private boolean displayMetricNameColumn;

	private HashMap<Integer, Integer> timeValueGroupMap;
	private boolean isTextOnly;

	/**
	 * @param response
	 * @param tagList
	 *            tag groups
	 * @param timeList
	 *            time groups
	 * @param valueList
	 *            value groups
	 * @param resultSetMetaData
	 *            No one knows why ResultSet must be able to return its meta
	 *            data. Will always be a mystery.
	 */
	public ResultSet(QueryResponse response, TreeSet<String> tagList,
			TreeSet<Duration> timeList, TreeSet<Integer> valueList,
			ResultSetMetaData resultSetMetaData, String valueType) {
		this.response = response;

		// wasNull is a marker that indicates when a getXXX encountered a null
		// value.
		this.wasNull = false;

		this.tagList = new ArrayList<String>(tagList);
		this.valueList = new ArrayList<Integer>(valueList);
		this.timeList = new ArrayList<Duration>(timeList);
		this.displayMetricNameColumn = response.getQueries().size() > 1;
		this.isTextOnly = valueType.equals(TEXT_ONLY_VALUE);
		this.resultSetMeta = resultSetMetaData;
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSet#getMetaData()
	 */
	@Override
	public IResultSetMetaData getMetaData() throws OdaException {
		System.out.println("------ getMetaData() from ResultSet ------");
		return resultSetMeta;

	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSet#setMaxRows(int)
	 */
	@Override
	public void setMaxRows(int max) throws OdaException {
		m_maxRows = max;
	}

	/**
	 * Returns the maximum number of rows that can be fetched from this result
	 * set.
	 * 
	 * @return the maximum number of rows to fetch.
	 */
	protected int getMaxRows() {
		return m_maxRows;
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSet#next()
	 */
	@Override
	public boolean next() throws OdaException {
		if (m_currentRowId < m_maxRows - 1) {
			m_currentRowId++;

			while (response.getQueries().get(currentQuery).getResults()
					.get(currentSeries).getDataPoints().size() == 0)
				switchSeries();

			if (indexInCurrentSeries < response.getQueries().get(currentQuery)
					.getResults().get(currentSeries).getDataPoints().size() - 1) {
				indexInCurrentSeries++;
			} else {
				indexInCurrentSeries = 0;
				do {
					switchSeries();
				} while (response.getQueries().get(currentQuery).getResults()
						.get(currentSeries).getDataPoints().size() == 0);

			}
			// if it is the first element of the series, generate the map for
			// time and value groups
			if (indexInCurrentSeries == 0)
				updateTimeValueMap();

			return true;
		}

		return false;
	}

	private void switchSeries() {
		if (currentSeries < response.getQueries().get(currentQuery)
				.getResults().size() - 1)
			currentSeries++;
		else {
			currentSeries = 0;
			currentQuery++;
		}
	}

	/**
	 * Creates a map of time and value groups at the beginning of each series
	 */
	private void updateTimeValueMap() {
		HashMap<Integer, Integer> newMap = new HashMap<Integer, Integer>();
		List<GroupResult> groupResults = response.getQueries()
				.get(currentQuery).getResults().get(currentSeries)
				.getGroupResults();
		if (groupResults == null) {
			timeValueGroupMap = newMap;
			return;
		}
		for (int i = tagList.size() + 1; i <= tagList.size() + valueList.size(); i++) {
			for (GroupResult groupResult : groupResults) {
				if (groupResult instanceof ValueGroupResult) {
					ValueGroupResult valueGroupResult = (ValueGroupResult) groupResult;
					if (valueGroupResult.getRangeSize() == valueList.get(i
							- tagList.size() - 1)) {
						newMap.put(i, valueGroupResult.getGroup()
								.getGroupNumber());
					}
				}
			}
		}
		for (int i = tagList.size() + valueList.size() + 1; i <= tagList.size()
				+ valueList.size() + timeList.size(); i++) {
			for (GroupResult groupResult : groupResults) {
				if (groupResult instanceof TimeGroupResult) {
					TimeGroupResult timeGroupResult = (TimeGroupResult) groupResult;
					if (timeGroupResult.getRangeSize().getValue() == timeList
							.get(i - tagList.size() - valueList.size() - 1)
							.getValue()
							&& timeGroupResult
									.getRangeSize()
									.getUnit()
									.equals(timeList
											.get(i - tagList.size()
													- valueList.size() - 1)
											.getUnit().name())) {
						newMap.put(i, timeGroupResult.getGroup()
								.getGroupNumber());
					}
				}
			}
		}
		timeValueGroupMap = newMap;
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSet#close()
	 */
	@Override
	public void close() throws OdaException {
		m_currentRowId = -1; // reset row counter
		currentSeries = 0;
		indexInCurrentSeries = -1;
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSet#getRow()
	 */
	@Override
	public int getRow() throws OdaException {
		return m_currentRowId;
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSet#getString(int)
	 */
	@Override
	public String getString(int index) throws OdaException {
		this.wasNull = false;
		if (displayMetricNameColumn) {
			index--;
			if (index == 0)
				return response.getQueries().get(currentQuery).getResults()
						.get(currentSeries).getName();
		}
		if (index <= tagList.size()) {
			List<String> tagValues = response.getQueries().get(currentQuery)
					.getResults().get(currentSeries).getTags()
					.get(tagList.get(index - 1));
			if (tagValues == null) {
				this.wasNull = true;
				return "";
			}
			if (tagValues.size() > 1)
				throw new OdaException("More than one tag value for '"
						+ tagList.get(index - 1) + "' despite grouping");
			return tagValues.get(0);
		} else if (index == tagList.size() + valueList.size() + timeList.size()
				+ 2) {
			try {
				DataPoint dp = response.getQueries().get(currentQuery).getResults()
						.get(currentSeries).getDataPoints().get(indexInCurrentSeries);

				if(isTextOnly && !(dp.getValue() instanceof String)){
					this.wasNull =true;
					return "";
				} else{
					return dp.stringValue();
				}
			} catch (DataFormatException e) {
				throw new OdaException("Illegal column index for getString");
			}
		}
		throw new OdaException("Illegal column index for getString");
	}

	/*
	 * @see
	 * org.eclipse.datatools.connectivity.oda.IResultSet#getString(java.lang
	 * .String)
	 */
	@Override
	public String getString(String columnName) throws OdaException {
		return getString(findColumn(columnName));
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSet#getInt(int)
	 */
	@Override
	public int getInt(int index) throws OdaException {
		if (displayMetricNameColumn)
			index--;
		this.wasNull = false;
		if (index > tagList.size()
				&& index <= tagList.size() + valueList.size() + timeList.size()) {
			Integer result = timeValueGroupMap.get(index);
			if (result != null)
				return result;
			else {
				this.wasNull = true;
				return 0;
			}
		}

		throw new OdaException("Illegal column index for getInt");
	}

	/*
	 * @see
	 * org.eclipse.datatools.connectivity.oda.IResultSet#getInt(java.lang.String
	 * )
	 */
	@Override
	public int getInt(String columnName) throws OdaException {
		return getInt(findColumn(columnName));
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSet#getDouble(int)
	 */
	@Override
	public double getDouble(int index) throws OdaException {
		this.wasNull = false;
		try {
			return response.getQueries().get(currentQuery).getResults()
					.get(currentSeries).getDataPoints()
					.get(indexInCurrentSeries).doubleValue();
		} catch (DataFormatException e) {
			this.wasNull = true;
			return 0;
		}
	}

	/*
	 * @see
	 * org.eclipse.datatools.connectivity.oda.IResultSet#getDouble(java.lang
	 * .String)
	 */
	@Override
	public double getDouble(String columnName) throws OdaException {
		return getDouble(findColumn(columnName));
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSet#getBigDecimal(int)
	 */
	@Override
	public BigDecimal getBigDecimal(int index) throws OdaException {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	/*
	 * @see
	 * org.eclipse.datatools.connectivity.oda.IResultSet#getBigDecimal(java.
	 * lang.String)
	 */
	@Override
	public BigDecimal getBigDecimal(String columnName) throws OdaException {
		return getBigDecimal(findColumn(columnName));
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSet#getDate(int)
	 */
	@Override
	public Date getDate(int index) throws OdaException {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	/*
	 * @see
	 * org.eclipse.datatools.connectivity.oda.IResultSet#getDate(java.lang.String
	 * )
	 */
	@Override
	public Date getDate(String columnName) throws OdaException {
		return getDate(findColumn(columnName));
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSet#getTime(int)
	 */
	@Override
	public Time getTime(int index) throws OdaException {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	/*
	 * @see
	 * org.eclipse.datatools.connectivity.oda.IResultSet#getTime(java.lang.String
	 * )
	 */
	@Override
	public Time getTime(String columnName) throws OdaException {
		return getTime(findColumn(columnName));
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSet#getTimestamp(int)
	 */
	@Override
	public Timestamp getTimestamp(int index) throws OdaException {
		this.wasNull = false;
		return new Timestamp(response.getQueries().get(currentQuery)
				.getResults().get(currentSeries).getDataPoints()
				.get(indexInCurrentSeries).getTimestamp());
	}

	/*
	 * @see
	 * org.eclipse.datatools.connectivity.oda.IResultSet#getTimestamp(java.lang
	 * .String)
	 */
	@Override
	public Timestamp getTimestamp(String columnName) throws OdaException {
		return getTimestamp(findColumn(columnName));
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSet#getBlob(int)
	 */
	@Override
	public IBlob getBlob(int index) throws OdaException {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	/*
	 * @see
	 * org.eclipse.datatools.connectivity.oda.IResultSet#getBlob(java.lang.String
	 * )
	 */
	@Override
	public IBlob getBlob(String columnName) throws OdaException {
		return getBlob(findColumn(columnName));
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSet#getClob(int)
	 */
	@Override
	public IClob getClob(int index) throws OdaException {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	/*
	 * @see
	 * org.eclipse.datatools.connectivity.oda.IResultSet#getClob(java.lang.String
	 * )
	 */
	@Override
	public IClob getClob(String columnName) throws OdaException {
		return getClob(findColumn(columnName));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.eclipse.datatools.connectivity.oda.IResultSet#getBoolean(int)
	 */
	@Override
	public boolean getBoolean(int index) throws OdaException {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.eclipse.datatools.connectivity.oda.IResultSet#getBoolean(java.lang
	 * .String)
	 */
	@Override
	public boolean getBoolean(String columnName) throws OdaException {
		return getBoolean(findColumn(columnName));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.eclipse.datatools.connectivity.oda.IResultSet#getObject(int)
	 */
	@Override
	public Object getObject(int index) throws OdaException {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.eclipse.datatools.connectivity.oda.IResultSet#getObject(java.lang
	 * .String)
	 */
	@Override
	public Object getObject(String columnName) throws OdaException {
		return getObject(findColumn(columnName));
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSet#wasNull()
	 */
	@Override
	public boolean wasNull() throws OdaException {
		return wasNull;
	}

	@Override
	public int findColumn(String columnName) throws OdaException {
		System.out.println("ResultSet: find column");
		int offset = 0;
		if (displayMetricNameColumn)
			offset = 1;
		System.out.println("findColumn called");
		int totalLength = tagList.size() + valueList.size() + timeList.size();
		if (columnName.equals(ResultSetMetaData.TIMESTAMP)) {
			return totalLength + 1 + offset;
		} else if (columnName.equals(ResultSetMetaData.VALUE)) {
			return totalLength + 2 + offset;
		} else if (columnName.startsWith("tag")) {
			return tagList.indexOf(columnName.substring(4)) + 1 + offset;
		} else if (columnName.startsWith("value")
				|| columnName.startsWith("time")) {
			throw new OdaException(
					"FindColumn not implemented for value and time columns"); // +offset
		}
		throw new OdaException("Column not found");
	}

}
