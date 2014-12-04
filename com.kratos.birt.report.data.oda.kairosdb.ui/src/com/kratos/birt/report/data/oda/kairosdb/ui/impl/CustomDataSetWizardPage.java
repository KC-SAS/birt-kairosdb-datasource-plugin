/*
 *************************************************************************
 * Copyright (c) 2014 <<Your Company Name here>>
 *  
 *************************************************************************
 */

package com.kratos.birt.report.data.oda.kairosdb.ui.impl;

import static com.kratos.birt.report.data.oda.kairosdb.impl.Query.END_TIME_PARAMETER_NAME;
import static com.kratos.birt.report.data.oda.kairosdb.impl.Query.START_TIME_PARAMETER_NAME;
import static com.kratos.birt.report.data.oda.kairosdb.impl.Query.VALUE_TYPE;

import java.io.IOException;

import javafx.application.Platform;
import javafx.beans.value.ChangeListener;
import javafx.beans.value.ObservableValue;
import javafx.embed.swt.FXCanvas;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;

import org.eclipse.datatools.connectivity.oda.IConnection;
import org.eclipse.datatools.connectivity.oda.IDriver;
import org.eclipse.datatools.connectivity.oda.IParameterMetaData;
import org.eclipse.datatools.connectivity.oda.IQuery;
import org.eclipse.datatools.connectivity.oda.IResultSetMetaData;
import org.eclipse.datatools.connectivity.oda.OdaException;
import org.eclipse.datatools.connectivity.oda.design.DataSetDesign;
import org.eclipse.datatools.connectivity.oda.design.DataSetParameters;
import org.eclipse.datatools.connectivity.oda.design.DesignFactory;
import org.eclipse.datatools.connectivity.oda.design.ParameterDefinition;
import org.eclipse.datatools.connectivity.oda.design.ResultSetColumns;
import org.eclipse.datatools.connectivity.oda.design.ResultSetDefinition;
import org.eclipse.datatools.connectivity.oda.design.ui.designsession.DesignSessionUtil;
import org.eclipse.datatools.connectivity.oda.design.ui.wizards.DataSetWizardPage;
import org.eclipse.datatools.connectivity.oda.design.util.DesignUtil;
import org.eclipse.jface.resource.ImageDescriptor;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;

import com.kratos.birt.report.data.oda.kairosdb.json.QueryParser;
import com.kratos.birt.report.data.oda.kairosdb.ui.FXMLController;

/**
 * Auto-generated implementation of an ODA data set designer page
 * for an user to create or edit an ODA data set design instance.
 * This custom page provides a simple Query Text control for user input.  
 * It further extends the DTP design-time framework to update
 * an ODA data set design instance based on the query's derived meta-data.
 * <br>
 * A custom ODA designer is expected to change this exemplary implementation 
 * as appropriate. 
 */
public class CustomDataSetWizardPage extends DataSetWizardPage
{	
	private static String DEFAULT_MESSAGE = "Define the query for the data set";

	private transient FXMLController controller;
	private transient QueryParser parser;
	

	/**
	 * Constructor
	 * @param pageName
	 */
	public CustomDataSetWizardPage( String pageName )
	{
		super( pageName );
		setTitle( pageName );
		setMessage( DEFAULT_MESSAGE );
		parser = new QueryParser();
	}
	

	/**
	 * Constructor
	 * @param pageName
	 * @param title
	 * @param titleImage
	 */
	public CustomDataSetWizardPage( String pageName, String title,
			ImageDescriptor titleImage )
	{
		super( pageName, title, titleImage );
		setMessage( DEFAULT_MESSAGE );
		parser = new QueryParser();
	}

	/* (non-Javadoc)
	 * @see org.eclipse.datatools.connectivity.oda.design.ui.wizards.DataSetWizardPage#createPageCustomControl(org.eclipse.swt.widgets.Composite)
	 */
	public void createPageCustomControl( Composite parent )
	{
		try {
			setControl( createPageControl( parent ) );
		} catch (IOException e) {
			e.printStackTrace();
			throw new IllegalArgumentException("Could not create query builder control");
		}
		initializeControl();
	}

	/**
	 * Creates custom control for user-defined query text.
	 * @throws IOException 
	 */
	private Control createPageControl( Composite parent ) throws IOException
	{
		
		Composite composite = new Composite( parent, SWT.NONE );
		composite.setLayout( new GridLayout( 1, false ) );
//		GridData gridData = new GridData( GridData.HORIZONTAL_ALIGN_FILL
//				| GridData.VERTICAL_ALIGN_FILL );
//		composite.setLayoutData( gridData );

		FXCanvas fxCanvas = new FXCanvas(composite, SWT.NONE);
		
		FXMLLoader loader = new FXMLLoader(getClass().getResource("/Scene.fxml"));
		Parent root = (Parent) loader.load();
		
		Scene scene = new Scene(root);
		fxCanvas.setScene(scene);

		Platform.setImplicitExit(false);
		GridData webData = new GridData( GridData.FILL_HORIZONTAL | GridData.FILL_VERTICAL);
		fxCanvas.setLayoutData( webData );
		controller = ((FXMLController) loader.getController());
		controller.addQueryAreaChangeListener(new ChangeListener<String>() {
		    @Override
		    public void changed(ObservableValue<? extends String> observable,
		            String oldValue, String newValue) {
		    	validateData();
		    }
		});
		
		try {
			java.util.Properties connProps = 
					DesignSessionUtil.getEffectiveDataSourceProperties( 
							getInitializationDesign().getDataSourceDesign() );
			String hostName = connProps.getProperty("hostName");
			String port = connProps.getProperty("port");
			controller.setSearchFieldText("http://"+hostName+":"+port);
			controller.search();
			
		} catch (OdaException e) {
			e.printStackTrace();
		}
		
		setPageComplete( false );
		return composite;
	}


	/**
	 * Initializes the page control with the last edited data set design.
	 */
	private void initializeControl( )
	{
		/* 
		 * To optionally restore the designer state of the previous design session, use
		 *      getInitializationDesignerState(); 
		 */

		// Restores the last saved data set design
		DataSetDesign dataSetDesign = getInitializationDesign();
		if( dataSetDesign == null || dataSetDesign.getQueryText() == null || dataSetDesign.getQueryText().equals("") ){
			controller.setQueryBuilderAsDefault();
			return; // nothing to initialize
		}

		if(dataSetDesign.getPrivateProperties()!=null &&
				dataSetDesign.getPrivateProperties().getProperty(START_TIME_PARAMETER_NAME)!=null &&
				dataSetDesign.getPrivateProperties().getProperty(END_TIME_PARAMETER_NAME)!=null){
			controller.setTimeParameter(true);
			controller.setStartTimeParameterName(dataSetDesign.getPrivateProperties().getProperty(START_TIME_PARAMETER_NAME));
			controller.setStopTimeParameterName(dataSetDesign.getPrivateProperties().getProperty(END_TIME_PARAMETER_NAME));
		}
		
		if(dataSetDesign.getPrivateProperties()!=null && dataSetDesign.getPrivateProperties().getProperty(VALUE_TYPE) != null ){
			controller.setValueType(dataSetDesign.getPrivateProperties().getProperty(VALUE_TYPE));
		}
		
		// initialize control
		controller.setQueryAreaText( dataSetDesign.getQueryText() );
		controller.setRawQueryAsDefault();
		validateData();
		setMessage( DEFAULT_MESSAGE );

		/*
		 * To optionally honor the request for an editable or
		 * read-only design session, use
		 *      isSessionEditable();
		 */
	}

	/**
	 * Obtains the user-defined query text of this data set from page control.
	 * @return query text
	 */
	private String getQueryText( )
	{
		return controller.getQueryAreaText();
	}

	/*
	 * (non-Javadoc)
	 * @see org.eclipse.datatools.connectivity.oda.design.ui.wizards.DataSetWizardPage#collectDataSetDesign(org.eclipse.datatools.connectivity.oda.design.DataSetDesign)
	 */
	protected DataSetDesign collectDataSetDesign( DataSetDesign design )
	{
		System.out.println("collectDataSetDesign( )");
		if( getControl() == null )     // page control was never created
			return design;             // no editing was done
		if( ! hasValidData() )
			return null;    // to trigger a design session error status
		savePage( design );
		return design;
	}

	/*
	 * (non-Javadoc)
	 * @see org.eclipse.datatools.connectivity.oda.design.ui.wizards.DataSetWizardPage#collectResponseState()
	 */
	protected void collectResponseState( )
	{
		super.collectResponseState( );
		/*
		 * To optionally assign a custom response state, for inclusion in the ODA
		 * design session response, use 
		 *      setResponseSessionStatus( SessionStatus status );
		 *      setResponseDesignerState( DesignerState customState );
		 */
	}

	/*
	 * (non-Javadoc)
	 * @see org.eclipse.datatools.connectivity.oda.design.ui.wizards.DataSetWizardPage#canLeave()
	 */
	protected boolean canLeave( )
	{
		return isPageComplete();
	}

	/**
	 * Validates the user-defined value in the page control exists
	 * and not a blank text.
	 * Set page message accordingly.
	 */
	private void validateData( )
	{
		boolean isValid = (getQueryText() != null && getQueryText().trim().length() > 0 );

		if( !isValid )
			setMessage( "Requires input value.", ERROR );
		else if(!parser.validateJson(getQueryText()))
		{
			setMessage( "Invalid Json.", ERROR );
			isValid = false;
		}
		else if(!parser.validateQuery(getQueryText())){
			isValid = false;
			setMessage( "Not a valid query.", ERROR );
		}
		if(isValid)
			setMessage(DEFAULT_MESSAGE);
		setPageComplete( isValid );
	}

	/**
	 * Indicates whether the custom page has valid data to proceed 
	 * with defining a data set.
	 */
	private boolean hasValidData( )
	{
		validateData( );

		return canLeave();
	}

	/**
	 * Saves the user-defined value in this page, and updates the specified 
	 * dataSetDesign with the latest design definition.
	 */
	private void savePage( DataSetDesign dataSetDesign )
	{
		// save user-defined query text
		String queryText = getQueryText();
		dataSetDesign.setQueryText( queryText );
		java.util.Properties properties = new java.util.Properties();
		if(dataSetDesign.getPrivateProperties()==null){
			dataSetDesign.setPrivateProperties(DesignUtil.convertToDesignProperties(properties));
		}
		dataSetDesign.getPrivateProperties().setProperty(START_TIME_PARAMETER_NAME, controller.getStartTimeParameterName());
		dataSetDesign.getPrivateProperties().setProperty(END_TIME_PARAMETER_NAME, controller.getEndTimeParameterName());
		dataSetDesign.getPrivateProperties().setProperty(VALUE_TYPE, controller.getValueType());
		// obtain query's current runtime metadata, and maps it to the dataSetDesign
		IConnection customConn = null;
		try
		{
			// instantiate your custom ODA runtime driver class
			/* Note: You may need to manually update your ODA runtime extension's
			 * plug-in manifest to export its package for visibility here.
			 */
			IDriver customDriver = new com.kratos.birt.report.data.oda.kairosdb.impl.Driver();

			// obtain and open a live connection
			customConn = customDriver.getConnection( null );
			
			// no need to open connection to get a query and its properties
//			java.util.Properties connProps = 
//					DesignSessionUtil.getEffectiveDataSourceProperties( 
//							getInitializationDesign().getDataSourceDesign() );
//          customConn.open( connProps );

			// update the data set design with the 
			// query's current runtime metadata
			updateDesign( dataSetDesign, customConn, queryText );
		}
		catch( OdaException e )
		{
			// not able to get current metadata, reset previous derived metadata
			dataSetDesign.setResultSets( null );
			dataSetDesign.setParameters( null );
			System.out.println("save page: catching exception:");
			e.printStackTrace();
			System.out.println("exception caught");
		}
		finally
		{
			closeConnection( customConn );
		}
	}
	/**
	 * Updates the given dataSetDesign with the queryText and its derived metadata
	 * obtained from the ODA runtime connection.
	 */
	private void updateDesign( DataSetDesign dataSetDesign,
			IConnection conn, String queryText )
					throws OdaException
	{
		IQuery query = conn.newQuery( null );
		query.prepare( queryText );
		query.setProperty(VALUE_TYPE, controller.getValueType());
		query.setProperty(START_TIME_PARAMETER_NAME, controller.getStartTimeParameterName());
		query.setProperty(END_TIME_PARAMETER_NAME, controller.getEndTimeParameterName());

		try
		{
			IResultSetMetaData md = query.getMetaData();
			updateResultSetDesign( md, dataSetDesign );
		}
		catch( OdaException e )
		{
			// no result set definition available, reset previous derived metadata
			dataSetDesign.setResultSets( null );
			e.printStackTrace();
		}

		// proceed to get parameter design definition
		try
		{
			IParameterMetaData paramMd = query.getParameterMetaData();
			updateParameterDesign( paramMd, dataSetDesign );
		}
		catch( OdaException ex )
		{
			// no parameter definition available, reset previous derived metadata
			dataSetDesign.setParameters( null );
			ex.printStackTrace();
		}

		/*
		 * See DesignSessionUtil for more convenience methods
		 * to define a data set design instance.  
		 */     
	}

	/**
	 * Updates the specified data set design's result set definition based on the
	 * specified runtime metadata.
	 * @param md    runtime result set metadata instance
	 * @param dataSetDesign     data set design instance to update
	 * @throws OdaException
	 */
	private void updateResultSetDesign( IResultSetMetaData md,
			DataSetDesign dataSetDesign ) 
					throws OdaException
	{
		ResultSetColumns columns = DesignSessionUtil.toResultSetColumnsDesign( md );

		ResultSetDefinition resultSetDefn = DesignFactory.eINSTANCE
				.createResultSetDefinition();
		// resultSetDefn.setName( value );  // result set name
		resultSetDefn.setResultSetColumns( columns );

		// no exception in conversion; go ahead and assign to specified dataSetDesign
		dataSetDesign.setPrimaryResultSet( resultSetDefn );
		dataSetDesign.getResultSets().setDerivedMetaData( true );
	}

	/**
	 * Updates the specified data set design's parameter definition based on the
	 * specified runtime metadata.
	 * @param paramMd   runtime parameter metadata instance
	 * @param dataSetDesign     data set design instance to update
	 * @throws OdaException
	 */
	private void updateParameterDesign( IParameterMetaData paramMd,
			DataSetDesign dataSetDesign ) 
					throws OdaException
	{
		DataSetParameters paramDesign = 
				DesignSessionUtil.toDataSetParametersDesign( paramMd, 
						DesignSessionUtil.toParameterModeDesign( IParameterMetaData.parameterModeIn ) );

		
		// no exception in conversion; go ahead and assign to specified dataSetDesign
		dataSetDesign.setParameters( paramDesign );        
		if( paramDesign == null )
			return;     // no parameter definitions; done with update

		paramDesign.setDerivedMetaData( true );

		// hard-coded parameter's default value for demo purpose
		if( paramDesign.getParameterDefinitions().size() == 2 )
		{
			ParameterDefinition paramDef = 
					(ParameterDefinition) paramDesign.getParameterDefinitions().get( 0 );
			if( paramDef != null )
				paramDef.setDefaultScalarValue( "One hour ago" );
			
			paramDef = 
					(ParameterDefinition) paramDesign.getParameterDefinitions().get( 1 );
			if( paramDef != null )
				paramDef.setDefaultScalarValue( "Now" );
		}
	}

	/**
	 * Attempts to close given ODA connection.
	 */
	private void closeConnection( IConnection conn )
	{
		try
		{
			if( conn != null && conn.isOpen() )
				conn.close();
		}
		catch ( OdaException e )
		{
			System.out.print("failed to close connection");
			e.printStackTrace();
		}
	}

}
