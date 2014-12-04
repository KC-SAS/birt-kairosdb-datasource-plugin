package com.kratos.birt.report.data.oda.kairosdb.ui;

import static com.kratos.birt.report.data.oda.kairosdb.impl.Query.EVERYTHING_TEXT_VALUE;
import static com.kratos.birt.report.data.oda.kairosdb.impl.Query.NUMBER_ONLY_VALUE;
import static com.kratos.birt.report.data.oda.kairosdb.impl.Query.TEXT_ONLY_VALUE;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

import javafx.beans.property.DoubleProperty;
import javafx.beans.property.SimpleDoubleProperty;
import javafx.beans.value.ChangeListener;
import javafx.beans.value.ObservableValue;
import javafx.concurrent.Worker.State;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.geometry.Pos;
import javafx.scene.Node;
import javafx.scene.control.Accordion;
import javafx.scene.control.CheckBox;
import javafx.scene.control.Label;
import javafx.scene.control.ProgressIndicator;
import javafx.scene.control.RadioButton;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;
import javafx.scene.control.TitledPane;
import javafx.scene.input.KeyCode;
import javafx.scene.input.KeyEvent;
import javafx.scene.layout.AnchorPane;
import javafx.scene.layout.Pane;
import javafx.scene.layout.Region;
import javafx.scene.transform.Scale;
import javafx.scene.web.WebEngine;
import javafx.scene.web.WebView;
import netscape.javascript.JSException;
import netscape.javascript.JSObject;

public class FXMLController implements Initializable {
    
    @FXML
    private WebView browser;
    @FXML
    private AnchorPane browserContainer;
    
    @FXML
    private TextField searchField;
    
    @FXML
    private ProgressIndicator pageStatusProgress;
    
    @FXML
    private Label pageStatusLabel;
    
    @FXML
    private TitledPane builderPane;
    
    @FXML
    private TitledPane jsonPane;
    
    @FXML
    private Accordion accordion;
    
    @FXML
    private TextArea queryArea;
    
    @FXML
    private CheckBox isTimeParameter;
    
    @FXML
    private TextField fromParameterField;
    @FXML
    private Label fromParameterLabel;
    
    
    @FXML
    private TextField toParameterField;
    @FXML
    private Label toParameterLabel;
    
    @FXML
    private RadioButton numberOnlyRadio;
    @FXML
    private RadioButton textOnlyRadio;
    @FXML
    private RadioButton everythingTextRadio;
    
    @Override
    public void initialize(URL url, ResourceBundle rb) {
        //accordion.setExpandedPane(builderPane);
//        browser.setScaleX(0.8);
//        browser.setScaleY(0.8);
    	final ZoomingPane zoomingPane = new ZoomingPane(browser);
    	zoomingPane.widthProperty().addListener(new ChangeListener<Number>() {
    	    @Override public void changed(ObservableValue<? extends Number> observableValue, Number oldSceneWidth, Number newSceneWidth) {
    	    	double scale = Math.max(0.77, Math.min(1.,newSceneWidth.doubleValue()/780.));
    	    	zoomingPane.setZoomFactors(scale,scale);
    	    }
    	});
    	zoomingPane.setPrefSize(Region.USE_COMPUTED_SIZE, Region.USE_COMPUTED_SIZE);
    	browserContainer.getChildren().clear();
    	browserContainer.getChildren().add(zoomingPane);
    	
    	AnchorPane.setLeftAnchor(browserContainer.getChildren().get(0), 0.0);
    	AnchorPane.setRightAnchor(browserContainer.getChildren().get(0), 0.0);
    	AnchorPane.setTopAnchor(browserContainer.getChildren().get(0), 0.0);
    	AnchorPane.setBottomAnchor(browserContainer.getChildren().get(0), 0.0);
    	zoomingPane.setZoomFactors(0.77,0.78);
        
    }
    
    private class ZoomingPane extends Pane {
        Node content;
        private final DoubleProperty zoomFactor = new SimpleDoubleProperty(1);
        private double zoomFactory = 1.0;

        private ZoomingPane(Node content) {
            this.content = content;
            getChildren().add(content);
            final Scale scale = new Scale(1, 1);
            content.getTransforms().add(scale);

            zoomFactor.addListener(new ChangeListener<Number>() {
                @Override
                public void changed(ObservableValue<? extends Number> observable, Number oldValue, Number newValue) {
                    scale.setX(newValue.doubleValue());
                    scale.setY(zoomFactory);
                    requestLayout();
                }
            });
        }

        @Override
        protected void layoutChildren() {
            Pos pos = Pos.TOP_LEFT;
            double width = getWidth();
            double height = getHeight();
            double top = getInsets().getTop();
            double right = getInsets().getRight();
            double left = getInsets().getLeft();
            double bottom = getInsets().getBottom();
            double contentWidth = (width - left - right)/zoomFactor.get();
            double contentHeight = (height - top - bottom)/zoomFactory;
            layoutInArea(content, left, top,
                    contentWidth, contentHeight,
                    0, null,
                    pos.getHpos(),
                    pos.getVpos());
        }


        public final void setZoomFactors(Double zoomFactorx, Double Zoomfactory) {
            this.zoomFactory = Zoomfactory;
            this.zoomFactor.set(zoomFactorx);
        }

    }
    
    @FXML
    private void keyPressed(KeyEvent e){
        if(e.getCode()==KeyCode.ENTER){
            search();
        }
    }
    
    @FXML
	public void search(){
        final WebEngine webEngine = browser.getEngine();
        pageStatusLabel.setVisible(false);
        pageStatusLabel.setText("");
        pageStatusProgress.setVisible(true);
        webEngine.load(searchField.getText());
        webEngine.getLoadWorker().stateProperty().addListener(
                new ChangeListener<State>() {
                    @SuppressWarnings("rawtypes")
					@Override
                    public void changed(ObservableValue ov, State oldState, State newState) {
                        if (newState == State.SUCCEEDED) {
                            pageStatusProgress.setVisible(false);
                            try {
                                BufferedReader in = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream("/queryBuilder.js")));
                                StringBuilder response = new StringBuilder();
                                String inputLine;
                                while ((inputLine = in.readLine()) != null)
                                    response.append(inputLine);
                                
                                in.close();
                                webEngine.executeScript(response.toString());
                                JSObject window = (JSObject) webEngine.executeScript("window");
                                window.setMember("app", new JavaApplication());
                            } catch (IOException ex) {
                                Logger.getLogger(FXMLController.class.getName()).log(Level.SEVERE, null, ex);
                            } catch (JSException e){
                                pageStatusLabel.setText("This page is not supported, errors might occur during query building");
                                pageStatusLabel.setVisible(true);
                                System.out.println("JSException: "+e.getMessage());
                            }
                            
                        } else if (newState == State.FAILED) {
                            webEngine.loadContent("");
                            pageStatusProgress.setVisible(false);
                            pageStatusLabel.setText("Could not load this page");
                            pageStatusLabel.setVisible(true);
                        }
                    }
                });
    }
    
    
    @FXML
    private void isTimeParameterAction(){
    	if(isTimeParameter.isSelected()){
    		fromParameterField.setDisable(false);
    		toParameterField.setDisable(false);
    		fromParameterLabel.setDisable(false);
    		toParameterLabel.setDisable(false);
    	} else{
    		fromParameterField.setDisable(true);
    		toParameterField.setDisable(true);
    		fromParameterLabel.setDisable(true);
    		toParameterLabel.setDisable(true);
    	}
    }
    
    public class JavaApplication {
        public void queryBuilt(String query) {
            queryArea.setText(query);
        }
    }
    
    public void setQueryAreaText(String text){
        queryArea.setText(text);
    }
    
    public String getQueryAreaText(){
        return queryArea.getText();
    }
    
    public void addQueryAreaChangeListener(ChangeListener<String> listener){
    	queryArea.textProperty().addListener(listener);
    }
    
    public void setSearchFieldText(String text){
    	searchField.setText(text);
    }
    
    public void setRawQueryAsDefault(){
    	accordion.setExpandedPane(jsonPane);
    }
    
    public void setQueryBuilderAsDefault(){
    	accordion.setExpandedPane(builderPane);
    }
    
    public boolean isTimeParameter(){
    	return isTimeParameter.isSelected();
    }
    
    /**
     * Returns start time parameter name or null if time parameter deactivated
     * @return
     */
    public String getStartTimeParameterName(){
    	if(isTimeParameter())
    		return fromParameterField.getText();
    	else
    		return null;
    }
    
    /**
     * Returns end time parameter name or null if time parameter deactivated
     * @return
     */
    public String getEndTimeParameterName(){
    	if(isTimeParameter())
    		return toParameterField.getText();
    	else
    		return null;
    }
    
    public void setStartTimeParameterName(String text){
    	fromParameterField.setText(text);
    }
    
    public void setStopTimeParameterName(String text){
    	toParameterField.setText(text);
    }
    
    public void setTimeParameter(boolean isTimePara){
    	isTimeParameter.setSelected(isTimePara);
    	isTimeParameterAction();
    }

    public String getValueType(){
    	if(numberOnlyRadio.isSelected())
    		return NUMBER_ONLY_VALUE;
    	else if(textOnlyRadio.isSelected())
    		return TEXT_ONLY_VALUE;
    	else
    		return EVERYTHING_TEXT_VALUE;
    }

    public void setValueType(String typeValue){
    	if(typeValue.equals(NUMBER_ONLY_VALUE)){
    		numberOnlyRadio.setSelected(true);
    		numberOnlyRadio.requestFocus();
    	} else if(typeValue.equals(TEXT_ONLY_VALUE)){
    		textOnlyRadio.setSelected(true);
    		textOnlyRadio.requestFocus();
    	} else{
    		everythingTextRadio.setSelected(true);
    		everythingTextRadio.requestFocus();
    	}
    }

    
}
