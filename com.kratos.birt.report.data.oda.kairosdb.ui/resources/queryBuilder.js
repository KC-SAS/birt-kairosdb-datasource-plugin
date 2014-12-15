function generateQueryAndGraph() {
	updateChart();
	app.queryBuilt($('#query-hidden-text').val());
}

function generateQuery() {
	clear();
	var queryText = JSON.stringify(buildKairosDBQuery(),undefined,2);
	app.queryBuilt(queryText);
	$('#query-hidden-text').val(queryText);
	displayQuery();
}

$("#submitButtonJSON").hide();
$("#submitButtonCSV").hide();
$("#saveButton").hide();
$("#deleteButton").button("enable");
$("#submitButton").text("Graph");
$("#submitButton").button().click(generateQueryAndGraph);
$("#deleteButton").text("Generate query");
$("#deleteButton").button().click(generateQuery);

