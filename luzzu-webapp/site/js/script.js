var menulinks = "";

$(document).ready(function() {
	$.getJSON( "params.json", function( data ) {
		// load footer items
		$("#lastUpdateDate").text(data['footer']['lastUpdateDate']);
		
		footerLinks = "";
		links = data['footer']['links'];
		$.each(links, function(item) {
			footerLinks = footerLinks.concat("<a href='"+links[item]['link']+".html'>"+links[item]['name']+"</a> | ");
		});
		$("#links").html(footerLinks);
	});	
	
	//hide other visualise and edit boxes
	loadPage("rank");
});

function loadFile(path){
	var stringData = $.ajax({
	      url: path,
	      async: false
	}).responseText;
	
	return stringData;
}

var facetOptions;
function loadPage(id){
	
	if (id === "rank"){
		$("#_mainCol").load('views/rank.html');
		getFacetOptions();
	}
	
	if (id === "visualise"){
		$("#_mainCol").load('views/visualisewizard.html');
	}
	
	if (id === "assess"){
		$("#_mainCol").load('views/assess.html');
	}
	
}

function getConfiguredMetrics(){
	var url = "http://localhost:8080/Luzzu/framework/web/get/configured/metrics";
	waitingDialog.show('Loading Configured Metrics from Luzzu');
	$.ajax(url, {
		type:"GET",
		error: function(jqXHR, textStatus, errorThrown) {
			waitingDialog.hide();
		    BootstrapDialog.show({
		              title: 'Error in Retrieving Configured Metrics',
		              message: 'Please check Luzzu Log for the error',
					  type: 'type-danger'
		          });
		},
		success: function(data, textStatus, jqXHR){
			waitingDialog.hide();
			$.each(data['@graph'], function(index){
				$("#_metricList").append("<input style='styled' type='checkbox' id='"+this['javaPackageName']+"'> <label for='"+this['javaPackageName']+"'>"+this['label']+"</label></input><br/>");
			});	
		}
	});
}

function getFacetOptions(){
	var url = "http://localhost:8080/Luzzu/framework/web/get/facet/options";
	waitingDialog.show('Loading Facets');
	$.ajax(url, {
		type:"GET",
		error: function(jqXHR, textStatus, errorThrown) {
			waitingDialog.hide();
		    BootstrapDialog.show({
		              title: 'Error in Retrieving Facet Options',
		              message: 'Please check Luzzu Log for the error',
					  type: 'type-danger'
		          });
		},
		success: function(data, textStatus, jqXHR){
			waitingDialog.hide();
			facetOptions = data;
			buildCategoryFacet();
			ranking('category',false);
		}
	});
	
}


function ranking(id, show){
	var allCats = $("#_" + id + " > .checkbox > input");
	
	var apiCallJson = [];

	$.each(allCats, function(index){
		var obj = {};
		obj['type'] = id;
		obj['uri'] = $(this).attr('id');
		obj['weight'] = $($("#_" + id + " > input")[index]).val();
		apiCallJson.push(obj);
	});

	rankResultsAPI(apiCallJson, show);
}

function rankResultsAPI(data, showValues){
	var url = "http://localhost:8080/Luzzu/rank";

	$.ajax(url, {
 		type:"POST",
 		dataType:"json",
 		data: JSON.stringify(data),
		error: function(jqXHR, textStatus, errorThrown) {
			waitingDialog.hide();
		    BootstrapDialog.show({
		              title: 'Error in Retrieving Ranked Object',
		              message: 'Please check Luzzu Log for the error',
					  type: 'type-danger'
		          });
		},
		success: function(data, textStatus, jqXHR){
			$("#results").empty();
			$.each(data['ranking'],function(index, element){
				html = "<div class='panel panel-body'><h3>"+ element['dataset'] +"</h3><br/>";
				
				if (showValues){
					html += "Quality Assessment Value: " + element['rankedValue'] + "<br/>";
				}
				
				html += "<span class='label label-primary' style='display: inline-block;'>Details</span> <span class='label label-success' style='display: inline-block;'>Download Quality Metadata</span>";
				html +="</div>";
				
				$("#results").append(html);
			})
			
		}
	});
}

function buildCategoryFacet(){
	$.each(facetOptions['category'], function(index,element){
		uri = element['uri'];
		label = element['label'];
		$("#_category").append("<label class='checkbox checkbox-info'> <input style='styled' type='checkbox' cid='"+index+"' id='"+uri+"' onclick='buildDimensionFacet()'> <label for='"+uri+"'>"+label+" : </label></input></label><input value='1.0' type='text' id='txt_"+uri+"' placeholder='Weight' class='form-control' style='height: 22px; width:70px;margin-left:10px; border-top:none; border-left:none; border-right:none; box-shadow:none'><br/><br/>");
	});	
}

function buildDimensionFacet(){
	$("#_dimension").empty();
	$("#_metric").empty();
	$("#refresh_dim").addClass('hide');
	$("#refresh_met").addClass('hide');
	
	
    $('#_category :checked').each(function() {
       catChosen = $(this).attr('cid');
	   dim = facetOptions['category'][catChosen]['dimension'];
	   assVal = 1 / dim.length;
	   $.each(dim, function(index,element){
   			uri = element['uri'];
   			label = element['label'];
   			$("#_dimension").append("<label class='checkbox checkbox-warning'> <input style='styled' type='checkbox' cid='"+catChosen+"' did='"+index+"' id='"+uri+"' onclick='buildMetricFacet()'> <label for='"+uri+"'>"+label+" : </label></input></label><input value='"+assVal+"' type='text' id='txt_"+uri+"' placeholder='Weight' class='form-control' style='height: 22px; width:70px;margin-left:10px;border-top:none; border-left:none; border-right:none; box-shadow:none' '><br/><br/>");
	   });
   		$("#refresh_dim").removeClass('hide');
    });

	ranking('dimension', true);
}

function buildMetricFacet(){
	$("#_metric").empty();
	$("#refresh_met").addClass('hide');
	
    $('#_dimension :checked').each(function() {
       catChosen = $(this).attr('cid');
       dimChosen = $(this).attr('did');
	   metric = facetOptions['category'][catChosen]['dimension'][dimChosen]['metric'];
	   assVal = 1 / metric.length;
	   $.each(metric, function(index,element){
   			uri = element['uri'];
   			label = element['label'];
   			$("#_metric").append("<label class='checkbox checkbox-success'> <input style='styled' type='checkbox' id='"+uri+"'> <label for='"+uri+"'>"+label+" : </label></input></label><input value='"+assVal+"' type='text' id='txt_"+uri+"' placeholder='Weight' class='form-control' style='height: 22px; width:70px; margin-left:10px;border-top:none; border-left:none; border-right:none; box-shadow:none''><br/><br/>");

	   });
	   $("#refresh_met").removeClass('hide');
    });
	
	ranking('metric', true);
}

function assessDataset(){
	var url = "http://localhost:8080/Luzzu/compute_quality";
	
	assessment = {};
	assessment['BaseUri'] = $('input#txtBaseURI').val();
	assessment['Dataset'] = $('input#txtDataset').val();
	
	//if ($("#tgl_query").attr('checked'))
	//assessment['isSparql'] = "false";
	//else
	//assessment['isSparql'] = "true";
	
	if ($("#chk_problemReport").checked)
		assessment['QualityReportRequired'] = "true";
	else
		assessment['QualityReportRequired'] = "false";
	
	chosenMetrics = [];
    $('#_metricList :checked').each(function() {
       chosenMetrics.push($(this).attr('id'));
     });
	
	
	var doc = {
	  "@id": "_:f641f585449924f2aa2fc7ad880021cb2b1",
	  "@type": "lmi:MetricConfiguration",
	  "lmi:metric": chosenMetrics
	};
	var context = {
	    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
	    "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
	    "xsd": "http://www.w3.org/2001/XMLSchema#",
		"lmi": "http://purl.org/eis/vocab/lmi#"
	};
	
	var stringify = "";
	jsonld.compact(doc, context, function(err, compacted) {
		 assessment['MetricsConfiguration'] = JSON.stringify(compacted, null, 2);
		 
	 	var url = "http://localhost:8080/Luzzu/compute_quality";
		waitingDialog.show('Assessing Quality Metrics');
		
	 	$.ajax(url, {
	 		type:"POST",
	 		dataType:"json",
	 		data:assessment,
	 		error: function(jqXHR, textStatus, errorThrown) {
				waitingDialog.hide();
			    BootstrapDialog.show({
			              title: 'Error in Quality Assessment',
			              message: 'Please check Luzzu Log for the error',
						  type: 'type-danger'
			          });
				console.log("error")
	 		},
	 		success: function(data, textStatus, jqXHR){
				waitingDialog.hide();
				console.log(data);
				console.log(textStatus);
			    BootstrapDialog.show({
			              title: 'Quality Assessment Ready',
			              message: 'Assessment is ready. Click <a href="" onclick="$("#_mainCol").load("views/rank.html");">here</a> to go to the ranking page.',
						  type: 'type-success'
			          });
	 		}
	 	});
	});
}

function wizardStp2(id){
	$("#frm_step2").empty();
	$('#frm_step3').empty();
	if(id == 'radio1'){
		//Visualise Two Datasets over a number of Metrics
		//basic line
		//bar
		//column
		$("#frm_step2").append("<div class='radio radio-success'  id='chk_step2_radio2' ><input onclick='wizardStp3(\"radio1\")' style='styled' type='radio' id='radio21' value='bar' name='stp2'><label for='radio21'>Bar Chart</label></input></div><div class='radio radio-success' id='chk_step2_radio3'><input onclick='wizardStp3(\"radio1\")' style='styled' type='radio' id='radio31' value='column' name='stp2'><label for='radio31'>Column Chart</label></input></div>");
	}
	
	if(id == 'radio2'){
		//Visualise Quality of Dataset of a Metric over Time
		//line chart
		//time series
		$("#frm_step2").append("<div class='radio radio-success' id='chk_step2_radio1'><input onclick='wizardStp3(\"radio2\")' style='styled' type='radio' id='radio12' value='line' name='stp2'><label for='radio12'>Line Chart</label></input></div>");
	}
	
	if(id == 'radio3'){
		//Visualise the Quality of a Dataset
		//pie chart
		//column chart
		$("#frm_step2").append("<div class='radio radio-success' id='chk_step2_radio1'><input onclick='wizardStp3(\"radio3\")' style='styled' type='radio' id='radio13' value='pie' name='stp2'><label for='radio13'>Pie Chart</label></input></div><div class='radio radio-success'  id='chk_step2_radio2'><input onclick='wizardStp3(\"radio3\")' style='styled' type='radio' id='radio23' value='column' name='stp2'><label for='radio23'>Column Chart</label></input></div>");
	}
}

function wizardStp3(id){
	var url = "http://localhost:8080/Luzzu/framework/web/post/wizard/option";
	
	data = ""
	if (id == "radio1") data = "type=one";
	if (id == "radio2") data = "type=two";
	if (id == "radio3") data = "type=three";
	 	
 	$.ajax(url, {
 		type:"POST",
 		dataType:"json",
 		data: data,
 		error: function(jqXHR, textStatus, errorThrown) {
		    BootstrapDialog.show({
		              title: 'Error in Quality Assessment',
		              message: 'Please check Luzzu Log for the error',
					  type: 'type-danger'
		          });
			console.log("error")
 		},
		success : function(data, textStatus, jqXHR) {
			if (id == "radio1") loadVisWizardOpt1(data);
			if (id == "radio2") loadVisWizardOpt2(data);
			if (id == "radio3") loadVisWizardOpt3(data);
 		}
 	});
	
}

function loadVisWizardOpt1(data){
	$('#frm_step3').empty();
	$.each(data['metrics'], function(index,element){
		_name = element['name'];
		_uri = element['uri'];
		
		
		_html = "<div class='radio radio-success'  id='chk_step1_radio3'>";
		_html += "<input style='styled' type='radio' id='"+index+"' value='"+_uri+"'  onclick='unCheckDatasets()' name='stp3'>";
		_html += "<label for='"+index+"'>"+_name+"</label></input></div>";
		
		_html += "<div id='met_"+index+"' class='hide secondlevel' style='margin-left:15px'>";
		$.each(element['commonDatasets'], function(idx,elm){
			_html += "<div class='checkbox checkbox-primary' id='checkbox11"+index+idx+"'>";
			_html += "<input style='styled' type='checkbox' id='"+elm+"'><label for='"+elm+"'>"+elm+"</label></input>";
			_html += "</div>";
		});
		_html += "</div>";
		
		$('#frm_step3').append(_html);
	});
	$('#frm_step3').append("<center><a class='btn btn-default' onclick='wizardVisualise()'>Visualise!</a></center>");
}

function loadVisWizardOpt2(data){
	$('#frm_step3').empty();
	$.each(data['datasets'], function(index,element){
		_name = element['name'];
		
		_html = "<div class='radio radio-success'  id='chk_step1_radio3'>";
		_html += "<input style='styled' type='radio' id='"+index+"' value='"+_name+"'  onclick='unCheckDatasets()' name='stp3'>";
		_html += "<label for='"+index+"'>"+_name+"</label></input></div>";
		
		_html += "<div id='met_"+index+"' class='hide secondlevel' style='margin-left:15px'>";
		$.each(element['metrics'], function(idx,elm){
			_html += "<div class='checkbox checkbox-primary' id='checkbox11"+index+idx+"'>";
			_html += "<input style='styled' type='checkbox' id='"+elm['uri']+"'><label for='"+elm['uri']+"'>"+elm['name']+"</label></input>";
			_html += "</div>";
		});
		_html += "</div>";
		
		$('#frm_step3').append(_html);
	});
	$('#frm_step3').append("<center><a class='btn btn-default' onclick='wizardVisualise()'>Visualise!</a></center>");
}

function loadVisWizardOpt3(data){
	$('#frm_step3').empty();
	$.each(data, function(index,element){
		_name = element;
		
		_html = "<div class='radio radio-success'  id='chk_step1_radio3'>";
		_html += "<input style='styled' type='radio' id='checkbox11"+index+"' value='"+_name+"' name='stp3'>";
		_html += "<label for='checkbox11"+index+"'>"+_name+"</label></input></div>";
		_html += "</div>";
		
		$('#frm_step3').append(_html);
	});
	$('#frm_step3').append("<center><a class='btn btn-default' onclick='wizardVisualise()'>Visualise!</a></center>");
}

function unCheckDatasets(){
	var radiobuttons = $("#frm_step3 > .radio > input");
	
	$.each(radiobuttons, function(){
		if ($(this).is(':checked')){
			$("#met_"+$(this).attr('id')).removeClass("hide")
		} else {
			var checkboxes = $("#met_"+$(this).attr('id')+" > .checkbox > input");
	
			var checked = false;
			$.each(checkboxes, function(){
				if ($(this).is(':checked')){
					$(this).removeAttr('checked');
					checked = true;
				} 
			});
			
			$("#met_"+$(this).attr('id')).addClass("hide");
		}
	});
}

function getCheckedBox(lstRadioButtons){
	var retVal = "";
	$.each(lstRadioButtons, function(){
		if ($(this).is(':checked')){
			retVal = $(this).val();
			return false;
		} 
	});
	return retVal;
}

function getChosenValues(values, type){

	if (type == 'radio'){
		var retVal = "";
		$.each(values, function(){
			if ($(this).is(':checked')){
				retVal = $(this).val();
				return false;
			} 
		});
		return retVal;
	}
	
	if (type == 'checkbox'){
		var multiplevalues = [];
		$.each(values, function(){
			if ($(this).is(':checked')){
				multiplevalues.push($(this).attr('id'));
			}
		});
		return multiplevalues;
	}
}

function createHighChartOption1(data, chartType){
	//parse data
	var metric = "";
	var rawData = [];
	$.each(data['datasets'], function(index, element){
		var ds = {};
		ds['name'] = element['name'];
		ds['value'] = element['metrics'][0]['latestValue'];
		metric = element['metrics'][0]['name'];
		rawData.push(ds);
	});
	
	return createSimpleChart("","Quality Percentage",rawData,chartType,metric);
}

function createHighChartOption2(data, chartType){
	console.log(data);
	var rawData = [];
	var dataset = data['name'];
	
	$.each(data['metrics'], function(index, element){
		metric = {}
		metric['name'] = element['name'];
		metric['values'] = [];
		
		$.each(element['lstObservations'], function(index, element){
			obs = {};
			obs['date'] = element['observationDate'];
			obs['value'] = element['observationValue'];
			metric['values'].push(obs);
		})
		rawData.push(metric);
	});

	return createTimeChart(dataset, rawData);
}

function createTimeChart(title, rawData){
	var chartObject = {};
		
	chartObject["title"] = {};
	chartObject["title"]["text"] = title;

	chartObject["xAxis"] = {};
	chartObject["xAxis"]["categories"] = [];
	chartObject["xAxis"]["type"] = 'datetime';	
	
	$.each(rawData, function(index,element){	
		$.each(element['values'],function(idx, elm){
			date = new Date(parseInt((elm['date'])));
			formatted = date.getFullYear() + "-" + 
			      ("0" + (date.getMonth() + 1)).slice(-2) + "-" + 
					("0" + date.getDate()).slice(-2); 
			  
			chartObject["xAxis"]["categories"].push(formatted);
		});
	});
	
	chartObject["yAxis"] = {};
	chartObject["yAxis"]["title"] = {};
	chartObject["yAxis"]["title"]["text"] = 'Value in %';
	chartObject["yAxis"]["min"] = '0';
	chartObject["yAxis"]["tickInterval"] = '5';
	chartObject["yAxis"]["ceiling"] = 100;
	
	chartObject["zoomType"] =  'y'
	
	chartObject["tooltip"] = {};
	chartObject["tooltip"]["valueSuffix"] = '%';
	
	chartObject["legend"] = {};
    chartObject["layout"] = 'vertical';
    chartObject["align"] = 'right';
    chartObject["verticalAlign"] = 'middle';
    chartObject["borderWidth"] = 0
	
	chartObject["series"] = [];
	
	$.each(rawData, function(index,element){
		ser = {};
		ser['name']= element['name'];
		ser['data'] = [];
		
		$.each(element['values'],function(index,element){
			ser['data'].push(element['value']);
		});
		chartObject["series"].push(ser);
	});
	
	return chartObject;
}
   
function createHighChartOption3(dataset, data, chartType){
	
	//parse data
	var rawData = [];
	$.each(data['metrics'], function(index, element){
		var ds = {};
		ds['name'] = element['name'];
		ds['value'] = element['latestValue'];
		rawData.push(ds);
	});
	
	if (chartType == 'pie') return createPieChart('Quality Metadata Visualisation for '+ dataset, rawData);
	if (chartType == 'column') return createSimpleChart('Quality Metadata Visualisation for '+ dataset, 'Quality Percentage', rawData, 'column', dataset);
	
}

function createSimpleChart(title, yaxis, rawData, chartType, seriesName){
	//create chart object
	var chartObject = {};
	
	chartObject["chart"] = {};
	chartObject["chart"]["type"] = chartType;
	
	chartObject["title"] = {};
	chartObject["title"]["text"] = title;
	
	chartObject["xAxis"] = {};
	chartObject["xAxis"]["categories"] = [];
	
	$.each(rawData, function(index, element){
		chartObject["xAxis"]["categories"].push(element['name']);
	});
	
	chartObject["yAxis"] = {};
	chartObject["yAxis"]["ceiling"] = 100;
	chartObject["yAxis"]["title"] = {};
	chartObject["yAxis"]["title"]["text"] = yaxis;
	
	chartObject["series"] = [];
	
	var seriesObj = {};
	seriesObj['name'] = seriesName;
	seriesObj['data'] = [];
	
	$.each(rawData, function(index, element){
		seriesObj['data'].push(element['value']);
	});
	
	chartObject["series"].push(seriesObj);
	
	return chartObject;
}

function createPieChart(title, rawData){
	//create chart object
	var chartObject = {};
	
	chartObject["chart"] = {};
	chartObject["chart"]["type"] = 'pie';
	
	chartObject["title"] = {};
	chartObject["title"]["text"] = title
	
	chartObject["tooltip"] = {};
	chartObject["tooltip"]["pointFormat"] = '{series.name}: <b>{point.percentage:.1f}%</b>'
	
	chartObject["series"] = [];
	
	var seriesObj = {};
	seriesObj['type'] = 'pie';
	seriesObj['name'] = 'Metric';
	seriesObj['data'] = [];
	
	$.each(rawData, function(index, element){
		var dataObj = [];
		dataObj.push(element['name']);
		dataObj.push(element['value']);
		seriesObj['data'].push(dataObj);
	});
	
	chartObject["series"].push(seriesObj);
	
	return chartObject;
}

function callVisApi(visValues, chartType, option, call){
 	var url = "http://localhost:8080/Luzzu/framework/web/post/visualisation/"+call;
	
 	$.ajax(url, {
 		type:"POST",
 		dataType:"json",
 		data: visValues,
 		error: function(jqXHR, textStatus, errorThrown) {
		    BootstrapDialog.show({
		              title: 'Error in Visualising View',
		              message: 'Please check Luzzu Log for the error',
					  type: 'type-danger'
		          });
			console.log("error")
 		},
		success : function(data, textStatus, jqXHR) {
			var chartObject = {};
			if (option == "option1") {
				chartObject = createHighChartOption1(data, chartType);
			}
			if (option == "option2"){
				chartObject = createHighChartOption2(data, chartType);
			}
			if (option == "option3") {
				chartObject = createHighChartOption3(visValues['dataset'], data, chartType);
			}
			visualiseGraphFromWizard(chartObject);
 		}
 	});
}

function wizardVisualise(){	
	var optionStp1 = getCheckedBox($("#_step1 > .panel-body > form > .radio > input")); // what do we want to display
	var chartType = getCheckedBox($("#_step2 > .panel-body > form > .radio > input")); // how do we want to display it
	
	var visValues = {};
	
	if (optionStp1 == "option1"){
		visValues['metric'] = getChosenValues($("#_step3 > .panel-body > form > .radio > input"), 'radio');
		visValues['datasets'] = JSON.stringify(getChosenValues($("#_step3 > .panel-body > form > .secondlevel > .checkbox > input"), 'checkbox'));
		callVisApi(visValues, chartType, 'option1', 'dsvsm');
		
	}
	
	if (optionStp1 == "option3"){
		visValues['dataset'] = getChosenValues($("#_step3 > .panel-body > form > .radio > input"), 'radio');
		callVisApi(visValues, chartType, 'option3', 'dsqualityvis');
		
	} 
	
	if (optionStp1 == "option2"){
		visValues['dataset'] = getChosenValues($("#_step3 > .panel-body > form > .radio > input"), 'radio');
		visValues['metrics'] = JSON.stringify(getChosenValues($("#_step3 > .panel-body > form > .secondlevel > .checkbox > input"), 'checkbox'));
		callVisApi(visValues, chartType, 'option2', 'dstime');
	}
}

function visualiseGraphFromWizard(chartObject){
	$( "#_mainCol" ).load("views/visualise.html", function(){
		$('#graph').highcharts(chartObject);
	});
}