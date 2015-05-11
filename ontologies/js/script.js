var menulinks = "";
var ser = "";
$(document).ready(function() {
	$.getJSON( "../params.json", function( data ) {
		// load footer items
		$("#title").text(data['topbar']['title']);
		$("#menutitle").text(data['topbar']['title']);
		
		links = data['topbar']['links'];
		
		$.each(links, function(item) {
				if (links[item] === "home") menulinks = menulinks.concat("<a href='index.html'>home</a> | ");
				menulinks = menulinks.concat("<li><a href='"+links[item]['link']+"'>"+links[item]['name']+"</a></li>");
			});
		$("#menuitems").html(menulinks);
		
		serialisation = data['serialisations'];
		$.each(serialisation, function(item) {
			ser = ser.concat("<a class='label label-primary' href='"+negotiate(serialisation[item])+"'>"+serialisation[item]+"</a>&nbsp;");
		})
		$("#serialisation").html(ser);
		
		//load settings
		if(data['settings']['topbar'] == false){
			$("#topbar").css('visibility', 'hidden');
			$("#menuitems").css('visibility', 'hidden');
		}
	});	
	

	$('#toc').toc({
		'listType':'<ol/>',
	    'selectors': 'h3,h4', //elements to use as headings
	    'container': 'content', //element to find all selectors in
	    smoothScrolling: function(target, options, callback) {
	      $(target).smoothScroller({
	        offset: options.scrollToOffset
	      }).on('smoothScrollerComplete', function() {
	        callback();
	      });
	    },
	    scrollToOffset: 0,
	    prefix: 'toc',
	    activeClass: 'toc-active',
	    onHighlight: function() {},
	    highlightOnScroll: true,
	    highlightOffset: 0,
	    anchorName: function(i, heading, prefix) {
	      return prefix+i;
	    },
	    headerText: function(i, heading, $heading) {
	      return $heading.data('toc-title') || $heading.text();
	    },
	    itemClass: function(i, heading, $heading, prefix) {
	      return prefix + '-' + $heading[0].tagName.toLowerCase();
	    }
	});
	
});

$(function() {
    $('a').tooltip({placement: 'bottom'});
});


function loadFile(path)
{
	
	var stringData = $.ajax({
	                    url: path,
	                    async: false
	}).responseText;
	
	return stringData;
}

function negotiate(serialisation){
	var str = document.URL;
	str = str.replace(".html","");
	if (serialisation == "rdf/xml")
		return str+".rdf";
	if (serialisation == "turtle")
		return str+".ttl";
	
	//TODO more serialisations
}

