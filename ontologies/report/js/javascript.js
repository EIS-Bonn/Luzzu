jQuery(document).ready(function(){
	$('#header').corner();
	$('#prologue').corner();
	$('#appendix').corner();
	$('.toc').corner();
    $('.details').corner();
    $('.vci').corner();
    $('.ti').corner();
    $('.inuse').corner();
    $('.otherinfo').corner();
    $('.menu span').corner();
    $('.distribution').corner();

    
    $('.term h3 span span.vci-menu').click(function() {
        $(this).parent().parent().nextAll('div').children('.vci').toggle('slow');
        return false;
    });

    $('.term h3 span span.ti-menu').click(function() {
        $(this).parent().parent().nextAll('div').children('.ti').toggle('slow');
        return false;
    });

    $('.term h3 span span.otherinfo-menu').click(function() {
        $(this).parent().parent().nextAll('div').children('.otherinfo').toggle('slow');
        return false;
    });

    $('.term h3 span span.inuse-menu').click(function() {
        $(this).parent().parent().nextAll('div').children('.inuse').toggle('slow');
        return false;
    });
    
    $('.menu span').click(function () {
        $(this).toggleClass('active no-active');
    });
    
    $("#bottom-bar").jixedbar({
        transparent: true,
        opacity: 0.5,
        slideSpeed: "slow",
        roundedCorners: true,
        roundedButtons: true,
        menuFadeSpeed: "slow",
        tooltipFadeSpeed: "fast",
        tooltipFadeOpacity: 0.5
    });
    
    $("#openCloseAll").toggle(function() {
            $('div.term h3 span.menu span').addClass('active');
    	    $('.term div div').show();
            return false;
      }, function() {
            $('div.term h3 span.menu span').removeClass('active');
    	    $(".term div div").hide();
            return false;
      });
    
    $.fn.qtip.styles.parrot = { 
            border: {
               width:1,
               radius: 5
            },
            padding: 10, 
            textAlign: 'center',
    		fontSize: '.9em',
//            width: 300,
            tip: true, // Give it a speech bubble tip with automatic corner detection
            name: 'green' // Style it according to the preset 'cream' style
      }

      $.fn.qtip.defaults.position.corner = {
           tooltip: 'bottomMiddle',
           target: 'topMiddle'
      }

      $.fn.qtip.defaults.hide.when.event = 'mouseout';
      $.fn.qtip.defaults.hide.fixed = true;
    
      $('.icon-info-description').qtip({
          content: 'Add a description using http://purl.org/dc/terms/description',
          style: 'parrot'
      });
      
      $('.datatype-property-icon').qtip({
          content: 'A <a href="http://www.w3.org/TR/owl2-syntax#Data_Properties" target="_blank">data property</a> is used to describe attributes of resources, such as the height of a person or the population of a country.',
          style: 'parrot'
      });
      
      $('.object-property-icon').qtip({
          content: 'An <a href="http://www.w3.org/TR/owl2-syntax#Object_Properties" target="_blank">object property</a> is used to describe relations to other resources, such as the mother of a person or the capital of a country.',
          style: 'parrot'
      });
      
      $('.annotation-property-icon').qtip({
          content: 'An <a href="http://www.w3.org/TR/owl2-syntax#Annotation_Properties" target="_blank">annotation property</a> is used to give more information of resources .',// FIXME complete the description 
          style: 'parrot'
      });
      
      $('.reflexive-property-icon').qtip({
          content: 'A <a href="http://www.w3.org/TR/owl2-syntax/#Reflexive_Object_Properties" target="_blank">reflexive property</a> describes a relation where every resource is related to itself.', 
          style: 'parrot'
      });
      
      $('.irreflexive-property-icon').qtip({
          content: '<a href="http://www.w3.org/TR/owl2-syntax/#Irreflexive_Object_Properties" target="_blank">Irreflexive property</a> describes a relation where none resource is related to itself.',
          style: 'parrot'
      });
      
      $('.symmetric-property-icon').qtip({
          content: '<a href="http://www.w3.org/TR/owl2-syntax/#Symmetric_Object_Properties" target="_blank">Symmetric property</a>', // FIXME complete the description
          style: 'parrot'
      });
      
      $('.asymmetric-property-icon').qtip({
          content: '<a href="http://www.w3.org/TR/owl2-syntax/#Asymmetric_Object_Properties" target="_blank">Asymmetric property</a>',// FIXME complete the description 
          style: 'parrot'
      });

      $('.transitive-property-icon').qtip({
          content: '<a href="http://www.w3.org/TR/owl2-syntax/#Transitive_Object_Properties" target="_blank">Transitive property</a>',// FIXME complete the description 
          style: 'parrot'
      });
      
      $('.functional-property-icon').qtip({
          content: '<a href="http://www.w3.org/TR/owl2-syntax/#Functional_Object_Properties" target="_blank">Functional property</a>',// FIXME complete the description 
          style: 'parrot'
      });
      
      $('.inverse-functional-property-icon').qtip({
          content: '<a href="http://www.w3.org/TR/owl2-syntax/#Inverse-Functional_Object_Properties" target="_blank">Inverse functional property</a>',// FIXME complete the description 
          style: 'parrot'
      });
      
  	$('#feedback').feedback({
		'position': 'left',
		'mouseoffColor': '#8470ff'
	});
	
    $('#feedback-dialog').dialog({
        autoOpen: false,
		modal: true,
		resizable: false,
		draggable: false
	});
    
    $('#feedback').click(function() {
        $('#feedback-dialog').dialog('open');
        $('#feedback-dialog a').blur(); // remove manually the focus on a elements
		return false;
    });
      
})
