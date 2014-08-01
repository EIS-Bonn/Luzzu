package de.unibonn.iai.eis.luzzu.datatypes;

/**
 * @author Jeremy Debatista
 * 
 * This class represent the StatusCodes required to Dereference URIs
 * 
 * @see <a href="http://dl.dropboxusercontent.com/u/4138729/paper/dereference_iswc2011.pdf">
 * Dereferencing Semantic Web URIs: What is 200 OK on the Semantic Web?</a> 
 * 
 */
public enum StatusCode {
	SC200,SC301,SC302,SC303,SC307,SC4XX,SC5XX,UNHASH,BAD,ANY,EMPTY
}
