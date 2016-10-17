package de.unibonn.iai.eis.luzzu.communications;

import java.util.concurrent.Callable;

import de.unibonn.iai.eis.luzzu.io.IOProcessor;

public abstract class ExtendedCallable<V> implements Callable<V> {

	protected IOProcessor strmProc = null;
	
	public IOProcessor getIOProcessor(){
		if (this.strmProc == null) return null;
		else return this.strmProc;
	}

}
