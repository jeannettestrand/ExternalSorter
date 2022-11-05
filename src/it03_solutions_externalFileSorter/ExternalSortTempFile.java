package it03_solutions_externalFileSorter;

import java.io.BufferedReader;
import java.io.IOException;

public class ExternalSortTempFile {

	private String l;
	private BufferedReader br;
	
	public ExternalSortTempFile(String line, BufferedReader buffr) {
        this.l = line;
        this.br= buffr;
    }
	
	public String getLine() {
		return this.l;
	}
	public String getSortKey() {
		return this.l.substring(0,10);
	}
	public BufferedReader getBuffer() {
		return this.br;
	}
	public void closeBuffer() {
		if ( this.br != null ) {
			try {
				this.br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
