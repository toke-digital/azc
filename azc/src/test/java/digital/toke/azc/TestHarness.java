package digital.toke.azc;

import static digital.toke.azc.Main.main;

import org.junit.Test;

public class TestHarness {

	@Test
	public void test0() {

		// assume you set up a config file with valid credentials exists for testing
		// purposes in D:/azc"

		String[] args0 = { "--config", "D:/azc/config.properties", "--verb", "list" };
		main(args0);

		// assume the blobstore contains a file called "somefile.zip"
		String[] args1 = { "--config", "D:/azc/config.properties", "--verb", "get", "-f", "somefile.zip", "-d",
				"D:/azc" };
		main(args1);

	}

}
