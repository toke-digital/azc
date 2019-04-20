/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2019 David R. Smith All Rights Reserved 
 */
package digital.toke.azc;

import io.reactivex.Observable;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.SocketAddress;
import java.net.URL;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.storage.blob.BlobRange;
import com.microsoft.azure.storage.blob.BlockBlobURL;
import com.microsoft.azure.storage.blob.CommonRestResponse;
import com.microsoft.azure.storage.blob.ContainerURL;
import com.microsoft.azure.storage.blob.ListBlobsOptions;
import com.microsoft.azure.storage.blob.PipelineOptions;
import com.microsoft.azure.storage.blob.ServiceURL;
import com.microsoft.azure.storage.blob.SharedKeyCredentials;
import com.microsoft.azure.storage.blob.StorageURL;
import com.microsoft.azure.storage.blob.TransferManager;
import com.microsoft.azure.storage.blob.models.BlobItem;
import com.microsoft.azure.storage.blob.models.ContainerListBlobFlatSegmentResponse;
import com.microsoft.rest.v2.http.HttpClient;
import com.microsoft.rest.v2.http.HttpClientConfiguration;
import com.microsoft.rest.v2.http.HttpPipeline;
import com.microsoft.rest.v2.http.HttpPipelineLogger;
import com.microsoft.rest.v2.http.Slf4jLogger;
import com.microsoft.rest.v2.util.FlowableUtil;

import digital.toke.azc.CmdLineParser.OptionException;

/**
 * Send and receive files from Microsoft Azure blobstore
 * 
 * @author David R. Smith <dave.smith10@det.nsw.edu.au>
 *
 */
public class Main {

	private final static Logger slf4jLogger = LoggerFactory.getLogger(Main.class);
	private static boolean silent = false;
	private static boolean useProxy = false;

	private static List<Item> blobList = new ArrayList<Item>();

	public static void main(String[] args) {

		if (args.length == 0) {
			help();
			return;
		}

		// get the arguments
		CmdLineParser parser = new CmdLineParser();
		CmdLineParser.Option<Boolean> helpOption = parser.addBooleanOption('h', "help");

		// useful for ansible
		CmdLineParser.Option<Boolean> silentOption = parser.addBooleanOption('s', "silent");

		CmdLineParser.Option<String> configFileOption = parser.addStringOption('c', "config");

		CmdLineParser.Option<String> destOption = parser.addStringOption('d', "dest");

		// VERBS

		CmdLineParser.Option<String> verbOption = parser.addStringOption('v', "verb"); // list, send, get, getAll

		// the file(s) we are sending - can be used multiple times
		CmdLineParser.Option<String> fileOption = parser.addStringOption('f', "file");

		try {
			parser.parse(args);
		} catch (OptionException e) {
			e.printStackTrace();
			return;
		}

		boolean needsHelp = parser.getOptionValue(helpOption, Boolean.FALSE);

		if (needsHelp) {
			help();
			return;
		}

		// silent mode, this is needed for use in ansible plugin
		silent = parser.getOptionValue(silentOption, Boolean.FALSE);

		String configPath = parser.getOptionValue(configFileOption, "./azc.properties");

		InputStream in = null;
		Properties props = new Properties();

		try {
			File azcConfig = new File(configPath);
			if (azcConfig.exists()) {
				// external config file
				in = new FileInputStream(configPath);
			} else {
				// fallback to internal config, really only useful in development
				in = Thread.currentThread().getClass().getResourceAsStream("/azc.properties");
			}

			props.load(in);

			// flag to set proxy if required, defaults to false if not set in config
			useProxy = Boolean.valueOf(props.getProperty("USE_PROXY", "false"));

			ContainerURL containerURL = null;

			try {
				if ((containerURL = setup(props)) == null) {
					throw new RuntimeException("ContainerURL was null, cannot proceed");
				}
			} catch (Exception x) {
				// Exit with non-zero if silent mode
				if (silent)
					System.exit(1);
				else
					throw x;
			}

			// file or files we will send or try to get
			Collection<String> files = parser.getOptionValues(fileOption);

			// for get, the directory where we will put the file or files, default is
			// current dir
			Path currentRelativePath = Paths.get("");
			String defaultPath = currentRelativePath.toAbsolutePath().toString();
			String destinationPath = parser.getOptionValue(destOption, defaultPath);

			String verb = parser.getOptionValue(verbOption, "");

			if ("".equals(verb))
				verb = "list";

			switch (verb) {
			case "list": {
				// in silent mode this outputs nothing
				list(containerURL);
				break;
			}
			case "send": {
				send(containerURL, files);
				break;
			}
			case "get": {
				get(containerURL, new File(destinationPath), files);
				break;
			}
			case "getAll": {
				getAll(containerURL, new File(destinationPath));
				break;
			}
			}

		} catch (IOException e1) {
			e1.printStackTrace();
		} finally {
			if (in != null) {
				try {
					in.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	private static ContainerURL setup(Properties props) {

		ContainerURL containerURL = null;

		// can get from environment
		String accountName = System.getenv("AZURE_STORAGE_ACCOUNT");
		String accountKey = System.getenv("AZURE_STORAGE_ACCESS_KEY");
		String containerName = System.getenv("CONTAINER_NAME");

		// if null, try properties
		if (accountName == null || accountName.equals("")) {
			accountName = props.getProperty("AZURE_STORAGE_ACCOUNT");
			if (accountName == null) {
				throw new RuntimeException("no accountName defined! Bailing out...");
			}
		}
		if (accountKey == null || accountKey.equals("")) {
			accountKey = props.getProperty("AZURE_STORAGE_ACCESS_KEY");
			if (accountKey == null) {
				throw new RuntimeException("no accountKey defined! Bailing out...");
			}
		}

		if (containerName == null || containerName.equals("")) {
			containerName = props.getProperty("CONTAINER_NAME");
			if (containerName == null) {
				throw new RuntimeException("no containerName defined! Bailing out...");
			}
		}

		try {
			// Create a ServiceURL to call the Blob service. We will also use this to
			// construct the ContainerURL
			SharedKeyCredentials creds = new SharedKeyCredentials(accountName, accountKey);

			HttpClient client = null;

			if (useProxy) {
				InetAddress addr = InetAddress.getByName(props.getProperty("HTTPS_PROXY"));
				int port = Integer.parseInt(props.getProperty("HTTPS_PROXY_PORT"));
				SocketAddress sockaddr = new InetSocketAddress(addr, port);

				info("Using PROXY. Settings: " + sockaddr.toString());

				Proxy proxy = new Proxy(Proxy.Type.HTTP, sockaddr);
				HttpClientConfiguration config = new HttpClientConfiguration(proxy);
				client = HttpClient.createDefault(config);
			} else {
				client = HttpClient.createDefault();
			}

			PipelineOptions opts = null;

			// log HTTP transactions if silent flag is false
			if (silent) {
				opts = new PipelineOptions().withClient(client);
			} else {
				HttpPipelineLogger logger = new Slf4jLogger(slf4jLogger);
				opts = new PipelineOptions().withClient(client).withLogger(logger);
			}

			HttpPipeline pipe = StorageURL.createPipeline(creds, opts);

			final ServiceURL serviceURL = new ServiceURL(new URL("https://" + accountName + ".blob.core.windows.net"),
					pipe);

			containerURL = serviceURL.createContainerURL(containerName);

		} catch (Exception x) {
			x.printStackTrace();
		}

		return containerURL;

	}

	public static Observable<BlobItem> listBlobsLazy(ContainerURL containerURL, ListBlobsOptions listBlobsOptions) {
		return containerURL.listBlobsFlatSegment(null, listBlobsOptions, null)
				.flatMapObservable((r) -> listContainersResultToContainerObservable(containerURL, listBlobsOptions, r));
	}

	private static Observable<BlobItem> listContainersResultToContainerObservable(ContainerURL containerURL,
			ListBlobsOptions listBlobsOptions, ContainerListBlobFlatSegmentResponse response) {
		Observable<BlobItem> result = Observable.fromIterable(response.body().segment().blobItems());

		// System.out.println("!!! count: " + response.body().segment().blobItems());

		if (response.body().nextMarker() != null) {
			// System.out.println("Hit continuation in listing at " +
			// response.body().segment().blobItems().get(
			// response.body().segment().blobItems().size() - 1).name());

			// Recursively add the continuation items to the observable.
			result = result.concatWith(containerURL
					.listBlobsFlatSegment(response.body().nextMarker(), listBlobsOptions, null).flatMapObservable(
							(r) -> listContainersResultToContainerObservable(containerURL, listBlobsOptions, r)));
		}

		return result;
	}

	// corresponds to one of our verbs, "list"
	private static void list(ContainerURL containerURL) {

		blobList.clear();

		ListBlobsOptions options = new ListBlobsOptions();
		options.withMaxResults(1000); // just an arbitrary limit

		Observable<BlobItem> items = listBlobsLazy(containerURL, options);
		Iterable<BlobItem> iter = items.blockingIterable();
		for (BlobItem item : iter) {
			info(item.name());
			final long length = item.properties().contentLength();
			final String name = item.name();

			blobList.add(new Item(name, length));
		}
	}

	private static void getAll(ContainerURL containerURL, File destFolder) {
		get(containerURL, destFolder);
	}

	// corresponds to one of our verbs, "send"
	private static void send(ContainerURL containerURL, Collection<String> filePaths) {

		for (String path : filePaths) {
			File file = new File(path);
			// Create a BlockBlobURL to run operations on Blobs
			final BlockBlobURL blobURL = containerURL.createBlockBlobURL(file.getName());
			try {
				// try and send it
				uploadFile(blobURL, file);
			} catch (IOException e) {
				try {
					error("Looks like we failed to upload " + file.getCanonicalPath());
				} catch (IOException e1) {
					e1.printStackTrace();
				}
				e.printStackTrace();
			}
		}
	}

	private static void uploadFile(BlockBlobURL blob, File sourceFile) throws IOException {

		AsynchronousFileChannel fileChannel = AsynchronousFileChannel.open(sourceFile.toPath());
		
		// v11
		CommonRestResponse response = TransferManager.uploadFileToBlockBlob(fileChannel, blob, 8*1024*1024, 1024*1024*1024, null).blockingGet();
		
		// v10
	//	CommonRestResponse response = TransferManager.uploadFileToBlockBlob(fileChannel, blob, 8 * 1024 * 1024, null).blockingGet();
		int status = response.response().statusCode();
		if (status == 201) {
			info("Success!");
		}
		info("Response code was: " + status + " for " + sourceFile.getName());

	}

	// used with getting one or more with input list of names
	private static void get(ContainerURL containerURL, File destFolder, Collection<String> files) {

		list(containerURL);

		for (String name : files) {

			File target = new File(destFolder, name);
			if (!target.exists()) {
				// just get it
				final BlockBlobURL blobURL = containerURL.createBlockBlobURL(name);
				getBlob(blobURL, target);
			} else {
				// already present, so skip download if possible - check size of blob

				Item matchingItem = null;
				for (Item item : blobList) {
					if (item.name.equals(name)) {
						matchingItem = item;
						break;
					}
				}

				// should have found a match; if not, it is an error
				if (matchingItem == null)
					throw new RuntimeException("We should have matched an item here..." + name);

				if (target.length() == matchingItem.size) {
					info("Looks like we already have current copy of " + target.getName() + ", skipping it");
				} else {
					info("Sizes do not match - downloading again " + target.getName());
					final BlockBlobURL blobURL = containerURL.createBlockBlobURL(matchingItem.name);
					getBlob(blobURL, target);
				}

			}
		}

	}

	// get all based on the blobstore list itself
	private static void get(ContainerURL containerURL, File destFolder) {

		list(containerURL);

		for (Item item : blobList) {

			File target = new File(destFolder, item.name);
			if (!target.exists()) {
				// just get it
				final BlockBlobURL blobURL = containerURL.createBlockBlobURL(item.name);
				getBlob(blobURL, target);
			} else {
				// already present, so skip download if possible - check size of blob

				if (target.length() == item.size) {
					info("Looks like we already have current copy of " + target.getName() + ", skipping it");
				} else {
					info("Sizes do not match - downloading again " + target.getName());
					final BlockBlobURL blobURL = containerURL.createBlockBlobURL(item.name);
					getBlob(blobURL, target);
				}

			}
		}

	}

	// corresponds to our verb "get", must supply a local path for the result to be
	// written into
	private static void getBlob(BlockBlobURL blobURL, File targetFile) {

		try {
			// Get the blob using the low-level download method in BlockBlobURL type
			// com.microsoft.rest.v2.util.FlowableUtil is a static class that contains
			// helpers to work with Flowable
			// Max Blob Buffer is defined as 1024MB
			blobURL.download(new BlobRange().withOffset(0).withCount(1024 * 1024 * 1024L), null, false, null)
					.flatMapCompletable(response -> {
						AsynchronousFileChannel channel = AsynchronousFileChannel.open(Paths.get(targetFile.getPath()),
								StandardOpenOption.CREATE, StandardOpenOption.WRITE);
						return FlowableUtil.writeFile(response.body(null), channel);
					}).doOnComplete(() -> info("The blob was downloaded to " + targetFile.getAbsolutePath()))
					.blockingAwait();
			// .subscribe();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	// info level
	private static void info(String msg) {
		if (!silent)
			slf4jLogger.info(msg);
	}

	// error level
	private static void error(String msg) {
		if (!silent)
			slf4jLogger.error(msg);
	}

	private static void help() {

		System.out.println("Toke Digital - Azure Blobstore Client, version 1.0.3");
		System.out.println("Author: David R. Smith <dave.smith10@det.nsw.edu.au>");
		System.out.println("");
		System.out.println("Options:");
		System.out.println("-c --config           | Config file location, required");
		System.out.println("-s --silent           | Do not emit anything");
		System.out.println("-i --idempotent       | Attempt to copy only if not present");
		System.out.println("-v --verb <command>   | Commands: list, get, getAll, send, required");
		System.out.println("-f --file <name>      | The filename to get, or the full file path to send");
		System.out.println("-d --dest <folder>    | use with get, the folder to put the file(s)");

		System.out.println("-h --help             | Show this help");
		System.out.println("");

	}
}

class Item {

	public final String name;
	public final long size;

	public Item(String name, long size) {
		this.name = name;
		this.size = size;
	}
}
