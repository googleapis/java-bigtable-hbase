package com.google.cloud.bigtable.beam.sequencefiles;

import java.io.File;
import java.net.URISyntaxException;
import java.util.Arrays;

/**
 * Entry point for import/export job submission.
 *
 * @author igorbernstein2
 */
public class Main {

  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      usage();
      System.exit(1);
    }

    String[] subArgs = Arrays.copyOfRange(args, 1, args.length);

    switch (args[0]) {
      case "export":
        ExportJob.main(subArgs);
        break;
      case "import":
        ImportJob.main(subArgs);
        break;
      default:
        usage();
        System.exit(1);
    }
  }


  private static void usage() {
    String jarName;

    try {
      jarName = new File(Main.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath()).getName();
    } catch (URISyntaxException e) {
      jarName = "<jar>";
    }

    System.out.printf(
        "java -jar %s <action> <action_params>\n"
            + "Where <action> can be 'export' or 'import'. To get further help, run: \n"
            + "java -jar %s <action> --help\n",
        jarName,
        jarName
    );
  }
}
