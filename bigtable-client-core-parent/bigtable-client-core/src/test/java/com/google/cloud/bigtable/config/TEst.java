package com.google.cloud.bigtable.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

public class TEst {

  public static void main(String args[]){
    System.out.println("findnig the credentiatlas");
    String json = CredentialOptions.getEnvJsonFile();
    try {
      FileInputStream ins = new FileInputStream(json);
      System.out.println(ins);

      System.out.println("second");
      String fileString = new String(Files.readAllBytes(Paths.get(json)),
          StandardCharsets.UTF_8);
      System.out.println("Contents (Java 7 with character encoding ) : " + fileString);


    } catch ( IOException e) {
      e.printStackTrace();
    }
    System.out.println(CredentialOptions.jsonCredentials(json));
  }
}
