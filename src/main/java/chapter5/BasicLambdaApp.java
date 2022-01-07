package chapter5;

import java.util.ArrayList;
import java.util.*;

public class BasicLambdaApp {

    public static void main(String[] args) {
        List<String> bengaliFirstnameList = new ArrayList<>();
        bengaliFirstnameList.add("Avijit");
        bengaliFirstnameList.add("Anita");
        bengaliFirstnameList.add("Subho");
        bengaliFirstnameList.add("Sumit");
        bengaliFirstnameList.add("Dishani");
        bengaliFirstnameList.add("Sonali");

        bengaliFirstnameList.forEach(
                name -> System.out.println(name + " and Rabindranath-" + name
                + " are different Bengali first names!")
        );
        System.out.println("**********************");
        System.out.println("**********************");

        bengaliFirstnameList.forEach(
                name -> {
                    String message = name + " and Abanindranath-";
                    message += name;
                    message += " are different Bengali first names!";
                    System.out.println(message);
                }
        );
    }
}
