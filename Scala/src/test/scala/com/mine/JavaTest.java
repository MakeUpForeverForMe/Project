package com.mine;

interface Usb {
  void start();
  void stop();
}

class Computer {
  static void plugin(Usb usb) {
    usb.start();
    usb.stop();
  }
}

class Flash implements Usb {
  public void start() {
    System.out.println("U start");
  }
  public void stop() {
    System.out.println("U stop");
  }
}

class Print implements Usb {
  public void start() {
    System.out.println("print start");
  }
  public void stop() {
    System.out.println("print stop");
  }
}

public class JavaTest {
  public static void main(String[] args) {
    Computer.plugin(new Flash());
    Computer.plugin(new Print());
  }
}