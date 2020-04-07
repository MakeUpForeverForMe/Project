package com.mine;

interface Usb {
    void start();

    void stop();
}

class Flash implements Usb {
    public void start() {
        System.out.println("Flash start");
    }

    public void stop() {
        System.out.println("Flash stop");
    }
}

class Print implements Usb {
    public void start() {
        System.out.println("Print start");
    }

    public void stop() {
        System.out.println("Print stop");
    }
}

class Computer {
    static void plugin(Usb usb) {
        usb.start();
        usb.stop();
    }
}

public class JavaTest {
    public static void main(String[] args) {
        Computer.plugin(new Flash());
        Computer.plugin(new Print());
    }
}