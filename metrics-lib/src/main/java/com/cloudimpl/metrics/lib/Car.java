package com.cloudimpl.metrics.lib;

public class Car {
    public final Integer carId;
    public final String manufacturer;
    public final String model;
    public final Integer doors;
    public final Integer horsepower;

    public Car(Integer carId, String manufacturer, String model, Integer doors, Integer horsepower) {
        this.carId = carId;
        this.manufacturer = manufacturer;
        this.model = model;
        this.doors = doors;
        this.horsepower = horsepower;
    }

    @Override
    public String toString() {
        return "Car{" +
                "carId=" + carId +
                ", manufacturer='" + manufacturer + '\'' +
                ", model='" + model + '\'' +
                ", doors=" + doors +
                ", horsepower=" + horsepower +
                '}';
    }
}