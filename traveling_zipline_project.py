# -*- coding: utf-8 -*-
"""
Zipline Project
"""

# import package
import os
import math
import pandas as pd
from typing import Dict, List, TextIO, Tuple

# Each Nest has this many Zips
NUM_ZIPS = 10

# Each Zip can carry between 1 and this many packages per flight
# Note: a Zip can deliver more than 1 package per stop
MAX_PACKAGES_PER_ZIP = 3

# Zips fly a constant groundspeed (m/s)
ZIP_SPEED_MPS = 30

# Zips can fly a total roundtrip distance (m)
ZIP_MAX_CUMULATIVE_RANGE_M = 160 * 1000  # 160 km -> meters

# The two acceptable priorities
EMERGENCY = "Emergency"
RESUPPLY = "Resupply"

class Hospital:
    def __init__(self, name: str, north_m: int, east_m: int):
        self.name = name
        self.north_m = north_m
        self.east_m = east_m

    @staticmethod
    def load_from_csv(f: TextIO) -> Dict[str, "Hospital"]:
        hospitals = {}
        for line in f.readlines():
            fields = [values.strip() for values in line.split(",")]
            name = fields[0]
            hospitals[name] = Hospital(name=name, north_m=int(fields[1]), east_m=int(fields[2]))
        return hospitals

class Order:
    def __init__(self, order_id: int, time: int, hospital: Hospital, priority: str):
        self.order_id = order_id
        self.time = time
        self.hospital = hospital
        self.priority = priority

    @staticmethod
    def load_from_csv(f: TextIO, hospitals: Dict[str, Hospital]) -> List["Order"]:
        orders = []
        for i, line in enumerate(f.readlines()):
            fields = [values.strip() for values in line.split(",")]
            orders.append(Order(order_id=i+1, time=int(fields[0]), hospital=hospitals[fields[1]], priority=fields[2]))
        return orders

class Flight:
    def __init__(self, flight_id: int, zip_id: int, launch_time: int, orders: List[Order], route: List[str], completion_time: int, route_times: List[int]):
        """
        Args:
            flight_id : Unique ID for the flight.
            zip_id : The Zip performing the flight.
            launch_time : Time the flight launches.
            orders : List of orders delivered in this flight.
            route : List of locations the flight visits.
            completion_time: Time the flight completes its journey.
            route_times: List of segment times for the flight route.
        """
        self.flight_id = flight_id
        self.zip_id = zip_id
        self.launch_time = launch_time
        self.orders = orders
        self.route = route
        self.completion_time = completion_time
        self.route_times = route_times

    def __str__(self) -> str:
        return f"<Flight {self.flight_id} (Zip {self.zip_id}) @ {self.launch_time} Route: {' -> '.join(self.route)}>"



class ZipScheduler:
    def __init__(self, hospitals: Dict[str, Hospital]):
        self.hospitals = hospitals
        self._unfulfilled_orders: List[Order] = []
        self.zip_availability = {i: 0 for i in range(1, NUM_ZIPS + 1)}  # each Zip is available


    def calculate_segment_time(self, start_location: str, end_location: str) -> int:
        """
        Calculate the time for a Zip to travel between two locations.
        """
        start = Hospital("Nest", 0, 0) if start_location == "Nest" else self.hospitals[start_location]
        end = Hospital("Nest", 0, 0) if end_location == "Nest" else self.hospitals[end_location]

        distance = math.sqrt((end.north_m - start.north_m) ** 2 + (end.east_m - start.east_m) ** 2)
        return int(distance / ZIP_SPEED_MPS)


    def calculate_route_times(self, route: List[str]) -> List[int]:
        """
        Calculate flight time for each segment of the route.
        """
        return [self.calculate_segment_time(route[i], route[i + 1]) for i in range(len(route) - 1)]


    def get_available_zips(self, current_time: int) -> List[int]:
        """
        Get a list of Zip IDs that are available.
        """
        return [zip_id for zip_id, available_time in self.zip_availability.items() if current_time >= available_time]


    def queue_order(self, order: Order) -> None:
        """
        Add a new order to the unfulfilled order queue.
        """
        self._unfulfilled_orders.append(order)


    def launch_flights(self, current_time: int, flight_id_counter: int) -> Tuple[List[Flight], int]:
        """
        Assigns orders to available Zips and schedules flights.
        """
        flights = []
        if not self._unfulfilled_orders:
            return flights, flight_id_counter

        available_zips = self.get_available_zips(current_time)
        if not available_zips:
            return flights, flight_id_counter

        # sort orders: Emergency orders first, then by earliest time
        self._unfulfilled_orders.sort(key=lambda x: (x.priority != EMERGENCY, x.time))

        for zip_id in available_zips:
            if not self._unfulfilled_orders:
                break

            flight_orders = []
            route = ["Nest"]
            total_distance = 0
            last_location = (0, 0)  # start at Nest

            for order in list(self._unfulfilled_orders):
                if len(flight_orders) >= MAX_PACKAGES_PER_ZIP:
                    break  # stop adding orders when max capacity is reached

                hospital_location = (order.hospital.north_m, order.hospital.east_m)
                distance_to_hospital = math.sqrt(
                    (hospital_location[0] - last_location[0]) ** 2 +
                    (hospital_location[1] - last_location[1]) ** 2
                )
                return_distance = math.sqrt(
                    (hospital_location[0] - 0) ** 2 + (hospital_location[1] - 0) ** 2
                )

                if total_distance + distance_to_hospital + return_distance > ZIP_MAX_CUMULATIVE_RANGE_M:
                    break  # ensure the Zip does not exceed max cumulative flight range

                # add hospital to route
                route.append(order.hospital.name)
                flight_orders.append(order)
                total_distance += distance_to_hospital
                last_location = hospital_location

                self._unfulfilled_orders.remove(order)

            # return to Nest after last delivery
            if flight_orders:
                route.append("Nest")

                # calculate segment times
                route_times = self.calculate_route_times(route)
                total_flight_time = sum(route_times)
                completion_time = current_time + total_flight_time

                # mark Zip as unavailable until completion
                self.zip_availability[zip_id] = completion_time

                # store flight details
                flights.append(Flight(
                    flight_id=flight_id_counter,
                    zip_id=zip_id,
                    launch_time=current_time,
                    orders=flight_orders,
                    route=route,
                    completion_time=completion_time,
                    route_times=route_times
                ))
                flight_id_counter += 1

        return flights, flight_id_counter


class Runner:
    def __init__(self, hospitals_path: str, orders_path: str):
        with open(hospitals_path, "r") as f:
            self.hospitals = Hospital.load_from_csv(f)
        with open(orders_path, "r") as f:
            self.orders = Order.load_from_csv(f, self.hospitals)
        self.scheduler = ZipScheduler(hospitals=self.hospitals)
        self.flight_id_counter = 1
        self.flight_data = []

    def run(self) -> None:
        sec_per_day = 24 * 60 * 60
        for sec_since_midnight in range(self.orders[0].time, sec_per_day, 60):
            self.__queue_pending_orders(sec_since_midnight)
            flights, self.flight_id_counter = self.scheduler.launch_flights(sec_since_midnight, self.flight_id_counter)
            for flight in flights:
                for stop in range(len(flight.route) - 1):
                    self.flight_data.append([
                        flight.flight_id,
                        flight.zip_id,
                        flight.route[stop],
                        flight.route[stop + 1],
                        flight.launch_time
                    ])

    def __queue_pending_orders(self, sec_since_midnight: int) -> None:
        while self.orders and self.orders[0].time <= sec_since_midnight:
            self.scheduler.queue_order(self.orders.pop(0))

    def get_flight_dataframe(self) -> pd.DataFrame:
        return pd.DataFrame(self.flight_data, columns=["Flight ID", "Zip ID", "From", "To", "Launch Time"])


"""
Usage:

> python3 traveling_zip_project.py

Runs the provided CSVs file

"""


if __name__ == "__main__":
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  
    inputs_dir = os.path.join(root_dir, "inputs")  # inputs folder
    outputs_dir = os.path.join(root_dir, "outputs")  # outputs folder

    # ensure the outputs directory exists
    os.makedirs(outputs_dir, exist_ok=True)

    hospitals_path = os.path.join(inputs_dir, "hospitals.csv")
    orders_path = os.path.join(inputs_dir, "orders.csv")
    output_path = os.path.join(outputs_dir, "flight_schedule.csv")

    runner = Runner(
        hospitals_path=hospitals_path,
        orders_path=orders_path,
    )
    runner.run()

    # save results to CSV
    runner.get_flight_dataframe().to_csv(output_path, index=False)

