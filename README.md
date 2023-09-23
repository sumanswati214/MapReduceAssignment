# MapReduceAssignment
In this assignment, we will use the NYC TLC yellow taxi data set for the year 2017 and perform various operations using the big data tools.
For this assignment, we will use MRJob, a popular library created by Yelp for simplifying the process of writing MapReduce code with Apache Sqoop and Apache HBase.
We will also learn how to work with AWS RDS (Relational Database Service).

The data set for the assignment can be downloaded from these links:

https://nyc-tlc-upgrad.s3.amazonaws.com/yellow_tripdata_2017-01.csv

https://nyc-tlc-upgrad.s3.amazonaws.com/yellow_tripdata_2017-02.csv

https://nyc-tlc-upgrad.s3.amazonaws.com/yellow_tripdata_2017-03.csv

https://nyc-tlc-upgrad.s3.amazonaws.com/yellow_tripdata_2017-04.csv

https://nyc-tlc-upgrad.s3.amazonaws.com/yellow_tripdata_2017-05.csv

https://nyc-tlc-upgrad.s3.amazonaws.com/yellow_tripdata_2017-06.csv

# Dataset Description

The data set that will be used for this assignment is the TLC trip record data for yellow taxis.
Please note that the input data is for the year 2017.


* **VendorID**	  :           
A code indicating the TPEP provider that provided the record.1= Creative Mobile Technologies, LLC; 2= VeriFone Inc.
* **tpep_pickup_datetime** :
  The date and time when the meter was engaged
* **tpep_dropoff_datetime** :
The date and time when the meter was disengaged
* **passenger_count**	   :             The number of passengers in the vehicle. This is a driver-entered value
* **trip_distance**	      :            The elapsed trip distance in miles reported by the taximeter
* **RatecodeID**	         :           The final rate code in effect at the end of the trip.
1= Standard rate       2=JFK       3=Newark    4=Nassau or Westchester         5=Negotiated fare 6=Group ride
* **store_and_fwd_flag**	  :          This flag indicates whether the trip record was held in vehicle memory before sending to the vendor, aka “store and forward,” 
                                because the vehicle did not have a connection to the server.
                                Y= store and forward trip                N= not a store and forward trip
* **PULocationID**	    :              The ID of the location from where the passenger was picked.
* **DOLocationID**	   :               The ID of the location from where the passenger was dropped.
* **payment_type**	  :                A numeric code signifying how the passenger paid for the trip.
                               1= Credit card         2= Cash        3= No charge       4= Dispute       5= Unknown       6= Voided trip
* **fare_amount**      :              The time-and-distance fare calculated by the meter.
* **extra**	            :              Miscellaneous extras and surcharges. Currently, this only includes the $0.50 and $1 rush hour and overnight charges.
* **mta_tax**	           :             $0.50 MTA tax that is automatically triggered based on the metered rate in use.
* **tip_amount**	       :             This field is automatically populated for credit card tips. Cash tips are not included.
* **tolls_amount**	      :            Total amount of all tolls paid in trip .
* **improvement_surcharge** :        	$0.30 improvement surcharge assessed trips at the flag drop. The improvement surcharge began being levied in 2015.
* **total_amount**	         :         The total amount charged to passengers. Does not include cash tips.
* **Airport_fee**             :      	$1.25 for pick up only at LaGuardia and John F. Kennedy Airports

