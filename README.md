# data-challenge
Here is the database schema:

CREATE TABLE `max_trips` (\
  `pickup_date` date NOT NULL,\
  `pu_location_id` int(10) DEFAULT NULL,\
  `do_location_id` int(10) DEFAULT NULL,\
  `max_amount` decimal(10,2) DEFAULT NULL,\
  `taxi_type` varchar(30) NOT NULL,\
  `pickup_zone` varchar(50) DEFAULT NULL,\
  `dropoff_zone` varchar(50) DEFAULT NULL,\
  PRIMARY KEY (`pickup_date`,`taxi_type`)\
) ENGINE=InnoDB DEFAULT CHARSET=latin1;\
\
CREATE TABLE `min_trips` (\
  `pickup_date` date DEFAULT NULL,\
  `pu_location_id` text,\
  `do_location_id` text,\
  `min_amount` float DEFAULT NULL,\
  `taxi_type` text NOT NULL,\
  `pickup_zone` text,\
  `dropoff_zone` text\
) ENGINE=InnoDB DEFAULT CHARSET=latin1;\
\
CREATE TABLE `trip_revenue` (\
  `pickup_date` date NOT NULL,\
  `revenue` decimal(20,2) DEFAULT NULL,\
  `week_num` int(10) unsigned DEFAULT NULL,\
  `week_day` int(10) unsigned DEFAULT NULL,\
  `taxi_type` varchar(30) NOT NULL,\
  PRIMARY KEY (`pickup_date`,`taxi_type`)\
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
