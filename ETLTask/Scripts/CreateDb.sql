CREATE DATABASE ETLTask;

USE ETLTask;

CREATE TABLE TripData (
    Id INT IDENTITY PRIMARY KEY,
    PickupDateTime DATETIME NOT NULL,
    DropoffDateTime DATETIME NOT NULL,
    PassengerCount INT NOT NULL,
    TripDistance FLOAT NOT NULL,
    StoreAndFwdFlag NVARCHAR(3) NOT NULL,
    PULocationID INT NOT NULL,
    DOLocationID INT NOT NULL,
    FareAmount DECIMAL(10, 2) NOT NULL,
    TipAmount DECIMAL(10, 2) NOT NULL
);

CREATE INDEX idx_PULocationID_TipAmount ON TripData (PULocationId, TipAmount);
CREATE INDEX idx_TripDistance ON TripData (PULocationId, TripDistance DESC);
CREATE INDEX idx_Pickup_Dropoff ON TripData (PULocationId, PickupDateTime , DropoffDateTime );