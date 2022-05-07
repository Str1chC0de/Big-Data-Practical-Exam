-- Create table and use num as primary key. Num can not be null
CREATE TABLE xkcd_datatable (
    num int NOT NULL,
    year int,
    title varchar(255),
    img varchar(255),
    PRIMARY KEY (num)
);