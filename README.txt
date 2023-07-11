Evan Springer

    The map function of each job (except SortByValue) takes each line of the Apache log file as a key,
and the contents of that line as the value. The map function of SortByValue takes the line of previous job's file
as the key, and the contents of that line as the value.
    For report 1, I used the URLCount and SortByValue map reduce job. The output of the URLCount map function is the
URL text as the key and a '1' as the value. For the reduce function, it sums the number of '1's and returns the URL
as key with the sum as the value. The SortByValue map function then takes the result of the URLCount job, and
returns the value as a key, with the key as the value. The reduce function, it writes out the value and key for each
key in the values.
    For report 2, I used CodeCount, which is nearly identical to URLCount, except it uses the response code as the
key.
    For report 3, I used TotalBytes with the hardcoded hostname "65-37-13-251.nrp2.roc.ny.frontiernet.net". The
output of the map function is the hostname as the key and the number of bytes sent in that record as the value. The
reduce function sums up all the bytes and returns the hostname with the total sum.
    For report 4, I used RequestCountPerClient and SortByValue again. The hardcoded URL is "/". The output of the
map function is the hostname as key, and a '1' as the value for every record that accessed "/". The reduce function
is nearly identical to CodeCount's reduce function. SortByValue then uses the output of RequestCountPerClient.
    For report 5, I used CalendarCount, which is nearly identical to URLCount, except it parses only the month and
year and uses that as the key.
    For report 6, I used TotalBytesByDay and SortByValue. TotalBytesByDay is nearly identical to TotalBytes, but
uses the calendar date as the key instead. SortByValue then uses the result of TotalBytesByDay.
