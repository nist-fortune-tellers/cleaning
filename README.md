# Map-Reduce Algorithm Conversion!
## Pre-MR
Read `detector_lane_inventory.csv` and for each line in the CSV, we get the `lane_id` and `zone_id` and place it in the Hadoop Job as follows: `job.setLong(<lane_ID>, <zone_id>)`
## Map Reduceeeee
### Median Calculator (for 10 min window around all pts. in a zone)
#### Mapper
- Group by Zone ID
- Parse Time
- For Every Possible Placement in 10 minute range (ex. if I am 2:10, emit all time values between 2:05 and 2:15).
   - emit(<zone_id,associated_time>, <data>)
#### Reducer
- Retrieve <zone_id,associated_time> and Array of <data>s.
- Create Array of 
   - Flow
   - Changed
   - Reason
- Get Median From Array
- Get Std. Dev
- Iterate through array of flows
   - If Not within 1 Std. Dev.
      - Correct Flow to Median Value.
      - Set Changed = true
      - Set Reason to 2
   - Emit(<lane_id,measurment_start>, <Flow,Changed,Reason>)

- Conslidate Everything
- Sort By <lane_id>, then <measurment_start>, emit <Flow,Changed,Reason>

