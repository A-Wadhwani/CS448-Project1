# CS44800 Project 1
## Aryan Wadhwani (wadhwani@purude.edu)

## Implementation Notes
I used `txnum >= 0` to determine whether a buffer was in either array. To implement assertions in some portions of the code, I added `bufferpool[10000] = 0`, to ensure a crash occurs. 

## Testing + Profiling:
I look at hits and misses for the buffers. I look at various database operations that may be done, and also look at a general case where a randomized set of queries is made. 

![Example](DataExample.png) 

See `simpledb.buffer.BufferMgrProfiling.java`

## Visualizations
See `CS_448_Project_1_Analysis.ipynb` and `CS_447_Project_1_Group_Analysis.ipynb`

## Data for Visualizations (Output from Profiling) 
See `LRU Tests/` and `MRU Tests/`