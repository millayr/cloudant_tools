Run the utility by:  $ python BulkReplicationDriver.py -s <source account> -t <target account>

options:
-m <mediator>  :  The Cloudant user to mediate the replications. This
                  allows for a third party account to perform the
                  replications. (Default is to use the source user)

-d             :  Use this flag to append the date to the end of each
                  database name when it is replicated to the target.
                  This should be used when the source and target users
                  are the same. (Default is to not rename the databases)

-l <filename>  :  Pass a filename containing a list of databases to
                  replicate. Each database name should be on a separate
                  line.

-f             :  Use this flag to filter out design docs when
                  replicating. This will prevent index builds from
                  starting on the target. (Default is to replicate all
                  design docs)

-i <#>         :  Create incremental replications based on a previous id.

-z             :  Use this flag to override the replication concurrency
                  limit. Note this will impact performance and is not
                  recommended.

-c <#>         :  Set the concurrency limit for replications. (Default
                  is 8)

-p <#>         :  Set the time delay in seconds between polls to
                  Cloudant when the max concurrent replications have been
                  reached. (Default is 5)

-q <#>         :  Set the shard count for all databases replicated to the
                  target. (Default is to use the cluster default Q)

-o             :  Make all replications continuous. Ensure the
                  concurrency limit is high enough to support all
                  replications or disable the lmit all together.
                  (Default is to use one-off replications)

-w <#>         :  Set the number of worker processes per replication.
                  Increase the throughput at the cost of memory and CPU.
                  (Default is 4)

-b <#>         :  Set the batch size per worker process. Larger batch
                  size can offer better performance, while lower values
                  implies checkpointing is done more frequently.
                  (Default is 500)

-C <#>         :  Set the number of http connections per replication.
                  (Default is 20)

-T <#>         :  Set the timeout for http connections in milliseconds.
                  (Default is 300000)

-h             :  Display this help message.
