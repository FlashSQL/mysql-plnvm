# mysql-plnvm
Partitioned Logging in NVM implementation in InnoDB/MySQL 5.7

Author: Trong-Dat Nguyen @ VLDB Lab, Sungkyunkwan University, Korea


## Building from the source code
Step 1: Checking PMEM libraries required by PMDK. Ensure below libraries existed in your `/usr/local/lib/` or `/usr/lib/`:
* `libpmem.a libpmemblk.a libpmemlog.a libpmemobj.a libpmempool.a`

Step 2: Change the `BUILD_NAME` variable in the `build_mysql.sh` file to build your desired approach:
* Original InnoDB:
`BUILD_NAME="-DUNIV_TRACE_FLUSH_TIME"`
* PB-NVM
`BUILD_NAME="-DUNIV_OPENMP -DUNIV_PMEMOBJ_BLOOM -DUNIV_PMEMOBJ_BUF -DUNIV_PMEMOBJ_BUF_PARTITION -DUNIV_PMEMOBJ_BUF_FLUSHER -DUNIV_PMEMOBJ_BUF_RECOVERY"`

* PL-NVM: TBD

Step 3: Build the source code

`# ./build_mysql.sh`

## Addtional config variables:
* For all NVM-based methods:
```
innodb_pmem_home_dir=<your_mount_pmem>
innodb_pmem_pool_size=<val> # in MB
```
* For PB-NVM
```
innodb_pmem_buf_size=<val> # in MB
innodb_pmem_buf_n_buckets=<int>
innodb_pmem_buf_bucket_size=<int> # in pages
innodb_pmem_buf_flush_pct=<val> #threshold 0 - 1
innodb_pmem_n_flush_threads=<int> #must smaller than your max # of CPUs
innodb_pmem_flush_threshold=<int> #1 - innodb_pmem_n_flush_threads
innodb_pmem_bloom_n_elements=<val>
innodb_pmem_bloom_fpr=<val>
innodb_pmem_n_space_bits=<int>
innodb_pmem_page_per_bucket_bits=<int>
innodb_aio_n_slots_per_seg
```
* For PL-NVM: TBD
