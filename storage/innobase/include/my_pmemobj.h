/* 
 * Author; Trong-Dat Nguyen
 * MySQL REDO log with NVDIMM
 * Using libpmemobj
 * Copyright (c) 2017 VLDB Lab - Sungkyunkwan University
 * */


#ifndef __PMEMOBJ_H__
#define __PMEMOBJ_H__


#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>                                                                      
#include <sys/time.h> //for struct timeval, gettimeofday()
#include <string.h>
#include <stdint.h> //for uint64_t
#include <math.h> //for log()
#include <assert.h>
#include <wchar.h>
#include <unistd.h> //for access()

#include "univ.i"
#include "ut0byte.h"
#include "ut0rbt.h"
//#include "hash0hash.h" //for hashtable
#include "buf0buf.h" //for page_id_t
#include "page0types.h"
#include "ut0dbg.h"
#include "ut0new.h"

#include "trx0trx.h" //for trx_t
#include "dyn0buf.h" // for mtr_buf_t

//#include "pmem_log.h"
#include <libpmemobj.h>
#include "my_pmem_common.h"
//#include "pmem0buf.h"
//cc -std=gnu99 ... -lpmemobj -lpmem
#if defined (UNIV_PMEMOBJ_BUF)
//GLOBAL variables
static uint64_t PMEM_N_BUCKETS;
static uint64_t PMEM_BUCKET_SIZE;
static double PMEM_BUF_FLUSH_PCT;

static uint64_t PMEM_N_FLUSH_THREADS;
//set this to large number to eliminate 
//static uint64_t PMEM_PAGE_PER_BUCKET_BITS=32;

// 1 < this_value < flusher->size (32)
//static uint64_t PMEM_FLUSHER_WAKE_THRESHOLD=5;
static uint64_t PMEM_FLUSHER_WAKE_THRESHOLD=30;

static FILE* debug_file = fopen("part_debug.txt","a");

#if defined (UNIV_PMEMOBJ_BUF_PARTITION)
//256 buckets => 8 bits, max 32 spaces => 5 bits => need 3 = 8 - 5 bits
static uint64_t PMEM_N_BUCKET_BITS = 8;
static uint64_t PMEM_N_SPACE_BITS = 5;
static uint64_t PMEM_PAGE_PER_BUCKET_BITS=10;
#endif //UNIV_PMEMOBJ_BUF_PARTITION

#if defined (UNIV_PMEMOBJ_BLOOM)
static uint64_t PMEM_BLOOM_N_ELEMENTS;
static double PMEM_BLOOM_FPR;

#endif 
#endif //UNIV_PMEMOBJ_BUF


struct __pmem_wrapper;
typedef struct __pmem_wrapper PMEM_WRAPPER;

struct __pmem_dbw;
typedef struct __pmem_dbw PMEM_DBW;

struct __pmem_log_buf;
typedef struct __pmem_log_buf PMEM_LOG_BUF;

#if defined (UNIV_PMEMOBJ_BUF)
struct __pmem_buf_block_t;
typedef struct __pmem_buf_block_t PMEM_BUF_BLOCK;

struct __pmem_buf_block_list_t;
typedef struct __pmem_buf_block_list_t PMEM_BUF_BLOCK_LIST;

struct __pmem_buf_free_pool;
typedef struct __pmem_buf_free_pool PMEM_BUF_FREE_POOL;


struct __pmem_buf;
typedef struct __pmem_buf PMEM_BUF;


struct __pmem_list_cleaner_slot;
typedef struct __pmem_list_cleaner_slot PMEM_LIST_CLEANER_SLOT;

struct __pmem_list_cleaner;
typedef struct __pmem_list_cleaner PMEM_LIST_CLEANER;

struct __pmem_flusher;
typedef struct __pmem_flusher PMEM_FLUSHER;

struct __pmem_buf_bucket_stat;
typedef struct __pmem_buf_bucket_stat PMEM_BUCKET_STAT;

struct __pmem_file_map_item;
typedef struct __pmem_file_map_item PMEM_FILE_MAP_ITEM;

struct __pmem_file_map;
typedef struct __pmem_file_map PMEM_FILE_MAP;

struct __pmem_sort_obj;
typedef struct __pmem_sort_obj PMEM_SORT_OBJ;
#if defined(UNIV_PMEMOBJ_LSB)
struct __pmem_LSB;
typedef struct __pmem_LSB PMEM_LSB;


struct __pmem_lsb_hash_entry_t;
typedef struct __pmem_lsb_hash_entry_t PMEM_LSB_HASH_ENTRY;

struct __pmem_lsb_hash_bucket_t;
typedef struct __pmem_lsb_hash_bucket_t PMEM_LSB_HASH_BUCKET;

struct __pmem_lsb_hashtable_t;
typedef struct __pmem_lsb_hashtable_t PMEM_LSB_HASHTABLE;
#endif //UNIV_PMEMOBJ_LSB

#if defined (UNIV_PMEMOBJ_BLOOM)
#define BLOOM_SIZE 5000000
struct __pmem_bloom_filter;
typedef struct __pmem_bloom_filter PMEM_BLOOM;

struct __pmem_counting_bloom_filter;
typedef struct __pmem_counting_bloom_filter PMEM_CBF;

#define BLOOM_MAY_EXIST 0
#define BLOOM_NOT_EXIST -1
#endif //UNIV_PMEMOBJ_BLOOM

#endif //UNIV_PMEMOBJ_BUF

////////////////////////////////////////////////
// PL-NVM
// /////////////////////////////////////////////
#if defined (UNIV_PMEMOBJ_PL)
///////////////////////////////////////////////////
//New PL
//
// Per-TX logging
struct __pmem_tx_part_log;
typedef struct __pmem_tx_part_log PMEM_TX_PART_LOG;

struct __pmem_tx_log_hashed_line;
typedef struct __pmem_tx_log_hashed_line PMEM_TX_LOG_HASHED_LINE;

struct __pmem_tx_log_block;
typedef struct __pmem_tx_log_block PMEM_TX_LOG_BLOCK;

struct __pmem_dpt;
typedef struct __pmem_dpt PMEM_DPT;

struct __pmem_dpt_hashed_line;
typedef struct __pmem_dpt_hashed_line PMEM_DPT_HASHED_LINE;

struct __pmem_dpt_entry;
typedef struct __pmem_dpt_entry PMEM_DPT_ENTRY;

struct __pmem_page_ref;
typedef struct __pmem_page_ref PMEM_PAGE_REF;
// End Per-TX logging
//
// Per-Page Logging
struct __pmem_page_part_log;
typedef struct __pmem_page_part_log PMEM_PAGE_PART_LOG;

struct __pmem_page_log_buf;
typedef struct __pmem_page_log_buf PMEM_PAGE_LOG_BUF;

struct __pmem_page_log_free_pool;
typedef struct __pmem_page_log_free_pool PMEM_PAGE_LOG_FREE_POOL;

struct __pmem_page_log_hashed_line;
typedef struct __pmem_page_log_hashed_line PMEM_PAGE_LOG_HASHED_LINE;

struct __pmem_page_log_block;
typedef struct __pmem_page_log_block PMEM_PAGE_LOG_BLOCK;

struct __pmem_log_flusher;
typedef struct __pmem_log_flusher PMEM_LOG_FLUSHER;

struct __pmem_log_redoer;
typedef struct __pmem_log_redoer PMEM_LOG_REDOER;

struct __pmem_mini_buf_free_pool;
typedef struct __pmem_mini_buf_free_pool PMEM_MINI_BUF_FREE_POOL;

struct __pmem_mini_buf;
typedef struct __pmem_mini_buf  PMEM_MINI_BUF;

struct __pmem_tt;
typedef struct __pmem_tt PMEM_TT;

struct __pmem_tt_hashed_line;
typedef struct __pmem_tt_hashed_line PMEM_TT_HASHED_LINE;

struct __pmem_tt_entry;
typedef struct __pmem_tt_entry PMEM_TT_ENTRY;

struct __pmem_log_group;
typedef struct __pmem_log_group PMEM_LOG_GROUP;

struct __pmem_recv_line;
typedef struct __pmem_recv_line PMEM_RECV_LINE;

struct __pmem_space_t;
typedef struct __pmem_space_t PMEM_SPACE;

// End Per-Page Logging

//////////////////////////////////////////////////
#endif //UNIV_PMEMOBJ_PL


POBJ_LAYOUT_BEGIN(my_pmemobj);
POBJ_LAYOUT_TOID(my_pmemobj, char);
POBJ_LAYOUT_TOID(my_pmemobj, TOID(char));
POBJ_LAYOUT_TOID(my_pmemobj, PMEM_LOG_BUF);
POBJ_LAYOUT_TOID(my_pmemobj, PMEM_DBW);

#if defined(UNIV_PMEMOBJ_BUF)
POBJ_LAYOUT_TOID(my_pmemobj, PMEM_BUF);
POBJ_LAYOUT_TOID(my_pmemobj, PMEM_BUF_FREE_POOL);
POBJ_LAYOUT_TOID(my_pmemobj, PMEM_BUF_BLOCK_LIST);
POBJ_LAYOUT_TOID(my_pmemobj, TOID(PMEM_BUF_BLOCK_LIST));
POBJ_LAYOUT_TOID(my_pmemobj, PMEM_BUF_BLOCK);
POBJ_LAYOUT_TOID(my_pmemobj, TOID(PMEM_BUF_BLOCK));
#if defined(UNIV_PMEMOBJ_LSB)
POBJ_LAYOUT_TOID(my_pmemobj, PMEM_LSB);
POBJ_LAYOUT_TOID(my_pmemobj, PMEM_LSB_HASHTABLE);
#endif //UNIV_PMEMOBJ_LSB

#endif //UNIV_PMEMOBJ_BUF

#if defined(UNIV_PMEMOBJ_PL)

// Per-TX Logging
POBJ_LAYOUT_TOID(my_pmemobj, PMEM_TX_PART_LOG);
POBJ_LAYOUT_TOID(my_pmemobj, TOID(PMEM_TX_LOG_HASHED_LINE));
POBJ_LAYOUT_TOID(my_pmemobj, PMEM_TX_LOG_HASHED_LINE);
POBJ_LAYOUT_TOID(my_pmemobj, TOID(PMEM_TX_LOG_BLOCK));
POBJ_LAYOUT_TOID(my_pmemobj, PMEM_TX_LOG_BLOCK);
POBJ_LAYOUT_TOID(my_pmemobj, uint64_t);
POBJ_LAYOUT_TOID(my_pmemobj, int64_t);

POBJ_LAYOUT_TOID(my_pmemobj, PMEM_DPT);
POBJ_LAYOUT_TOID(my_pmemobj, TOID(PMEM_DPT_HASHED_LINE));
POBJ_LAYOUT_TOID(my_pmemobj, PMEM_DPT_HASHED_LINE);
POBJ_LAYOUT_TOID(my_pmemobj, TOID(PMEM_DPT_ENTRY));
POBJ_LAYOUT_TOID(my_pmemobj, PMEM_DPT_ENTRY);
POBJ_LAYOUT_TOID(my_pmemobj, TOID(PMEM_PAGE_REF));
POBJ_LAYOUT_TOID(my_pmemobj, PMEM_PAGE_REF);
// End Per-TX Logging

// Per-Page Logging
POBJ_LAYOUT_TOID(my_pmemobj, PMEM_PAGE_PART_LOG);
POBJ_LAYOUT_TOID(my_pmemobj, PMEM_PAGE_LOG_BUF);
POBJ_LAYOUT_TOID(my_pmemobj, PMEM_PAGE_LOG_FREE_POOL);

POBJ_LAYOUT_TOID(my_pmemobj, TOID(PMEM_PAGE_LOG_HASHED_LINE));
POBJ_LAYOUT_TOID(my_pmemobj, PMEM_PAGE_LOG_HASHED_LINE);
POBJ_LAYOUT_TOID(my_pmemobj, TOID(PMEM_PAGE_LOG_BLOCK));
POBJ_LAYOUT_TOID(my_pmemobj, PMEM_PAGE_LOG_BLOCK);
//POBJ_LAYOUT_TOID(my_pmemobj, uint64_t);
//POBJ_LAYOUT_TOID(my_pmemobj, int64_t);

POBJ_LAYOUT_TOID(my_pmemobj, PMEM_MINI_BUF_FREE_POOL);
POBJ_LAYOUT_TOID(my_pmemobj, PMEM_MINI_BUF);

POBJ_LAYOUT_TOID(my_pmemobj, PMEM_TT);
POBJ_LAYOUT_TOID(my_pmemobj, TOID(PMEM_TT_HASHED_LINE));
POBJ_LAYOUT_TOID(my_pmemobj, PMEM_TT_HASHED_LINE);
POBJ_LAYOUT_TOID(my_pmemobj, TOID(PMEM_TT_ENTRY));
POBJ_LAYOUT_TOID(my_pmemobj, PMEM_TT_ENTRY);

POBJ_LAYOUT_TOID(my_pmemobj, PMEM_SPACE);
POBJ_LAYOUT_TOID(my_pmemobj, TOID(PMEM_SPACE));

// End Per-page logging
//
#endif // UNIV_PMEMOBJ_PL

POBJ_LAYOUT_END(my_pmemobj);


////////////////////////// THE WRAPPER ////////////////////////
/*The global wrapper*/
struct __pmem_wrapper {
	char name[PMEM_MAX_FILE_NAME_LENGTH];
	PMEMobjpool* pop;
	PMEM_LOG_BUF* plogbuf;
	PMEM_DBW* pdbw;
#if defined (UNIV_PMEMOBJ_BUF)
	PMEM_BUF* pbuf;
#endif
#if defined (UNIV_PMEMOBJ_PL)
	PMEM_TX_PART_LOG* ptxl;
	PMEM_PAGE_PART_LOG* ppl;
#endif

#if defined (UNIV_PMEMOBJ_LSB)
	PMEM_LSB* plsb;
#endif
#if defined (UNIV_PMEM_SIM_LATENCY)
	uint64_t PMEM_SIM_CPU_CYCLES; //used in simulate latency
#endif
	bool is_new;
};



/* FUNCTIONS*/

PMEM_WRAPPER*
pm_wrapper_create(
		const char*		path,
	   	const size_t	pool_size);

void
pm_wrapper_free(PMEM_WRAPPER* pmw);


PMEMoid pm_pop_alloc_bytes(PMEMobjpool* pop, size_t size);
void pm_pop_free(PMEMobjpool* pop);

#if defined (UNIV_PMEM_SIM_LATENCY)
#define PMEM_DELAY(start, end, t) do {\
	start = my_timer_cycles();\
	end = start;\
	while( (end - start) < t){\
		end = my_timer_cycles();\
	}\
} while(0)
#endif
//////////////////// PARTITIONED-LOG 2018.11.2/////////////

#if defined (UNIV_PMEMOBJ_PL)

////////////////////////////////////////////////////
////////////////////// NEW PART LOG IMPLEMENT////////////
//////////////////////////////////////////////////

////////////// Per-Transaction Logging//////////////

/*Implement the PMEM log in the similar way with the PMEM_BUF
 *A sequential bytes are allocated in PMEM to avaoid allocate/deallocate overhead
 * */
struct __pmem_tx_part_log {
	/*metadata*/
	uint64_t	size; //the total size for the partitioned log
	TOID(PMEM_DPT)	dpt; //dirty page table

	/*pmem address*/
	PMEMoid		data; //log data
	byte*		p_align; //align

	bool		is_new;
	
	/*log data as hash table*/
	uint64_t			n_buckets; //# of buckets
	TOID_ARRAY(TOID(PMEM_TX_LOG_HASHED_LINE)) buckets;

	uint64_t			n_blocks_per_bucket; //# load_factor, of log block per bucket
	uint64_t			block_size; //log block size in bytes
	

	/*Statistic info*/	
	uint64_t	pmem_alloc_size;
	uint64_t	pmem_dpt_size;
	uint64_t	pmem_tx_log_size;

	/*Debug*/
	FILE* deb_file;

};
/*
 * A hashed line has array of log blocks share the same hashed value
 * */
struct __pmem_tx_log_hashed_line {
	PMEMrwlock		lock;
	
	int hashed_id;

	uint64_t n_blocks; //the total log block in bucket
	uint64_t cur_block_id; //current free block id, follow the round-robin fashion

	TOID_ARRAY(TOID(PMEM_TX_LOG_BLOCK)) arr;

};
/*
 * One log block per transaction
 * Follow the implementation of PMEM_BUF_BLOCK
 * The REDO logs of a transactions are append on the log block
 * */
struct __pmem_tx_log_block {
	PMEMrwlock		lock;
	uint64_t				bid; //block id
	uint64_t				tid; //transaction id
	uint64_t				pmemaddr; //the begin offset to the pmem data in PMEM_TX_PART_LOG
	uint64_t				cur_off; //the current offset (0 - log block size) 
	uint64_t				n_log_recs; //the current number of log records 
	PMEM_LOG_BLOCK_STATE	state;

	int32_t				count; //number of dirty pages caused by this transaction

	/*update this array first, then update the dirty page table*/
	TOID_ARRAY(TOID(PMEM_PAGE_REF))			dp_array; //dirty page array consists of key of entry in dirty page table
	uint64_t				n_dp_entries; 

	//debug only
	uint64_t				prev_tid; //transaction id

#if defined (UNIV_PMEMOBJ_PART_PL_STAT)
	/*Statistic information */
	//Overall stat info for the whole lifetime
	uint64_t				all_n_reused; // 1 reused = FREE --> ACTIVE --> FREE
	float					all_avg_small_log_recs;
	float					all_avg_log_rec_size;
	uint64_t				all_min_log_rec_size;
	uint64_t				all_max_log_rec_size;
	
	uint64_t				all_max_log_buf_size;
	uint64_t				all_avg_log_buf_size;

	uint64_t				all_max_n_recs;
	uint64_t				all_avg_n_recs;
	uint64_t				all_max_n_pagerefs;

	float					all_avg_block_lifetime;

	//Per log block info, only available in the lifetime of transaction. Reset after trx commit/abort
	uint64_t				n_small_log_recs; // number of small log record < CACHELINE_SIZE 

	uint64_t				avg_log_rec_size; // average log records size in this block
	uint64_t				min_log_rec_size; // minimum log record size in this block
	uint64_t				max_log_rec_size; // maximum log record size in this block 

	uint64_t				avg_pageref_size;
	ulint					lifetime; //time of the first access to this block, usage: ut_time_us(), ut_time_ms(), ut_difftime()
	ulint					start_time; //time of the first access to this block, usage: ut_time_us(), ut_time_ms(), ut_difftime()
#endif //UNIV_PMEMOBJ_PART_PL_STAT
	
};


//Dirty Page Table Entry
struct __pmem_dpt_entry {
	PMEMrwlock		lock; 
	bool			is_free; //flag
	uint64_t		key; //fold of page_id_t.fold() in InnoDB
	int64_t		eid; //entry id in the DPT
	int32_t		count; //number of current active transactions
	uint64_t		pageLSN;

	TOID(int64_t)	tx_idx_arr; //array of transaction index
	uint16_t	    n_tx_idx; //array length

#if defined (UNIV_PMEMOBJ_PART_PL_STAT)
	uint64_t		n_reused; 
	uint64_t		max_txref_size; 
#endif
};
// A DPT hashed line in the DPT
struct __pmem_dpt_hashed_line {
	PMEMrwlock		lock; 

	uint64_t hashed_id;
	uint64_t n_entries; // # of entries in line
	TOID_ARRAY(TOID(PMEM_DPT_ENTRY)) arr;
#if defined (UNIV_PMEMOBJ_PART_PL_STAT)
	uint64_t n_free; //number of free entry
	uint64_t n_idle; //number of entries have count 0 and not free
#endif
};

/*Dirty Page Table*/
struct __pmem_dpt {
	uint64_t	n_buckets; 
	uint64_t	n_entries_per_bucket;
	TOID_ARRAY(TOID(PMEM_DPT_HASHED_LINE)) buckets;
};
/////////////// Per-Page Logging ///////////////////

/*
 * The high-level struc of page-level logging
 * */
struct __pmem_page_part_log {
	//we only use this lock to protect below values
	PMEMrwlock		ckpt_lock;
	PMEMrwlock		recv_lock;
	uint64_t	max_oldest_lsn;
	uint64_t	min_oldest_lsn;
	uint64_t	ckpt_lsn;
	/// end lock
	
	/*Space array, used to reconstruct space when recovery*/	
	TOID_ARRAY(TOID(PMEM_SPACE)) space_arr;
	uint16_t		n_spaces;
	/*log buffer area*/

	uint64_t			size; //the total size 
	PMEMoid				data; //pointer to allocated nvm
	byte*				p_align; //align

	uint64_t						log_buf_size; //log block size in bytes
	uint64_t						n_log_bufs;

	// Transaction Table
	TOID(PMEM_TT) tt; // transaction table	

	bool		is_new;
	/*log area as hash table*/
	uint64_t			n_buckets; //# of buckets
	TOID_ARRAY(TOID(PMEM_PAGE_LOG_HASHED_LINE)) buckets;

	uint64_t			n_blocks_per_bucket; //# load_factor, of log block per bucket

	/* 
	 * DRAM objects, alloc every time the server start
	 * */

	/*Flusher*/	
	os_event_t free_log_pool_event; //event for free_pool
	PMEM_LOG_FLUSHER*	flusher;
	TOID(PMEM_PAGE_LOG_FREE_POOL)	free_pool;
	
	/*RECOVERY*/
	PMEM_RECV_LINE* recv_line; /*the global recv_line*/

	/*Redoer*/	
	PMEM_LOG_REDOER*	redoer;
	uint16_t			n_redoing_lines; /*# lines are redoing*/
	bool				is_redoing_done; /*true iff n_redoing_lines == 0*/
	os_event_t redoing_done_event; //event for redoing

	/*DRAM Log File*/	
	uint64_t			log_file_size;
	pfs_os_file_t		log_files[4096];
	uint64_t			n_log_files_per_bucket;
	PMEM_LOG_GROUP**	log_groups; //n_buckets groups
	fil_node_t**		node_arr; // n_buckets fil_nodes
	fil_space_t*		log_space;
	/*Statistic info*/	
	//log area
	uint64_t			pmem_alloc_size;
	
	//metadata
	uint64_t			pmem_page_log_size;
	uint64_t			pmem_page_log_free_pool_size;
	uint64_t			pmem_mini_free_pool_size;
	uint64_t			pmem_tt_size;

	/*Debug*/
	FILE*				deb_file;

};

struct __pmem_space_t {
	char name[256];
	uint32_t space_no;
};

struct plog_hash_t {
	uint64_t	key;
	uint32_t	block_off; /*block offset in the line*/
	hash_node_t	addr_hash;/*!< hash node in the hash bucket chain */
};

/*
 * A hashed line has array of log blocks share the same hashed value and log buffer
 * */
struct __pmem_page_log_hashed_line {
	/*general lock protect data in PART 1 and PART 3*/
	PMEMrwlock		lock;
	
	/*PART 1, writing log rec to logbuf*/
	int hashed_id;
	TOID(PMEM_PAGE_LOG_BUF)	logbuf;	//pointer to current log buffer (head)
	TOID(PMEM_PAGE_LOG_BUF)	tail_logbuf;	//pointer to the oldest log buffer (tail)

	os_event_t		log_flush_event;
	bool			is_flushing;

	uint64_t		diskaddr; //log file offset, update when flush log, reset when purging file
	uint64_t		write_diskaddr; //diskaddr that log recs are durable write write_diskaddr < diskaddr

	/*for checkpoint*/	
	uint32_t		oldest_block_off;//offset of the block has min (start_diskaddr + start_off)
	uint64_t		ckpt_lsn;
	bool			is_req_checkpoint;

	/* recovery */
	uint64_t		recv_diskaddr; //the diskaddr begin the recover, min of log blocks diskaddr
	uint64_t		recv_off; //the offset begin the recover
	uint64_t		recv_lsn; //min lsn of block's beginLSN in this line
	
	/*PART 2, metadata: log_block array, hashtable, std::map*/
	PMEMrwlock		meta_lock;

	TOID_ARRAY(TOID(PMEM_PAGE_LOG_BLOCK)) arr;
	uint64_t n_blocks; //the current non-free blocks
	uint64_t max_blocks; //the total log block in bucket
	long long* bit_arr; //bit array to manage free slots
	uint16_t n_bit_blocks; //number of block in bit_arr

	/*Hash table*/
	hash_table_t* addr_hash; //hash the log block in this line
	
	std::map<uint64_t, uint32_t>* offset_map;

	/*PART 3: recovery*/
	//Alternative to recv_sys_t in InnoDB, allocate in DRAM when recovery
	PMEM_RECV_LINE* recv_line;
	bool			is_redoing;

	/*Statistic info*/
#if defined(UNIV_PMEMOBJ_PPL_STAT)
	uint64_t log_write_lock_wait_time; //total time lock holding for copying log records to log buffer
	uint64_t n_log_write; //total copies

	uint64_t log_flush_lock_wait_time; //total time of lock holding for flushing log buffer to disk 
	uint64_t n_log_flush; //total 
#endif
};


struct __pmem_page_log_free_pool {
	PMEMrwlock			lock;
	POBJ_LIST_HEAD(buf_list, PMEM_PAGE_LOG_BUF) head;
	size_t				cur_free_bufs;
	size_t				max_bufs;
};


struct __pmem_page_log_buf {
	PMEMrwlock				lock;

	PMEMoid					self; //self reference

	uint64_t				pmemaddr; //the begin offset to the pmem data in PMEM_PAGE_PART_LOG

	uint64_t				id;
	int64_t					hashed_id;

	PMEM_LOG_BUF_STATE		state;
	uint64_t				size;
	uint64_t				cur_off; //the current offset (0 - log buf size), reset when switching log_buf 
	uint64_t				n_recs;

	int						check; //for AIO
	
	uint64_t				diskaddr; //write address assigned when the buffer is full

	TOID(PMEM_PAGE_LOG_BUF) prev;
	TOID(PMEM_PAGE_LOG_BUF) next;

	//link with the list free pool
	POBJ_LIST_ENTRY(PMEM_PAGE_LOG_BUF) list_entries;
};

/*
 * One log block per-page
 * Metadata of a page during forward processing
 * Persistent after the system crash
 * */
struct __pmem_page_log_block {
	PMEMrwlock		lock;

	PMEM_BLOCK_STATE state;

	bool			is_free; //flag
	uint64_t		bid; //block id
	uint32_t		id; //id on the line
	uint64_t		key; //fold id

	int32_t			count; //number of active tx

	/*LSN */
	uint64_t		pageLSN; // pageLSN of the NVM-page
	uint64_t		lastLSN; // LSN of the last log record

	uint64_t		firstLSN; // LSN of the first log record
	uint32_t		start_off;// offset of the first log rec of this page on log buffer/ disk
	uint32_t		start_diskaddr;// diskaddr when the first log rec is written 

	//this value is used for testing recovery
	bool			first_rec_found;
	uint32_t		first_rec_size;
	mlog_id_t		first_rec_type;
};

/*
 * per-line mini recovery system
 * folow the design of recv_sys_t in InnoDB
 */
struct __pmem_recv_line {
	PMEMrwlock		lock;

	uint32_t	hashed_id;
	
	bool		skip_zero_page;
	bool		apply_log_recs;
	bool		apply_batch_on;

	byte*		buf;	/*!< buffer for parsing log records */
	ulint		len;	/*!< amount of data in buf */

	lsn_t		parse_start_lsn;
	lsn_t		scanned_lsn;
	ulint		scanned_checkpoint_no;

	ulint		recovered_offset;
	ulint		recovered_addr;
	lsn_t		recovered_lsn;

	bool		found_corrupt_log;
	bool		found_corrupt_fs;

	bool		is_ibuf_avail; //used in applying phase

	lsn_t		mlog_checkpoint_lsn;

	mem_heap_t*	heap;	/*!< memory heap of log records and file addresses */
	ulint		alloc_hash_size; //allocated heap size
	hash_table_t*	addr_hash;/*!< hash table of file addresses of pages */
	ulint			n_addrs;/*!< number of not processed pages in the hash table */
	ulint			n_skip_done;/*!< number of not processed pages in the hash table */
	ulint			n_cache_done;/*!< number of not processed pages in the hash table */
	ulint			n_read_reqs; /*number of read request in phase 2 REDO */
	ulint			n_read_done; /*number of read request in phase 2 REDO */

	recv_dblwr_t	dblwr;
	encryption_list_t* encryption_list;

	/*statistic info*/
	ulint		redo1_thread_id;
	ulint		redo1_start_time;
	ulint		redo1_end_time;
	ulint		redo1_elapse_time;

	ulint		redo2_thread_id;
	ulint		redo2_start_time;
	ulint		redo2_end_time;
	ulint		redo2_elapse_time;
};

////////////////     FLUSHER         /////////////////
/*
 * FLUSHER - Handle async flus logs to disk
 * */
struct __pmem_log_flusher {
	ib_mutex_t			mutex;
	//for worker
	os_event_t			is_log_req_not_empty; //signaled when there is a new flushing list added
	os_event_t			is_log_req_full;

	os_event_t			is_log_all_finished; //signaled when all workers are finished flushing and no pending request 
	os_event_t			is_log_all_closed; //signaled when all workers are closed 
	volatile ulint n_workers;
	//the waiting_list
	ulint size;
	ulint tail; //always increase, circled counter, mark where the next flush list will be assigned
	ulint n_requested;
	//ulint n_flushing;

	bool is_running;
	FLUSHER_TYPE type; //flusher type , FLUSHER)

	//pointer array
	PMEM_PAGE_LOG_BUF** flush_list_arr;

};

PMEM_LOG_FLUSHER*
pm_log_flusher_init(
				const size_t	size,
				FLUSHER_TYPE type);
void
pm_log_flusher_close(PMEM_LOG_FLUSHER*	flusher);

void
pm_log_buf_assign_flusher(
		PMEM_PAGE_PART_LOG*			ppl,
		PMEM_PAGE_LOG_BUF*	plogbuf);

//log flusher worker call back
void 
pm_log_flush_log_buf(
		PMEMobjpool*			pop,
		PMEM_PAGE_PART_LOG*		ppl,
		PMEM_PAGE_LOG_BUF*		plogbuf);

void
pm_handle_finished_log_buf(
		PMEMobjpool*			pop,
		PMEM_PAGE_PART_LOG*		ppl,
		fil_node_t*				node,
		PMEM_PAGE_LOG_BUF*		plogbuf);

void
pm_log_fil_io(
		PMEMobjpool*			pop,
		PMEM_PAGE_PART_LOG*		ppl,
		PMEM_PAGE_LOG_BUF*		plogbuf,
		fil_node_t*			node,
		const IORequest&	type,
		bool			sync,
		const page_id_t&	page_id,
		const page_size_t&	page_size,
		ulint			byte_offset,
		ulint			len,
		void*			buf,
		void*			message);
void
pm_log_fil_io(
		PMEMobjpool*			pop,
		PMEM_PAGE_PART_LOG*		ppl,
		PMEM_PAGE_LOG_BUF*		plogbuf);

dberr_t
pm_log_fil_read(
	PMEMobjpool*			pop,
	PMEM_PAGE_PART_LOG*		ppl,
	PMEM_PAGE_LOG_HASHED_LINE*		pline,
	byte* buf,
	uint64_t offset,
	uint64_t len
	);
extern "C"
os_thread_ret_t
DECLARE_THREAD(pm_log_flusher_coordinator)(
		void* arg);

extern "C"
os_thread_ret_t
DECLARE_THREAD(pm_log_flusher_worker)(
		void* arg);
////////////////     END FLUSHER   /////////////////

////////////////    REDOER   /////////////////
/*
 * Handle parallelism REDOing in recovery
 * Similar to FLUSHER
 * */
struct __pmem_log_redoer {
	ib_mutex_t			mutex;
	//for worker
	os_event_t			is_log_req_not_empty; //signaled when there is a new flushing list added
	os_event_t			is_log_req_full;

	os_event_t			is_log_all_finished; //signaled when all workers are finished flushing and no pending request 
	os_event_t			is_log_all_closed; //signaled when all workers are closed 

	uint16_t			phase;/*1: scan and parse, 2: apply*/
	volatile ulint n_workers;
	//the waiting_list
	ulint size;
	ulint tail; //always increase, circled counter, mark where the next line  will be assigned
	ulint n_requested;
	ulint n_remains;// number of remain lines need to REDO

	bool is_running;
	//pointer array
	PMEM_PAGE_LOG_HASHED_LINE** hashed_line_arr;
};

PMEM_LOG_REDOER*
pm_log_redoer_init(
				const size_t	size);
void
pm_log_redoer_close(PMEM_LOG_REDOER*	redoer);

extern "C"
os_thread_ret_t
DECLARE_THREAD(pm_log_redoer_coordinator)(
		void* arg);

extern "C"
os_thread_ret_t
DECLARE_THREAD(pm_log_redoer_worker)(
		void* arg);
////////////////    END REDOER   /////////////////

/*Transaction Table Entry*/

// Dirty Page Entry Ref in the log block
struct __pmem_page_ref {
	uint64_t	key; //fold of page_id_t.fold() in InnoDB
	//int64_t		idx; // index of the entry in DPT
	uint64_t		idx; // index of the entry in DPT
	uint64_t	pageLSN; //latest LSN on this page caused by the host transction, this value used in reclaiming logs
};

struct __pmem_mini_buf_free_pool{
	PMEMrwlock			lock;
	POBJ_LIST_HEAD(mbuf_list, PMEM_MINI_BUF) head;
	size_t				cur_free_bufs;
	size_t				max_bufs;
};

struct __pmem_mini_buf {
	PMEMoid					self; //self reference
	uint32_t				id;

	TOID(char)				buf;
	uint32_t				cur_buf_size;
	uint32_t				max_buf_size;
	bool					is_new_write;
	uint64_t				start_time;

	PMEMoid				entry_oid;//pointer back to the tt entry caller
	POBJ_LIST_ENTRY(PMEM_MINI_BUF) mlist_entries; //link to the free pool
};

struct __pmem_tt_entry {
	PMEMrwlock		lock; //the major lock protect below fields
	uint64_t		eid; //block id
	uint64_t		tid; //transaction id
	PMEM_TX_STATE	state;
	/*temp buffer*/
	TOID(PMEM_MINI_BUF) mbuf;
	bool		is_commit;


	uint16_t n_mbuf_reqs; //number of mbuf is on processing
	PMEMrwlock		pref_lock; //the pref_lock protect the array of pref, used in partition 
	/*dirty page array consists of (key, bid) pair of dirty pages caused by this transaction*/
	TOID_ARRAY(TOID(PMEM_PAGE_REF))		dp_arr;
	uint64_t							n_dp_entries; 
	uint64_t							max_dp_entries; //this value may differ to n_pref_per_entry in PMEM_TT due to the realloc mechanism
	
};

struct __pmem_tt_hashed_line {
	PMEMrwlock		lock;

	uint64_t		hashed_id;

	TOID_ARRAY(TOID(PMEM_TT_ENTRY)) arr;
	uint64_t		n_entries; //current number of entries
	uint64_t		max_entries; //capacity (extendable)
};

struct __pmem_tt {
	TOID_ARRAY(TOID(PMEM_TT_HASHED_LINE)) buckets;
	uint64_t	n_buckets; 
	
	TOID(PMEM_TT_HASHED_LINE) spec_bucket; //the special bucket to handle NULL trx

	uint64_t	n_entries_per_bucket;
	uint64_t	n_pref_per_entry;
};

struct __pmem_log_group {
	uint64_t	id;
	uint64_t	n_files;
	uint64_t	format;
	uint64_t	file_size;
	uint64_t	space_id;

	log_group_state_t		state;

	/** lsn used to fix coordinates within the log group */
	lsn_t				lsn;
	/** the byte offset of the above lsn */
	lsn_t				lsn_offset;
	/** unaligned buffers */
	byte**				file_header_bufs_ptr;
	/** buffers for each file header in the group */
	byte**				file_header_bufs;
};


/////////////// End Per-Page Logging ///////////////////


////////////// NEW PARTITIONED LOG ////////////////////
////////// PER-TX LOGGING/////////////////////////
void
pm_wrapper_tx_log_alloc_or_open(
		PMEM_WRAPPER*	pmw,
		uint64_t		n_buckets,
		uint64_t		n_blocks_per_bucket,
		uint64_t		block_size);

PMEM_TX_PART_LOG* alloc_pmem_tx_part_log(
		PMEMobjpool*	pop,
		uint64_t		n_buckets,
		uint64_t		n_blocks_per_bucket,
		uint64_t		block_size);
void 
pm_tx_part_log_bucket_init(
		PMEMobjpool*		pop,
		PMEM_TX_PART_LOG*		pl,
		uint64_t			n_buckets,
		uint64_t			n_blocks_per_bucket,
		uint64_t			block_size); 

void 
__init_dpt(
		PMEMobjpool*		pop,
		PMEM_TX_PART_LOG*		ptxl,
		uint64_t n,
		uint64_t k);

/*Called when a mini-transaction write log record to log buffer in the traditional InnoDB*/
int64_t
pm_ptxl_write(
			PMEMobjpool*		pop,
			PMEM_TX_PART_LOG*		ptxl,
			uint64_t			tid,
			byte*				log_src,
			uint64_t			size,
			uint64_t			n_recs,
			uint64_t*			key_arr,
			uint64_t*			LSN_arr,
			uint64_t*			size_arr,
			uint64_t*			space_arr,
			uint64_t*			page_arr,
			int64_t			block_id);

void
pm_ptxl_commit(
		PMEMobjpool*		pop,
		PMEM_TX_PART_LOG*		ptxl,
		uint64_t tid,
		int64_t bid);
void 
pm_ptxl_on_flush_page(
		PMEMobjpool*		pop,
		PMEM_TX_PART_LOG*	ptxl,
		uint64_t			key,
		uint64_t			pageLSN);


int64_t
__update_dpt_entry_on_write_log(
		PMEMobjpool*		pop,
		PMEM_TX_PART_LOG*	ptxl,
		uint64_t			tid,
		uint64_t			key); 

bool
__update_dpt_entry_on_commit(
		PMEMobjpool*		pop,
		PMEM_DPT*			pdpt,
		PMEM_PAGE_REF* pref);
void 
__reset_DPT_entry(PMEM_DPT_ENTRY* pe);
void 
__reset_tx_log_block(PMEM_TX_LOG_BLOCK* plog_block);

bool
__check_and_reclaim_tx_log_block(
		PMEMobjpool*		pop,
		PMEM_TX_PART_LOG*	ptxl,
		int64_t			block_id);
bool
pm_ptxl_check_and_reset_dpt_entry(
		PMEMobjpool*		pop,
		PMEM_DPT*			pdpt,
		uint64_t			key);


#if defined (UNIV_PMEMOBJ_PART_PL_STAT)
void 
__print_page_blocks_state(FILE* f,
		PMEM_PAGE_PART_LOG* ppl);
void 
__print_tx_blocks_state(FILE* f,
		PMEM_TX_PART_LOG* ptxl);
void
ptxl_consolidate_stat_info(PMEM_TX_LOG_BLOCK*	plog_block);
void 
ptxl_print_log_block_stat_info (FILE* f, PMEM_TX_LOG_BLOCK* plog_block);
void 
ptxl_print_all_stat_info (FILE* f, PMEM_TX_PART_LOG* ptxl);
void
__print_DPT(
		PMEM_DPT*			pdpt,
		FILE* f);
void
__print_TT(
		FILE* f,
		PMEM_TT*			ptt);
#endif
///////////////////// END PER-TX LOGGING /////////////

////////////////// PER-PAGE LOGGING //////////////////

#define PMEM_FOLD(fold, space, page_no) do {\
	fold = (space << 20) + space + page_no;\
}while(0)

PMEM_PAGE_PART_LOG* 
pm_pop_get_ppl (PMEMobjpool* pop);

void
pm_wrapper_page_log_alloc_or_open(
		PMEM_WRAPPER*	pmw
		);

void
pm_wrapper_page_log_close(
		PMEM_WRAPPER*	pmw);

PMEM_PAGE_PART_LOG* alloc_pmem_page_part_log(
		PMEMobjpool*	pop,
		uint64_t		n_buckets,
		uint64_t		n_blocks_per_bucket,
		uint64_t		n_log_bufs,
		uint64_t		log_buf_size);

void 
pm_page_part_log_bucket_init(
		PMEMobjpool*		pop,
		PMEM_PAGE_PART_LOG*		ppl,
		uint64_t			n_buckets,
		uint64_t			n_blocks_per_bucket,
		uint64_t			log_buf_size,
		uint64_t			&log_buf_id,
		uint64_t			&log_buf_offset
		); 
void 
pm_ppl_init_in_mem(
		PMEMobjpool*		pop,
		PMEM_PAGE_PART_LOG*		ppl);

void 
pm_ppl_free_in_mem(
		PMEMobjpool*		pop,
		PMEM_PAGE_PART_LOG*		ppl);
void
pm_page_part_log_hash_create(
		PMEMobjpool*		pop,
		PMEM_PAGE_PART_LOG*		ppl);
void
pm_page_part_log_hash_free(
		PMEMobjpool*		pop,
		PMEM_PAGE_PART_LOG*		ppl);

plog_hash_t*
pm_ppl_hash_get(
		PMEMobjpool*		pop,
		PMEM_PAGE_PART_LOG*		ppl,
		PMEM_PAGE_LOG_HASHED_LINE* pline,
		uint64_t			key	);

/*
 * Add a plogblock's key on the pline hashtable
 * @param[in] pop
 * @param[in] ppl
 * @param[in] pline pointer to the line that has hashtable to add on
 * @param[in] plog_block pointer to the logblock
 * @param[in] idx index of the block on the line
 * @return: the hash item added
 * */
static inline void
pm_ppl_hash_add(
		PMEM_PAGE_LOG_HASHED_LINE* pline,
		PMEM_PAGE_LOG_BLOCK*	plog_block,
		uint32_t				idx
		)
{

	plog_hash_t* item;

	item = (plog_hash_t*) malloc(sizeof(plog_hash_t));
	item->key = plog_block->key;
	item->block_off = idx;
	HASH_INSERT(plog_hash_t, addr_hash, pline->addr_hash, plog_block->key, item);
	
}

plog_hash_t*
pm_ppl_hash_check_and_add(
		PMEMobjpool*		pop,
		PMEM_PAGE_PART_LOG*		ppl,
		PMEM_PAGE_LOG_HASHED_LINE* pline,
		uint64_t			key
		);

void
pm_ppl_hash_remove(
		PMEMobjpool*		pop,
		PMEM_PAGE_PART_LOG*		ppl,
		PMEM_PAGE_LOG_HASHED_LINE* pline,
		uint64_t			key
		);

void 
__init_page_log_free_pool(
		PMEMobjpool*			pop,
		PMEM_PAGE_PART_LOG*		ppl,
		uint64_t				n_free_log_bufs,
		uint64_t				log_buf_size,
		uint64_t				&log_buf_id,
		uint64_t				&log_buf_offset
		); 


void 
__init_tt(
		PMEMobjpool*		pop,
		PMEM_PAGE_PART_LOG*		ppl,
		uint64_t n,
		uint64_t k,
		uint64_t pages_per_tx);

void __init_tt_entry(
		PMEMobjpool*			pop,
		PMEM_PAGE_PART_LOG*		ppl,
		PMEM_TT_HASHED_LINE*	pline,
		uint64_t n,
		uint64_t k,
		uint64_t pages_per_tx);

uint64_t
pm_ppl_create_entry_id(
	   	uint16_t type,
	   	uint32_t row,
	   	uint32_t col);
void
pm_ppl_parse_entry_id(
		uint64_t val,
	   	uint16_t* type,
	   	uint32_t* row,
	   	uint32_t* col);

//void
uint64_t
pm_ppl_write_rec(
			PMEMobjpool*		pop,
			PMEM_WRAPPER*		pmw,
			PMEM_PAGE_PART_LOG*	ppl,
			uint64_t			key,
			byte*				log_src,
			uint32_t			rec_size);

void
pm_ppl_check_for_ckpt(
			PMEMobjpool*				pop,
			PMEM_PAGE_PART_LOG*			ppl,
			PMEM_PAGE_LOG_HASHED_LINE*	pline,
			PMEM_PAGE_LOG_BUF*			plogbuf,
			uint64_t					cur_lsn
			);
uint64_t
pm_ppl_compute_ckpt_lsn(
			PMEMobjpool*				pop,
			PMEM_PAGE_PART_LOG*			ppl
			);

/*Called when a mini-transaction write log record to log buffer in the traditional InnoDB*/
uint64_t
pm_ppl_write(
			PMEMobjpool*		pop,
			PMEM_PAGE_PART_LOG*	ppl,
			uint64_t			tid,
			byte*				log_src,
			uint64_t			size,
			uint64_t			n_recs,
			uint64_t*			key_arr,
			uint64_t*			size_arr,
			uint64_t*			ret_start_lsn,
			uint64_t*			ret_end_lsn,
			uint64_t			block_id);

void __handle_pm_ppl_write_by_entry(
			PMEMobjpool*		pop,
			PMEM_PAGE_PART_LOG*	ppl,
			uint64_t			tid,
			byte*				log_src,
			uint64_t			size,
			uint64_t			n_recs,
			uint64_t*			key_arr,
			uint64_t*			size_arr,
			//uint64_t*			LSN_arr,
			uint64_t*			ret_start_lsn,
			uint64_t*			ret_end_lsn,
			PMEM_TT_ENTRY*		pe,
			bool				is_new);


uint64_t
__update_page_log_block_on_write(
			PMEMobjpool*		pop,
			PMEM_PAGE_PART_LOG*	ppl,
			byte*				log_src,
			uint64_t			cur_off,
			uint64_t			rec_size,
			uint64_t			key,
			uint64_t*			LSN,
			uint64_t			bid);

PMEM_PAGE_LOG_BLOCK*
__get_log_block_by_id(
		PMEMobjpool*		pop,
		PMEM_PAGE_PART_LOG*	ppl,
		uint64_t			bid);

PMEM_PAGE_LOG_BLOCK*
pm_ppl_get_log_block_by_key(
		PMEMobjpool*		pop,
		PMEM_PAGE_PART_LOG*	ppl,
		uint64_t			key);

/// write log_buf --> full --> assign flusher worker --> handle finish
void
__pm_write_log_buf(
			PMEMobjpool*				pop,
			PMEM_PAGE_PART_LOG*			ppl,
			//PMEM_PAGE_LOG_HASHED_LINE*	pline,
			uint64_t					hashed_id,
			byte*						log_src,
			uint64_t					src_off,
			uint64_t					rec_size,
			uint64_t*					rec_lsn,
			PMEM_PAGE_LOG_BLOCK*		plog_block,
			bool						is_first_write);

static inline void
pm_write_log_rec_low(
			PMEMobjpool*			pop,
			byte*					log_des,
			byte*					log_src,
			uint64_t				size)
{
	pmemobj_memcpy_persist(
			pop, 
			log_des,
			log_src,
			size);
	//if (size <= CACHELINE_SIZE){
	//	//We need persistent copy, Do not need a transaction for atomicity
	//		pmemobj_memcpy_persist(
	//			pop, 
	//			log_des,
	//			log_src,
	//			size);

	//}
	//else{
	//	TX_BEGIN(pop) {
	//		TX_MEMCPY(log_des, log_src, size);
	//	}TX_ONABORT {
	//	}TX_END
	//}
}

void
__pm_write_log_rec_low(
			PMEMobjpool*			pop,
			byte*					log_des,
			byte*					log_src,
			uint64_t				rec_size);


void
pm_recv_assign_redoer(
		PMEM_PAGE_PART_LOG*			ppl,
		PMEM_PAGE_LOG_HASHED_LINE*	pline);


//////////// COMMIT //////////////////

void
pm_ppl_commit(
		PMEMobjpool*			pop,
		PMEM_PAGE_PART_LOG*		ppl,
		uint64_t				tid,
		uint64_t				eid);

void
pm_ppl_set_flush_state(
		PMEMobjpool*		pop,
		PMEM_PAGE_PART_LOG*	ppl,
		buf_page_t*			bpage);

void 
pm_ppl_flush_page(
		PMEMobjpool*		pop,
		PMEM_WRAPPER*		pmw,
		PMEM_PAGE_PART_LOG*	ppl,
		buf_page_t*			bpage,
		uint64_t			space,
		uint64_t			page_no,
		uint64_t			key,
		uint64_t			pageLSN);
void
pm_ppl_update_oldest(
		PMEMobjpool*		pop,
		PMEM_PAGE_PART_LOG*		ppl,
		PMEM_PAGE_LOG_HASHED_LINE* pline
		);
//////////////// CHECKPOINT ///////////

//implemented in log0log.cc
uint64_t
pm_ppl_checkpoint(
			PMEMobjpool*				pop,
			PMEM_PAGE_PART_LOG*			ppl
			);

void
pm_ppl_load_spaces(
		PMEMobjpool*		pop,
		PMEM_PAGE_PART_LOG*		ppl);

void
pm_ppl_add_space(
		PMEMobjpool*		pop,
		PMEM_PAGE_PART_LOG*		ppl,
		char* name,
		uint32_t space_id
		);

void
pm_ppl_fil_names_clear(
		lsn_t lsn
		);
void
pm_ppl_buf_flush_request_force(
		uint64_t lsn);
////// RECOVERY
void 
pm_ppl_recv_init(
		PMEMobjpool*		pop,
		PMEM_PAGE_PART_LOG*	ppl
		);

void 
pm_ppl_recv_end(
		PMEMobjpool*		pop,
		PMEM_PAGE_PART_LOG*	ppl);

dberr_t
pm_ppl_recovery(
		PMEMobjpool*		pop,
		PMEM_PAGE_PART_LOG*	ppl,
		lsn_t flush_lsn);

void 
pm_ppl_analysis(
		PMEMobjpool*		pop,
		PMEM_PAGE_PART_LOG*	ppl,
		ulint*				global_max_lsn);

void 
pm_ppl_redo(
		PMEMobjpool*		pop,
		PMEM_PAGE_PART_LOG*	ppl);

bool
pm_ppl_redo_line(
		PMEMobjpool*		pop,
		PMEM_PAGE_PART_LOG*	ppl,
		PMEM_PAGE_LOG_HASHED_LINE* pline);

int64_t
pm_ppl_parse_recs(
		PMEMobjpool*		pop,
		PMEM_PAGE_PART_LOG*	ppl,
		PMEM_PAGE_LOG_HASHED_LINE* pline,
		byte* recv_buf,
		uint64_t len,
		uint64_t* n_skip1_recs,
		uint64_t* n_skip2_recs,
		uint64_t* n_need_recs
		);

uint64_t
pm_ppl_recv_parse_log_rec(
	PMEMobjpool*		pop,
	PMEM_PAGE_PART_LOG*	ppl,
	PMEM_PAGE_LOG_HASHED_LINE* pline,
	mlog_id_t*	type,
	byte*		ptr,
	byte*		end_ptr,
	ulint*		space,
	ulint*		page_no,
	bool		apply,
	uint64_t*   rec_lsn,
	byte**		body,
	PMEM_PARSE_RESULT*	parse_res,
	bool*		is_need);


void 
pm_ppl_recv_line_empty_hash(
	PMEMobjpool*		pop,
	PMEM_PAGE_PART_LOG*	ppl,
	PMEM_PAGE_LOG_HASHED_LINE* pline);

void
pm_ppl_recv_add_to_hash_table(
	PMEMobjpool*		pop,
	PMEM_PAGE_PART_LOG*	ppl,
	PMEM_RECV_LINE* recv_line,
	mlog_id_t	type,		/*!< in: log record type */
	ulint		space,		/*!< in: space id */
	ulint		page_no,	/*!< in: page number */
	byte*		body,		/*!< in: log record body */
	byte*		rec_end,	/*!< in: log record end */
	lsn_t		start_lsn,
	lsn_t		end_lsn);

recv_addr_t*
pm_ppl_recv_get_fil_addr_struct(
	PMEM_RECV_LINE* recv_line,
	ulint	space,
	ulint	page_no);

ulint
pm_ppl_recv_get_max_recovered_lsn(
	PMEMobjpool*		pop,
	PMEM_PAGE_PART_LOG*	ppl);

ulint
pm_ppl_get_max_lsn(
	PMEMobjpool*		pop,
	PMEM_PAGE_PART_LOG*	ppl);

void
pm_ppl_get_min_max_tid(
	PMEMobjpool*		pop,
	PMEM_PAGE_PART_LOG*	ppl,
	ulint* min_tid,
	ulint* max_tid);

dberr_t
pm_ppl_recv_init_crash_recovery_spaces(
	PMEMobjpool*		pop,
	PMEM_PAGE_PART_LOG*	ppl,
	ulint max_recovered_lsn);

void
pm_ppl_fil_names_dirty(
	PMEMobjpool*		pop,
	PMEM_PAGE_PART_LOG*	ppl,
	ulint max_recovered_lsn,
	fil_space_t*		space);

void
pm_ppl_recv_recover_page_func(
	PMEMobjpool*		pop,
	PMEM_PAGE_PART_LOG*	ppl,
	PMEM_PAGE_LOG_HASHED_LINE* pline,
	ibool		just_read_in,
				/*!< in: TRUE if the i/o handler calls
				this for a freshly read page */
	buf_block_t*	block);	/*!< in/out: buffer block */

ulint
pm_ppl_recv_read_in_area(
	PMEMobjpool*		pop,
	PMEM_PAGE_PART_LOG*	ppl,
	PMEM_RECV_LINE* recv_line,
	const page_id_t&	page_id);

void
pm_ppl_buf_read_recv_pages(
		PMEMobjpool*		pop,
		PMEM_PAGE_PART_LOG*	ppl,
		PMEM_RECV_LINE* recv_line,
		bool sync,
		ulint space_id,
		const ulint* page_nos,
		ulint n_stored);

void
pm_ppl_recv_apply_hashed_log_recs(
		PMEMobjpool*		pop,
		PMEM_PAGE_PART_LOG*	ppl,
		ibool	allow_ibuf);

void
pm_ppl_recv_check_hashed_line(
	PMEMobjpool*		pop,
	PMEM_PAGE_PART_LOG*	ppl,
	PMEM_PAGE_LOG_HASHED_LINE* pline);

void
pm_ppl_recv_apply_prior_pages(
	PMEMobjpool*		pop,
	PMEM_PAGE_PART_LOG*	ppl,
	PMEM_PAGE_LOG_HASHED_LINE* pline,
	ibool	allow_ibuf);

void
pm_ppl_recv_apply_single_page(
	PMEMobjpool*		pop,
	PMEM_PAGE_PART_LOG*	ppl,
	PMEM_PAGE_LOG_HASHED_LINE* pline,
	ulint space, ulint page_no);

void
pm_ppl_recv_apply_hashed_line(
	PMEMobjpool*		pop,
	PMEM_PAGE_PART_LOG*	ppl,
	PMEM_PAGE_LOG_HASHED_LINE* pline,
	ibool	allow_ibuf);

void
pm_ppl_buf_flush_recv_note_modification(
	PMEMobjpool*		pop,
	PMEM_PAGE_PART_LOG*	ppl,
	buf_block_t*    block,
	lsn_t       start_lsn,
	lsn_t       end_lsn);


//// RESET after recovery
void 
pm_ppl_reset_all(
	PMEMobjpool*		pop,
	PMEM_PAGE_PART_LOG*	ppl);

void 
pm_page_part_log_bucket_reset(
		PMEMobjpool*			pop,
		PMEM_PAGE_PART_LOG*		ppl);

void
__reset_tt(
		PMEMobjpool*			pop,
		PMEM_PAGE_PART_LOG*		ppl);
////////////// END RECOVERY

void
pm_ppl_remove_fil_spaces();

////////////// UTILITY////////////

//////////// BIT ARRAY /////////
void
pm_bit_set(
		PMEM_PAGE_LOG_HASHED_LINE* pline,
		long long*	arr,
		size_t		block_size,
		uint64_t	bit_i);

void
pm_bit_clear(
		PMEM_PAGE_LOG_HASHED_LINE* pline,
		long long*	arr,
		size_t		block_size,
		uint64_t	bit_i);

int32_t 
pm_search_first_free_slot(
		PMEM_PAGE_LOG_HASHED_LINE* pline,
		long long*	bit_arr,
		uint16_t	n_bit_blocks,
		uint16_t	block_size);

#define PM_BIT_SET(A, bs, i) ( A[i / bs] |= 1 << (i % bs) )

#define PM_BIT_CLEAR(A, bs, i) ( A[i / bs] &= ~(1 << (i % bs)) )

/////////// END BIT ARRAY //////

int64_t
__update_tt_entry_on_write_log(
		PMEMobjpool*		pop,
		PMEM_PAGE_PART_LOG*	ppl,
		uint64_t			tid,
		uint64_t			key); 

bool
__update_page_log_block_on_commit(
		PMEMobjpool*				pop,
		PMEM_PAGE_PART_LOG*			ppl,
		PMEM_PAGE_REF*				pref,
		uint64_t					eid);
void 
__reset_TT_entry(
		PMEMobjpool*				pop,
		PMEM_PAGE_PART_LOG*			ppl,
		PMEM_TT_ENTRY*				pe);

void __realloc_page_log_block_line(
		PMEMobjpool*			pop,
		PMEM_PAGE_PART_LOG*		ppl,
		PMEM_PAGE_LOG_HASHED_LINE*	pline,
		uint64_t				new_size);

void __realloc_TT_line(
		PMEMobjpool*			pop,
		PMEM_PAGE_PART_LOG*		ppl,
		PMEM_TT_HASHED_LINE*	pline,
		uint64_t				new_size);
void 
__realloc_TT_entry(
		PMEMobjpool*				pop,
		PMEM_PAGE_PART_LOG*			ppl,
		PMEM_TT_ENTRY*				pe,
		uint64_t					new_size);

void 
__reset_page_log_block(
		PMEM_PAGE_LOG_BLOCK* plog_block);

PMEM_TT_ENTRY* 
pm_ppl_get_tt_entry_by_tid(
		PMEMobjpool*				pop,
		PMEM_PAGE_PART_LOG*			ppl,
		uint64_t tid);

bool
pm_ppl_check_and_reset_tt_entry(
		PMEMobjpool*		pop,
		PMEM_TT*			ptt,
		uint64_t			tid);

inline bool
__is_page_log_block_reclaimable(
		PMEM_PAGE_LOG_BLOCK* plog_block);

PMEM_PAGE_LOG_HASHED_LINE*
pm_ppl_get_line_from_key(
		PMEMobjpool*				pop,
		PMEM_PAGE_PART_LOG*			ppl,
		uint64_t key);

/////////////////////// LOG FILES ////////////////////////////

// See srv0start.cc for implementation
dberr_t
pm_create_or_open_part_log_files(
		PMEM_PAGE_PART_LOG*	ppl,
		char*   logfilename,
		size_t  dirnamelen,
		char*&  logfile0);

void
pm_close_and_free_log_files(
		PMEM_PAGE_PART_LOG*	ppl);

PMEM_LOG_GROUP*
pm_log_group_init(
/*===========*/
	uint64_t	id,			/*!< in: group id */
	uint64_t	n_files,		/*!< in: number of log files */
	uint64_t	file_size,		/*!< in: log file size in bytes */
	uint64_t	space_id);	

void 
pm_log_group_free(
		PMEM_LOG_GROUP* group);

dberr_t
pm_create_log_file(
	pfs_os_file_t*	file,
	const char*	name,
	uint64_t log_file_size);

//dberr_t
//pm_truncate_log_file(
//	pfs_os_file_t*	file,
//	const char*	name,
//	uint64_t new_size);

fil_node_t*
pm_log_fil_node_create(
	const char*	name,
	ulint		size,
	fil_space_t*	space);
/////////////// END LOG FILES ///////////

//Statistic 
#if defined(UNIV_PMEMOBJ_PPL_STAT)
void
__print_lock_overhead(FILE* f,
		PMEM_PAGE_PART_LOG* ppl);

#endif
//for debugging
void 
__print_blocks_state(FILE* f,
		PMEM_PAGE_PART_LOG* ppl);
void 
__print_page_log_hashed_lines(
		FILE* f,
		PMEM_PAGE_PART_LOG* ppl);

///////////////// END PER-PAGE LOGGING //////////////////

////////////// END NEW PARTITIONED LOG /////////////////
#endif //UNIV_PMEMOBJ_PL
//
////////////////////// LOG BUFFER /////////////////////////////
/*
 * This implement PMEM_WAL that copy the way InnoDB log buffer work.
 * We allocate the centralized log buffer in PMEM instead of DRAM and follow the same logic of InnoDB's log buffer.
 * */
struct __pmem_log_buf {
	size_t				size;
	PMEM_OBJ_TYPES		type;	
	PMEMoid				data; //log data
    uint64_t			lsn; 	
	uint64_t			buf_free; /* first free offset within the log buffer */
	bool				need_recv; /*need recovery, it is set to false when init and when the server shutdown
					  normally. Whenever a log record is copy to log buffer, this flag is set to true
	*/
	uint64_t			last_tsec_buf_free; /*the buf_free updated in previous t seconds, update this value in srv_sync_log_buffer_in_background() */
};

void* pm_wrapper_logbuf_get_logdata(PMEM_WRAPPER* pmw);
int
pm_wrapper_logbuf_alloc(
		PMEM_WRAPPER*		pmw,
	   	const size_t		size);

int
pm_wrapper_logbuf_realloc(
		PMEM_WRAPPER*		pmw,
	   	const size_t		size);
PMEM_LOG_BUF* pm_pop_get_logbuf(PMEMobjpool* pop);
PMEM_LOG_BUF*
pm_pop_logbuf_alloc(
		PMEMobjpool*		pop,
	   	const size_t		size);
PMEM_LOG_BUF* 
pm_pop_logbuf_realloc(
		PMEMobjpool*		pop,
	   	const size_t		size);
ssize_t  pm_wrapper_logbuf_io(PMEM_WRAPPER* pmw, 
							const int type,
							void* buf, 
							const uint64_t offset,
							unsigned long int n);

///////////// DOUBLE WRITE BUFFER //////////////////////////

struct __pmem_dbw {
	size_t size;
	PMEM_OBJ_TYPES type;	
	PMEMoid  data; //dbw data
	uint64_t s_first_free;
	uint64_t b_first_free;
	bool is_new;
};

void* pm_wrapper_dbw_get_dbwdata(PMEM_WRAPPER* pmw);

int 
pm_wrapper_dbw_alloc(
		PMEM_WRAPPER*		pmw,
	   	const size_t		size);

PMEM_DBW* pm_pop_get_dbw(PMEMobjpool* pop);
PMEM_DBW*
pm_pop_dbw_alloc(
		PMEMobjpool*		pop,
	   	const size_t		size);
ssize_t  pm_wrapper_dbw_io(PMEM_WRAPPER* pmw, 
							const int type,
							void* buf, 
							const uint64_t offset,
							unsigned long int n);

/////// PMEM BUF  //////////////////////
#if defined (UNIV_PMEMOBJ_BUF)
//This struct is used only for POBJ_LIST_INSERT_NEW_HEAD
//modify this struct according to struct __pmem_buf_block_t
struct list_constr_args{
//	uint64_t		id;
	page_id_t					id;
//	size_t			size;
	page_size_t					size;
	int							check;
	//buf_page_t*		bpage;
	PMEM_BLOCK_STATE			state;
	TOID(PMEM_BUF_BLOCK_LIST)	list;
	uint64_t					pmemaddr;
};

/*
 *A unit page in pmem
 It wrap buf_page_t and an address in pmem
 * */
struct __pmem_buf_block_t{
	PMEMrwlock					lock; //this lock protects remain properties

	page_id_t					id;
	page_size_t					size;
	//pfs_os_file_t				file_handle;
	char						file_name[256];
	int							check; //PMEM AIO flag used in fil_aio_wait
	bool	sync;
	PMEM_BLOCK_STATE			state;
	TOID(PMEM_BUF_BLOCK_LIST)	list;
	uint64_t		pmemaddr; /*
						  the offset of the page in pmem
						  note that the size of page can be got from page
						*/
};

struct __pmem_buf_block_list_t {
	PMEMrwlock				lock;
	uint64_t				list_id; //id of this list in total PMEM area
	int						hashed_id; //id of this list if it is in a bucket, PMEM_ID_NONE if it is in free list
	TOID_ARRAY(TOID(PMEM_BUF_BLOCK))	arr;
	//POBJ_LIST_HEAD(block_list, PMEM_BUF_BLOCK) head;
	TOID(PMEM_BUF_BLOCK_LIST) next_list;
	TOID(PMEM_BUF_BLOCK_LIST) prev_list;
	TOID(PMEM_BUF_BLOCK) next_free_block;

	POBJ_LIST_ENTRY(PMEM_BUF_BLOCK_LIST) list_entries;

	size_t				max_pages; //max number of pages
	size_t				cur_pages; // current buffered pages
	bool				is_flush;
	size_t				n_aio_pending; //number of pending aio
	size_t				n_sio_pending; //number of pending sync io 
	size_t				n_flush; //number of flush
	int					check;
	ulint				last_time;
	
	ulint				param_arr_index; //index of the param in the param_arrs used to carry the info of this list
	//int					flush_worker_id;
	//bool				is_worker_handling;
	
};

struct __pmem_buf_free_pool {
	PMEMrwlock			lock;
	POBJ_LIST_HEAD(list_list, PMEM_BUF_BLOCK_LIST) head;
	size_t				cur_lists;
	size_t				max_lists;
};


struct __pmem_buf {
	size_t size;
	size_t page_size;
	PMEM_OBJ_TYPES type;	

	PMEMoid  data; //pmem data
	//char* p_align; //align 
	byte* p_align; //align 

	bool is_new;
	TOID(PMEM_BUF_FREE_POOL) free_pool;
	TOID_ARRAY(TOID(PMEM_BUF_BLOCK_LIST)) buckets;
	TOID(PMEM_BUF_BLOCK_LIST) spec_list; //list of page 0 used in recovery

	FILE* deb_file;
#if defined(UNIV_PMEMOBJ_BUF_STAT)
	PMEM_BUCKET_STAT* bucket_stats; //array of bucket stats
#endif

	bool is_async_only; //true if we only capture non-sync write from buffer pool

	//Common used variables, used as const
	
	uint64_t PMEM_N_BUCKETS;
	uint64_t PMEM_BUCKET_SIZE;
	double PMEM_BUF_FLUSH_PCT;

	uint64_t PMEM_N_FLUSH_THREADS;
	//set this to large number to eliminate 
	//static uint64_t PMEM_PAGE_PER_BUCKET_BITS=32;

	uint64_t PMEM_FLUSHER_WAKE_THRESHOLD;
#if defined (UNIV_PMEMOBJ_BUF_PARTITION)
	//256 buckets => 8 bits, max 32 spaces => 5 bits => need 3 = 8 - 5 bits
	uint64_t PMEM_N_BUCKET_BITS;
	uint64_t PMEM_N_SPACE_BITS;
	uint64_t PMEM_PAGE_PER_BUCKET_BITS;
#endif //UNIV_PMEMOBJ_BUF_PARTITION

	//Those varables are in DRAM
	bool is_recovery;
	os_event_t*  flush_events; //N flush events for N buckets
	os_event_t free_pool_event; //event for free_pool
	

	PMEMrwlock				param_lock;
	PMEM_AIO_PARAM_ARRAY* param_arrs;//circular array of pointers
	ulint			param_arr_size; //size of the array
	ulint			cur_free_param; //circular index, where the next free params is
	
	PMEM_FLUSHER* flusher;	

	PMEM_FILE_MAP* filemap;
	/// End new in PL-NVM
	
#if defined (UNIV_PMEMOBJ_BLOOM)
	uint64_t PMEM_BLOOM_N_ELEMENTS;
	double PMEM_BLOOM_FPR;
	//PMEM_BLOOM* bf;
	PMEM_CBF* cbf;
#endif //UNIV_PMEMOBJ_BLOOM
};

// PARTITION //////////////
/*Map space id to hashed_id, for partition purpose
 * */
struct __pmem_file_map_item {
	uint32_t		space_id;
	char*			name;

	int*			hashed_ids; //list of hash_id this space appears on 
	uint64_t		count; //number of hashed list this space appears on

	uint64_t*		freqs; //freq[i] is the number of times this space apearts on hashed_ids[i]
};
struct __pmem_file_map {
	PMEMrwlock			lock;

	uint64_t					max_size;
	uint64_t					size;
	PMEM_FILE_MAP_ITEM**		items;
};

//this struct for space_oriented sort
struct __pmem_sort_obj {
	uint32_t			space_no;
	
	uint32_t			n_blocks;
	uint32_t*			block_indexes;
};

void 
pm_filemap_init(
		PMEM_BUF*		buf);
void
pm_filemap_close(PMEM_BUF* buf);

/*Update the page_id in the filemap
 *
 * */
void
pm_filemap_update_items(
		PMEM_BUF*		buf,
	   	page_id_t		page_id,
		int				hashed_id,
		uint64_t		bucket_size); 

void
pm_filemap_print(
		PMEM_BUF*		buf, 
		FILE*			outfile);

#if defined(UNIV_PMEMOBJ_BUF_STAT)
//statistic info about a bucket
//Objects of those struct do not need in PMEM
struct __pmem_buf_bucket_stat {
	PMEMrwlock		lock;

	uint64_t		n_writes;/*number of writes on the bucket*/ 
	uint64_t		n_overwrites;/*number of overwrites on the bucket*/
	uint64_t		n_reads;/*number of reads on the list (both flushing and normal)*/	
	uint64_t		n_reads_hit;/*number of reads successful on PMEM buffer*/	
	uint64_t		n_reads_flushing;/*number of reads on the on-flushing list, n_reads_flushing < n_reads_hit < n_reads*/	
	uint64_t		max_linked_lists;
	uint64_t		n_flushed_lists; /*number of of flushes on the bucket*/
};

#endif

bool pm_check_io(byte* frame, page_id_t  page_id);


void
pm_wrapper_buf_alloc_or_open(
		 PMEM_WRAPPER*		pmw,
		 const size_t		buf_size,
		 const size_t		page_size);

void pm_wrapper_buf_close(PMEM_WRAPPER* pmw);

int
pm_wrapper_buf_alloc(
		PMEM_WRAPPER*		pmw,
	    const size_t		size,
		const size_t		page_size);

PMEM_BUF* pm_pop_get_buf(PMEMobjpool* pop);

PMEM_BUF* 
pm_pop_buf_alloc(
		 PMEMobjpool*		pop,
		 const size_t		size,
		 const size_t		page_size);


int 
pm_buf_block_init(PMEMobjpool *pop, void *ptr, void *arg);

void 
pm_buf_lists_init(
		PMEMobjpool*	pop,
		PMEM_BUF*		buf, 
		const size_t	total_size,
		const size_t	page_size);

// allocate and init pages in a list
void
pm_buf_single_list_init(
		PMEMobjpool*				pop,
		TOID(PMEM_BUF_BLOCK_LIST)	inlist,
		size_t&						offset,
		struct list_constr_args*	args,
		const size_t				n,
		const size_t				page_size);
int
pm_buf_write(
			PMEMobjpool*	pop,
		   	PMEM_BUF*		buf,
		   	page_id_t		page_id,
		   	page_size_t		size,
		   	byte*			src_data,
		   	bool			sync);

int
pm_buf_write_no_free_pool(
			PMEMobjpool*	pop,
		   	PMEM_BUF*		buf,
		   	page_id_t		page_id,
		   	page_size_t		size,
		   	byte*			src_data, 
			bool			sync);

int
pm_buf_write_with_flusher(
			PMEMobjpool*	pop,
			PMEM_WRAPPER*	pmw,
		   	page_id_t		page_id,
		   	page_size_t		size,
			uint64_t		pageLSN,
		   	byte*			src_data,
		   	bool			sync);
int
pm_buf_write_with_flusher_append(
			PMEMobjpool*	pop,
		   	PMEM_BUF*		buf,
		   	page_id_t		page_id,
		   	page_size_t		size,
		   	byte*			src_data,
		   	bool			sync);


const PMEM_BUF_BLOCK*
pm_buf_read(
			PMEMobjpool*		pop,
		   	PMEM_BUF*			buf,
		   	const page_id_t		page_id,
		   	const page_size_t	size,
		   	byte*				data,
		   	bool sync);

const PMEM_BUF_BLOCK*
pm_buf_read_lasted(
			PMEMobjpool*		pop,
		   	PMEM_BUF*			buf,
		   	const page_id_t		page_id,
		   	const page_size_t	size,
		   	byte*				data,
		   	bool sync);

const PMEM_BUF_BLOCK*
pm_buf_read_page_zero(
			PMEMobjpool*		pop,
		   	PMEM_BUF*			buf,
			char*				file_name,
		   	byte*				data);

void
pm_buf_flush_list(
			PMEMobjpool*			pop,
		   	PMEM_BUF*				buf,
		   	PMEM_BUF_BLOCK_LIST*	plist);
void
pm_buf_resume_flushing(
			PMEMobjpool*			pop,
		   	PMEM_WRAPPER*				pmw);


void
pm_buf_handle_full_hashed_list(
	PMEMobjpool*	pop,
	PMEM_WRAPPER*		pmw,
	ulint			hashed);

void
pm_buf_assign_flusher(
	PMEM_BUF*				buf,
	PMEM_BUF_BLOCK_LIST*	phashlist);

void
pm_buf_write_aio_complete(
			PMEMobjpool*			pop,
		   	PMEM_BUF*				buf,
		   	TOID(PMEM_BUF_BLOCK)*	ptoid_block);

PMEM_BUF* pm_pop_get_buf(PMEMobjpool* pop);


////////////////////// LSB ///////////////////////
#if defined (UNIV_PMEMOBJ_LSB)
//LSB: Implement Log-structure Buffer
struct __pmem_lsb_hash_entry_t{
	page_id_t					id;
	size_t						size;
	char						file_name[256];
	uint64_t		pmemaddr; /*
						  the offset of the page in pmem
						  note that the size of page can be got from page
						*/
	int				lsb_entry_id; //id of the entry in lsb list
	__pmem_lsb_hash_entry_t* prev;
	__pmem_lsb_hash_entry_t* next;

};
struct __pmem_lsb_hash_bucket_t{
	PMEMrwlock				lock;//the bucket lock
	__pmem_lsb_hash_entry_t* head;
	__pmem_lsb_hash_entry_t* tail;
	size_t n_entries;
	ulint param_arr_index; //index of param entry, used when transfer data from the bucket to the io_submit()
};
struct __pmem_lsb_hashtable_t{
	__pmem_lsb_hash_bucket_t* buckets;
	size_t n_buckets;
};

//The wrapper LSB
struct __pmem_LSB {
	//centralized mutex lock for whole buffer
	PMEMrwlock				lsb_lock;
	
	//the lock protect counters in AIO
	PMEMrwlock				lsb_aio_lock;
	ulint n_aio_completed;
	ulint n_aio_submitted;

	os_event_t			all_aio_finished;

	size_t size;
	size_t page_size;
	PMEM_OBJ_TYPES type;	

	PMEMoid  data; //pmem data
	//char* p_align; //align 
	byte* p_align; //align 

	bool is_new;
	TOID(PMEM_LSB_HASHTABLE) ht;
	TOID(PMEM_BUF_BLOCK_LIST) lsb_list; //list of page 0 used in recovery


	FILE* deb_file;

	//Those varables are in DRAM
	bool is_recovery;
	os_event_t*  flush_events; //N flush events for N buckets
	PMEMrwlock				param_lock;
	PMEM_AIO_PARAM_ARRAY* param_arrs;//circular array of pointers
	ulint			param_arr_size; //size of the array
	ulint			cur_free_param; //circular index, where the next free params is

	PMEM_FLUSHER* flusher;	
};

void
pm_wrapper_lsb_alloc_or_open(
		PMEM_WRAPPER*		pmw,
		const size_t		buf_size,
		const size_t		page_size);

void pm_wrapper_lsb_close(PMEM_WRAPPER* pmw);

int
pm_wrapper_lsb_alloc(
		PMEM_WRAPPER*		pmw,
	    const size_t		size,
		const size_t		page_size);

PMEM_LSB* 
pm_pop_lsb_alloc(
		PMEMobjpool*		pop,
		const size_t		size,
		const size_t		page_size);
void 
pm_lsb_lists_init(
		PMEMobjpool*	pop,
		PMEM_LSB*		lsb, 
		const size_t	total_size,
	   	const size_t	page_size);
void 
pm_lsb_hashtable_init(
		PMEMobjpool*	pop,
		PMEM_LSB*		lsb, 
		const size_t	n_entries);

void
pm_lsb_hashtable_free(
		PMEMobjpool*	pop,
		PMEM_LSB*		lsb);

void
pm_lsb_hashtable_reset(
		PMEMobjpool*	pop,
		PMEM_LSB*		lsb);
int 
pm_lsb_hashtable_add_entry(
		PMEMobjpool*			pop,
		PMEM_LSB*				lsb,
		PMEM_LSB_HASH_ENTRY*	entry);

void 
pm_lsb_hashtable_remove_entry(
		PMEMobjpool*			pop,
		PMEM_LSB*				lsb,
		PMEM_LSB_HASH_ENTRY*	entry);

PMEM_LSB_HASH_ENTRY*
pm_lsb_hashtable_search_entry(
		PMEMobjpool*			pop,
		PMEM_LSB*				lsb,
		PMEM_LSB_HASH_ENTRY*	in_entry);


int
pm_lsb_write(
			PMEMobjpool*	pop,
		   	PMEM_LSB*		lsb,
		   	page_id_t		page_id,
		   	page_size_t		size,
		   	byte*			src_data,
		   	bool			sync);

void
pm_lsb_assign_flusher(
		PMEM_LSB*				lsb);

void
pm_lsb_flush_bucket_batch(
			PMEMobjpool*			pop,
		   	PMEM_LSB*				lsb,
		   	PMEM_LSB_HASH_BUCKET*	pbucket);
void
pm_lsb_flush_bucket(
			PMEMobjpool*			pop,
		   	PMEM_LSB*				lsb,
		   	PMEM_LSB_HASH_BUCKET*	pbucket);
//Implemented in buf0flu.cc using with pm_buf_write_with_flsuher
void
pm_lsb_handle_finished_block(
		PMEMobjpool*		pop,
	   	PMEM_LSB*			lsb,
	   	PMEM_BUF_BLOCK*		pblock);

const PMEM_BUF_BLOCK* 
pm_lsb_read(
		PMEMobjpool*		pop,
	   	PMEM_LSB*			lsb,
	   	const page_id_t		page_id,
	   	const page_size_t	size,
	   	byte*				data, 
		bool				sync);
#endif //UNIV_PMEMOBJ_LSB

//////////////////////// End of LSB //////////////
//Flusher

/*The flusher thread
 *if is waked by a signal (there is at least a list wait for flush): scan the waiting list and assign a worker to flush that list
 if there is no list wait for flush, sleep and wait for a signal
 * */
struct __pmem_flusher {

	ib_mutex_t			mutex;
	//for worker
	os_event_t			is_req_not_empty; //signaled when there is a new flushing list added
	os_event_t			is_req_full;

	//os_event_t			is_flush_full;

	os_event_t			is_all_finished; //signaled when all workers are finished flushing and no pending request 
	os_event_t			is_all_closed; //signaled when all workers are closed 
	volatile ulint n_workers;

	//the waiting_list
	ulint size;
	ulint tail; //always increase, circled counter, mark where the next flush list will be assigned
	ulint n_requested;
	//ulint n_flushing;

	bool is_running;

	PMEM_BUF_BLOCK_LIST** flush_list_arr;
#if defined (UNIV_PMEMOBJ_LSB)
	PMEM_LSB_HASH_BUCKET** bucket_arr; //array pointer to buckets in the lsb list
#endif
};
PMEM_FLUSHER*
pm_flusher_init(
				const size_t	size);
void
pm_buf_flusher_close(PMEM_FLUSHER*	flusher);

#if defined(UNIV_PMEMOBJ_BUF_STAT)
void
	pm_buf_bucket_stat_init(PMEM_BUF* pbuf);

void
	pm_buf_stat_print_all(PMEM_BUF* pbuf);
#endif
//DEBUG functions

void pm_buf_print_lists_info(PMEM_BUF* buf);


//version 1: implemented in pmem0buf, directly handle without using thread slot
void
pm_handle_finished_block(
		PMEMobjpool*		pop,
		PMEM_WRAPPER*		pmw	,
	   	PMEM_BUF*			buf,
	   	PMEM_BUF_BLOCK* pblock);

void
pm_handle_finished_block_no_free_pool(
		PMEMobjpool*		pop,
	   	PMEM_BUF* buf,
	   	PMEM_BUF_BLOCK* pblock);

//Implemented in buf0flu.cc using with pm_buf_write_with_flsuher
void
pm_handle_finished_block_with_flusher(
		PMEMobjpool*		pop,
		PMEM_WRAPPER*		pmw	,
	   	PMEM_BUF*			buf,
	   	PMEM_BUF_BLOCK*		pblock);

//version 2 is implemented in buf0flu.cc that handle threads slot
void
pm_handle_finished_block_v2(PMEM_BUF_BLOCK* pblock);

bool
pm_lc_wait_finished(
	ulint*	n_flushed_list);

ulint
pm_lc_sleep_if_needed(
	ulint		next_loop_time,
	int64_t		sig_count);

extern "C"
os_thread_ret_t
DECLARE_THREAD(pm_buf_flush_list_cleaner_coordinator)(
		void* arg);

extern "C"
os_thread_ret_t
DECLARE_THREAD(pm_buf_flush_list_cleaner_worker)(
		void* arg);

extern "C"
os_thread_ret_t
DECLARE_THREAD(pm_flusher_coordinator)(
		void* arg);

extern "C"
os_thread_ret_t
DECLARE_THREAD(pm_flusher_worker)(
		void* arg);


#ifdef UNIV_DEBUG

void
pm_buf_flush_list_cleaner_disabled_loop(void);
#endif

ulint 
hash_f1(
		ulint&			hashed,
		uint32_t		space_no,
	   	uint32_t		page_no,
	   	uint64_t		n,
		uint64_t		B,	
		uint64_t		S,
		uint64_t		P);

#define PMEM_BUF_LIST_INSERT(pop, list, entries, type, func, args) do {\
	POBJ_LIST_INSERT_NEW_HEAD(pop, &list.head, entries, sizeof(type), func, &args); \
	list.cur_size++;\
}while (0)

/*Evenly distributed map that one space_id evenly distribute across buckets*/
#define PMEM_HASH_KEY(hashed, key, n) do {\
	hashed = key ^ PMEM_HASH_MASK;\
	hashed = hashed % n;\
}while(0)


#if defined (UNIV_PMEMOBJ_BUF_PARTITION)

//Use this macro for production build 
#define PMEM_LESS_BUCKET_HASH_KEY(pbuf, hashed, space, page)\
   	PARTITION_FUNC1(hashed, space, page,\
		   	PMEM_N_BUCKETS,\
		   	PMEM_N_BUCKET_BITS,\
		   	PMEM_N_SPACE_BITS,\
		   	PMEM_PAGE_PER_BUCKET_BITS) 

#define PARTITION_FUNC1(hashed, space, page, n, B, S, P) do {\
	hashed = (((space & (0xffffffff >> (32 - S))) << (B - S)) + ((page & (0xffffffff >> (32 - P - (B - S)))) >> P)) % n;\
} while(0)
#endif //UNIV_PMEMOBJ_BUF_PARTITION


#endif //UNIV_PMEMOBJ_BUF

/*Bloom Filter on PMEM implementation in Dec 2018*/
#if defined (UNIV_PMEMOBJ_BLOOM)

typedef void (*bh_func) (uint64_t* hashed_vals, uint64_t num_hashes, char *str);

struct __pmem_bloom_filter {
    /* bloom parameters */
    uint64_t		est_elements;
    float			false_pos_prob;
    uint64_t		n_hashes; // the number of hash functions
    uint64_t		n_bits; // the number of bits in bloom
    /* bloom filter */
    unsigned char	*bloom; //the bit array implemented as byte array
    long			bloom_length;// the number of bytes in bloom
    uint64_t		elements_added;
    uint64_t		n_false_pos_reads;
    bh_func			hash_func;
};

PMEM_BLOOM* 
pm_bloom_alloc(
		uint64_t	est_elements,
		float		false_positive_rate,
		bh_func		bloom_hash_func);
//PMEM_BLOOM* 
//pm_bloom_alloc(
//		uint64_t	est_elements,
//		float		false_positive_rate
//		);
void
pm_bloom_free(PMEM_BLOOM* pm_bloom);

int
pm_bloom_add(
		PMEM_BLOOM*		pm_bloom, 
		uint64_t		key);

int
pm_bloom_check(
		PMEM_BLOOM*		pm_bloom,
		uint64_t		key);

uint64_t
pm_bloom_get_set_bits(
		PMEM_BLOOM*		pm_bloom);

// Counting Bloom Filter, support deleting an element
struct __pmem_counting_bloom_filter {
    /* bloom parameters */
    uint64_t		est_elements;
    float			false_pos_prob;
    uint64_t		n_hashes; // the number of hash functions
    uint64_t		n_counts; // the number of counting in bloom
    /* bloom filter */
    uint16_t		*bloom; //the count array 
    //long			bloom_length;// the number of bytes in bloom
    uint64_t		elements_added;
    uint64_t		n_false_pos_reads;
    bh_func			hash_func;
};

PMEM_CBF* 
pm_cbf_alloc(
		uint64_t	est_elements,
		float		false_pos_prob,
		bh_func		bloom_hash_func);
//PMEM_BLOOM* 
//pm_bloom_alloc(
//		uint64_t	est_elements,
//		float		false_positive_rate
//		);
void
pm_cbf_free(PMEM_CBF* cbf);

int
pm_cbf_add(
		PMEM_CBF*		cbf, 
		uint64_t		key);

int
pm_cbf_check(
		PMEM_CBF*		cbf,
		uint64_t		key);

int
pm_cbf_remove(
		PMEM_CBF*		cbf, 
		uint64_t		key);

///////////// STATISTIC FUNCTIONS ///////////
void
pm_bloom_stats(PMEM_BLOOM* bf);
uint64_t pm_bloom_est_elements(PMEM_BLOOM*	bf);
uint64_t pm_bloom_count_set_bits(PMEM_BLOOM* bf);
float pm_bloom_current_false_pos_prob(PMEM_BLOOM *bf);


void
pm_cbf_stats(PMEM_CBF* cbf);
float pm_cbf_current_false_pos_prob(PMEM_CBF *cbf);
///////////////// LOCAL FUNCTIONS//////////////
void
__default_hash(
		uint64_t* hashed_vals,
		uint64_t n_hashes,
		char* str);

uint64_t __fnv_1a (char* key);
static int __sum_bits_set_char(char c);

#endif //UNIV_PMEMOBJ_BLOOM

#if defined (UNIV_PMEMOBJ_PART_PL_STAT)
#define STAT_CAL_AVG(new_avg, n, old_avg, val) do {\
	new_avg = (old_avg * (n-1) + val) / n; \
}while (0)
#endif //PMEMOBJ_PART_PL_STAT

#if defined (UNIV_PMEMOBJ_PL) || defined (UNIV_PMEMOBJ_PART_PL)
#define PMEM_LOG_HASH_KEY(hashed, key, n) do {\
	hashed = key ^ PMEM_HASH_MASK;\
	hashed = hashed % n;\
}while(0)

#endif //UNIV_PMEMOBJ_PL
#endif /*__PMEMOBJ_H__ */
