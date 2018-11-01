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

//#include "pmem_log.h"
#include <libpmemobj.h>
#include "my_pmem_common.h"
//#include "pmem0buf.h"
//cc -std=gnu99 ... -lpmemobj -lpmem
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
#endif

#endif //UNIV_PMEMOBJ_BUF

POBJ_LAYOUT_BEGIN(my_pmemobj);
POBJ_LAYOUT_TOID(my_pmemobj, char);
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
#endif
#endif //UNIV_PMEMOBJ_LSB
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

#if defined (UNIV_PMEMOBJ_LSB)
	PMEM_LSB* plsb;
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


////////////////////// LOG BUFFER /////////////////////////////

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
		   	PMEM_BUF*		buf,
		   	page_id_t		page_id,
		   	page_size_t		size,
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
		   	PMEM_BUF*				buf);


void
pm_buf_handle_full_hashed_list(
	PMEMobjpool*	pop,
	PMEM_BUF*		buf,
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
/*LESS_BUCKET partition
 *One space is mapped with as less buckets as possible
@hashed		[out]: return hashed value
@space		[in]: space_no
@page		[in]: page_no
@n			[in]: number of buckets 
@B			[in]: number of bits present number of buckets
@S			[in]: number of bits present space_no
@P			[in]: number of bits present max number of pages per space on a bucket, this value is log2(page_per_bucket)
 * */
//Use this funciton for DEBUG only
//#define PMEM_LESS_BUCKET_HASH_KEY(hashed, space, page)\
//   hash_f1(hashed, space, page,\
//		   	PMEM_N_BUCKETS,\
//		   	PMEM_N_BUCKET_BITS,\
//		   	PMEM_N_SPACE_BITS,\
//		   	PMEM_PAGE_PER_BUCKET_BITS) 

//Use this macro for production build 
#define PMEM_LESS_BUCKET_HASH_KEY(hashed, space, page)\
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

#endif /*__PMEMOBJ_H__ */
