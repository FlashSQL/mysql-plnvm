
/* 
 * Author; Trong-Dat Nguyen
 * MySQL REDO log with NVDIMM
 * Using libpmemobj
 * Copyright (c) 2017 VLDB Lab - Sungkyunkwan University
 * */

#ifndef __PMEM_PLNVM_H__
#define __PMEM_PLNVM_H__


#include "my_pmem_common.h"

#define MAX_DPT_ENTRIES 8192
#define MAX_TT_ENTRIES 8192

enum PMEM_LOG_TYPE {
	PMEM_REDO = 1,
	PMEM_UNDO = 2
};

struct __pmem_log_rec;
typedef struct __pmem_log_rec PMEM_LOG_REC;

struct __pmem_log_list;
typedef struct __pmem_log_list PMEM_LOG_LIST;

struct __pmem_DPT_entry;
typedef struct __pmem_DPT_entry PMEM_DPT_ENTRY;

struct __pmem_DPT;
typedef struct __pmem_DPT PMEM_DPT;

struct __pmem_TT_entry;
typedef struct __pmem_TT_entry PMEM_TT_ENTRY;

struct __pmem_TT;
typedef struct __pmem_TT PMEM_TT;


/*
 * The log record wrapper, can be REDO log or UNDO log
 * */
struct __pmem_log_rec {
	uint64_t		pmemaddr;/*log content*/ 
	
	page_id_t			pid; //page id		
	uint64_t			tid; //transaction id
    uint64_t			lsn;//order of the log record in page
	PMEM_LOG_TYPE type; //log type

	PMEM_LOG_REC* prev;
	PMEM_LOG_REC* next;
};

/*
 * The double-linked list
 * */
struct __pmem_log_list {
	ib_mutex_t			lock; //the mutex lock protect all items

	PMEM_LOG_REC*		head;
	PMEM_LOG_REC*		tail;
	uint64_t			n_items;
};

/*
 * The Dirty Page Table Entry
 * Each entry present for a dirty page in DRAM. One dirty page keep the list of its dirty log records
 * Entry = key + data
 * key = page_id
 * data = double-linked list sorted by lsn
 * */
struct __pmem_DPT_entry {
	ib_mutex_t			lock; //the mutex lock protect all items
	page_id_t			id;
	uint64_t			curLSN; //used to generate the next LSN inside its list
	PMEM_LOG_LIST*		list; //sorted list by lsn

	PMEM_DPT_ENTRY*		next; //next entry in the bucket
};

/*
 * The Dirty Page Table, implement as the hash talbe of PMEM_DPT_ENTRY
 * */
struct __pmem_DPT {

	uint64_t			n;
	PMEM_DPT_ENTRY**	buckets;	
};

/*
 * The Transaction Table Entry
 * Entry = key + data
 * key = tid
 * data = FIFO double-linked list
 * */
struct __pmem_TT_entry {
	ib_mutex_t			lock; //the mutex lock protect all items
	uint64_t			tid; //transaction id

	PMEM_LOG_LIST*		list; // FIFO list

	PMEM_TT_ENTRY*		next; //next entry
};

/*
 * The Transaction Table, implement as the hash table of PMEM_TT_ENTRY
 *
 * */
struct __pmem_TT {
	uint64_t			n;
	PMEM_TT_ENTRY**		buckets;
};

PMEM_DPT* init_DPT(uint64_t n);
void add_log_to_DPT(PMEM_DPT* dpt, PMEM_LOG_REC* rec);
void add_log_to_DPT_entry(PMEM_DPT_ENTRY* entry, PMEM_LOG_REC* rec);



PMEM_TT* init_TT(uint64_t n);
void add_log_to_TT(PMEM_TT* tt, PMEM_LOG_REC* rec);
void add_log_to_TT_entry(PMEM_TT_ENTRY* entry, PMEM_LOG_REC* rec);

/////////////// INLINE, MACRO//////////////////////
#define PMEM_LOG_HASH_KEY(hashed, key, n) do {\
	hashed = key ^ PMEM_HASH_MASK;\
	hashed = hashed % n;\
}while(0)

#endif /*__PMEM_PLNVM_H__ */
