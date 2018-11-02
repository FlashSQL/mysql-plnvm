/* 
 * Author; Trong-Dat Nguyen
 * MySQL Partitioned log with NVDIMM
 * Using libpmemobj
 * Copyright (c) 2018 VLDB Lab - Sungkyunkwan University
 * */

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

#include "my_pmem_common.h"
#include "my_pmemobj.h"
#include "pmem0log.h"

#include "os0file.h"

//////////////////// Dirty Page Table functions//////////
/*
 * Init the Dirty Page Table as the hashtable with n buckets
 * */
PMEM_DPT* init_DPT(uint64_t n) {
	PMEM_DPT* dpt;
	int i;

	dpt = (PMEM_DPT*) malloc(sizeof(PMEM_DPT));
	dpt->n = n;
	dpt->buckets = (PMEM_DPT_ENTRY**) calloc(
			n, sizeof(PMEM_DPT_ENTRY*));

	for (i = 0; i < n; i++) {
		dpt->buckets[i] = NULL;
	}

	return dpt;
}

/*
 * Add a log record into the dirty page table.
 * The same log record has added in Transaction Table
 * */
void add_log_to_DPT(PMEM_DPT* dpt, PMEM_LOG_REC* rec){
	ulint hashed;
	PMEM_DPT_ENTRY* bucket;
	PMEM_DPT_ENTRY* prev_bucket;
	
	//(1) Get the hash
	PMEM_LOG_HASH_KEY(hashed, rec->pid.fold(), dpt->n);
	assert (hashed < dpt->n);

	bucket = dpt->buckets[hashed];
	prev_bucket = NULL;

	//(2) Multiple entries can have the same hashed value, Search for the right entry in the same hashed 
	while (bucket != NULL){
		if (bucket->id.equals_to(rec->pid)){
			//(2.1) insert the log record into this bucket
			add_log_to_DPT_entry(bucket, rec); 
			return;
		}
		prev_bucket = bucket;
		bucket = bucket->next;
	}
	// (2.2) the dirty page is not exist
	if (bucket == NULL){
		//(2.2.1) new bucket 
		PMEM_DPT_ENTRY* new_entry = (PMEM_DPT_ENTRY*) malloc(sizeof(PMEM_DPT_ENTRY));	

		new_entry->id.copy_from(rec->pid);
		new_entry->curLSN = 0;
		new_entry->next = NULL;

		//new list and add the rec as the first item
		new_entry->list = (PMEM_LOG_LIST*) malloc(sizeof(PMEM_LOG_LIST));
		new_entry->list->head = new_entry->list->tail = NULL;
		new_entry->list->n_items = 0;
		add_log_to_DPT_entry(new_entry, rec); 

		//link the new bucket
		if (prev_bucket == NULL){
			//this is the first bucket
			dpt->buckets[hashed] = new_entry;
		}	
		else{
			prev_bucket->next = new_entry;
		}

	} //end if (bucket == NULL)

}

/*
 *Add a log record to a DPT entry (as sorted doubled linked list)
 * */
void add_log_to_DPT_entry(PMEM_DPT_ENTRY* entry, PMEM_LOG_REC* rec){
	PMEM_LOG_REC*	it;
	PMEM_LOG_LIST*	list;
	uint64_t		lsn;
	
	assert(entry);
	assert(rec);

	mutex_enter(&entry->lock);
	// (1) Generate the LSN
	lsn = entry->curLSN = entry->curLSN	+ 1;
	rec->lsn = lsn;

	// (2) insert the log record to the list
	list = entry->list;
	if (list->n_items == 0){
		list->head = list->tail = rec;
		list->n_items++;

		mutex_exit(&entry->lock);

		return;
	}

	it = list->head;
	while(it != NULL){
		if (it->lsn > rec->lsn){
			//insert the log right before it
			if(it->prev == NULL){
				rec->next = it;
				rec->prev = it->prev;
				it->prev = rec;
				list->head = rec;
			}
			else {
				rec->next = it;
				rec->prev = it->prev;
				it->prev->next = rec;
				it->prev = rec;
			}
			break;
		}
		it = it->next;
	}
	if (it == NULL){
		//insert to the tail
		rec->next = NULL;
		rec->prev = list->tail;
		list->tail->next = rec;
		list->tail = rec;
	}

	list->n_items++;
	mutex_exit(&entry->lock);
}
//////////////////// End Dirty Page Table functions//////////


//////////////////// Transaction Table functions//////////
/*
 * Init the Transaction Table as the hashtable with n buckets
 * */
PMEM_TT* init_TT(uint64_t n) {
	PMEM_TT* tt;

	tt = (PMEM_TT*) malloc(sizeof(PMEM_TT));
	tt->n = n;
	tt->buckets = (PMEM_TT_ENTRY**) calloc(
			n, sizeof(PMEM_TT_ENTRY*));

	return tt;
}

/*
 * Add a log record into the dirty page table.
 * */
void add_log_to_TT(PMEM_TT* tt, PMEM_LOG_REC* rec){
	ulint hashed;
	PMEM_TT_ENTRY* bucket;
	PMEM_TT_ENTRY* prev_bucket;

	//(1) Get the hash by transaction id
	PMEM_LOG_HASH_KEY(hashed, rec->tid, tt->n);
	assert (hashed < tt->n);

	bucket = tt->buckets[hashed];
	prev_bucket = NULL;

	//(2) Multiple entries can have the same hashed value, Search for the right entry in the same hashed 
	while (bucket != NULL){
		if (bucket->tid == rec->tid){
			//(2.1) insert the log record into this bucket
			add_log_to_TT_entry(bucket, rec); 
			return;
		}
		prev_bucket = bucket;
		bucket = bucket->next;
	}
	// (2.2) the transaction is not exist
	if (bucket == NULL){
		//(2.2.1) new bucket 
		PMEM_TT_ENTRY* new_entry = (PMEM_TT_ENTRY*) malloc(sizeof(PMEM_TT_ENTRY));	

		new_entry->tid = rec->tid;
		new_entry->next = NULL;

		//new list and add the rec as the first item
		new_entry->list = (PMEM_LOG_LIST*) malloc(sizeof(PMEM_LOG_LIST));
		new_entry->list->head = new_entry->list->tail = NULL;
		new_entry->list->n_items = 0;
		add_log_to_TT_entry(new_entry, rec); 

		//link the new bucket
		if (prev_bucket == NULL){
			//this is the first bucket
			tt->buckets[hashed] = new_entry;
		}	
		else{
			prev_bucket->next = new_entry;
		}

	} //end if (bucket == NULL)

	return;
}

/*
 *Add a log record to a tail of TT entry (as doubled linked list)
 * */
void add_log_to_TT_entry(PMEM_TT_ENTRY* entry, PMEM_LOG_REC* rec){

	PMEM_LOG_LIST*	list;

	assert(entry);
	assert(rec);
	//we don't need to acquire the lock because only this transaction can touch the entry
	//mutex_enter(&entry->lock);
	//(1) Insert the log record at the tail of the list
	list = entry->list;
	//If the list is empty
	if (list->n_items == 0){
		list->head = list->tail = rec;
		list->n_items++;

		return;
	}
	
	rec->next = NULL;
	rec->prev = list->tail;
	list->tail->next = rec;
	list->tail = rec;

	list->n_items++;
	return;
}
