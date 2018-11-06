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

#include "os0file.h"

//////////////////// Dirty Page Table functions//////////
/*
 * Init the Dirty Page Table as the hashtable with n buckets
 * */
MEM_DPT* init_DPT(uint64_t n) {
	MEM_DPT* dpt;
	int i;

	dpt = (MEM_DPT*) malloc(sizeof(MEM_DPT));
	dpt->n = n;
	dpt->buckets = (MEM_DPT_ENTRY**) calloc(
			n, sizeof(MEM_DPT_ENTRY*));

	for (i = 0; i < n; i++) {
		dpt->buckets[i] = NULL;
	}

	return dpt;
}

/*
 * Add a log record into the dirty page table.
 * The same log record has added in Transaction Table
 * dpt (in): dirty page table pointer
 * rec (in/out): log record. If is_local_dpt is set, output is rec->lsn is set and corresponding pointers in rec are linked
 * is_local_dpt (in)
 * true: insert log record into the local dpt of transaction
 * false: insert log recrod into the global dpt
 * */
void add_log_to_DPT(
		MEM_DPT* dpt,
	   	MEM_LOG_REC* rec,
		bool is_local_dpt){
	ulint hashed;
	MEM_DPT_ENTRY* bucket;
	MEM_DPT_ENTRY* prev_bucket;
	
	//(1) Get the hash
	PMEM_LOG_HASH_KEY(hashed, rec->pid.fold(), dpt->n);
	assert (hashed < dpt->n);

	bucket = dpt->buckets[hashed];
	prev_bucket = NULL;

	//(2) Multiple entries can have the same hashed value, Search for the right entry in the same hashed 
	while (bucket != NULL){
		if (bucket->id.equals_to(rec->pid)){
			//(2.1) insert the log record into this bucket
			if (is_local_dpt)
				add_log_to_global_DPT_entry(bucket, rec); 
			else 
				add_log_to_local_DPT_entry(bucket, rec);
			return;
		}
		prev_bucket = bucket;
		bucket = bucket->next;
	}
	// (2.2) the dirty page is not exist
	if (bucket == NULL){
		//(2.2.1) new bucket 
		MEM_DPT_ENTRY* new_entry = (MEM_DPT_ENTRY*) malloc(sizeof(MEM_DPT_ENTRY));	

		new_entry->id.copy_from(rec->pid);
		new_entry->curLSN = 0;
		new_entry->next = NULL;

		//new list and add the rec as the first item
		new_entry->list = (MEM_LOG_LIST*) malloc(sizeof(MEM_LOG_LIST));
		new_entry->list->head = new_entry->list->tail = NULL;
		new_entry->list->n_items = 0;

		if (is_local_dpt)
			add_log_to_global_DPT_entry(new_entry, rec); 
		else 
			add_log_to_local_DPT_entry(new_entry, rec);


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
 Generate next lsn and assign rec->lsn = lsn + 1
Output: dpt_next, dpt_prev pointers changed
Caller: add_log_to_DPT() with is_local_dpt = false
 * */
void
add_log_to_global_DPT_entry(
		MEM_DPT_ENTRY* entry,
	   	MEM_LOG_REC* rec)
{
	MEM_LOG_REC*	it;
	MEM_LOG_LIST*	list;
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
			if(it->dpt_prev == NULL){
				rec->dpt_next = it;
				rec->dpt_prev = it->dpt_prev;
				it->dpt_prev = rec;
				list->head = rec;
			}
			else {
				rec->dpt_next = it;
				rec->dpt_prev = it->dpt_prev;
				it->dpt_prev->dpt_next = rec;
				it->dpt_prev = rec;
			}
			break;
		}
		it = it->dpt_next;
	}
	if (it == NULL){
		//insert to the tail
		rec->dpt_next = NULL;
		rec->dpt_prev = list->tail;
		list->tail->dpt_next = rec;
		list->tail = rec;
	}

	list->n_items++;
	mutex_exit(&entry->lock);
}
/*
 *Add a log record to a local DPT entry of a transaction (as sorted doubled linked list)
 Does not enerate next lsn
Output: trx_page_next and trx_page_prev pointers changed
Caller: add_log_to_DPT() with is_local_dpt = true
 * */
void
add_log_to_local_DPT_entry(
		MEM_DPT_ENTRY* entry,
	   	MEM_LOG_REC* rec)
{
	MEM_LOG_REC*	it;
	MEM_LOG_LIST*	list;
	uint64_t		lsn;
	
	assert(entry);
	assert(rec);

	mutex_enter(&entry->lock);

	// (1) insert the log record to the list
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
			if(it->trx_page_prev == NULL){
				rec->trx_page_next = it;
				rec->trx_page_prev = it->trx_page_prev;
				it->trx_page_prev = rec;
				list->head = rec;
			}
			else {
				rec->trx_page_next = it;
				rec->trx_page_prev = it->trx_page_prev;
				it->trx_page_prev->trx_page_next = rec;
				it->trx_page_prev = rec;
			}
			break;
		}
		it = it->trx_page_next;
	}
	if (it == NULL){
		//insert to the tail
		rec->trx_page_next = NULL;
		rec->trx_page_prev = list->tail;
		list->tail->trx_page_next = rec;
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
MEM_TT* init_TT(uint64_t n) {
	ulint i;
	MEM_TT* tt;

	tt = (MEM_TT*) malloc(sizeof(MEM_TT));
	tt->n = n;
	tt->buckets = (MEM_TT_ENTRY**) calloc(
			n, sizeof(MEM_TT_ENTRY*));
	for (i = 0; i < n; i++)
		tt->buckets[i] = NULL;

	return tt;
}

MEM_TT_ENTRY*
init_TT_entry(uint64_t tid){

	MEM_TT_ENTRY* new_entry = (MEM_TT_ENTRY*) malloc(sizeof(MEM_TT_ENTRY));	

	new_entry->tid = tid;
	new_entry->next = NULL;

	//new list
	new_entry->list = (MEM_LOG_LIST*) malloc(sizeof(MEM_LOG_LIST));
	new_entry->list->head = new_entry->list->tail = NULL;
	new_entry->list->n_items = 0;

	//local dpt
	new_entry->local_dpt = init_DPT(MAX_DPT_ENTRIES);

}

/*
 * Remove a transation entry when the correspondign transaction commit
 * tt (in): The global transaction table
 * entry (in): The tt entry to be removed
 * prev_entry (in): The tt entry right before the entry
 * hashed (in): Hashed value of the hashed line consists the entry
 *
 * Caller: trx_commit_TT()
 * */
void 
remove_TT_entry(
		MEM_TT* tt,
		MEM_DPT* global_dpt,
	   	MEM_TT_ENTRY* entry,
	   	MEM_TT_ENTRY* prev_entry,
		ulint hashed)
{
	MEM_DPT*			local_dpt;
	MEM_DPT_ENTRY*		dpt_entry;
	MEM_DPT_ENTRY*		prev_dpt_entry;
	ulint				i;

	local_dpt = entry->local_dpt;

	//(1) Remove entry out of the hashed line
	if (prev_entry != NULL){
		prev_entry->next = entry->next;
		entry->next = NULL;
	}
	else {
		//entry is the first item in the hashed line
		tt->buckets[hashed] = entry->next;
		entry->next = NULL;
	}

	//(2) Remove the local DPT of the entry
	//(2.1) for each hashed line in the local DPT 
	for (i = 0; i < local_dpt->n; i++){
		//Get a hashed line
		dpt_entry = local_dpt->buckets[i];
		if (dpt_entry == NULL) 
			continue;
		assert(dpt_entry);
		//for each entry in the hashed line
		while (dpt_entry != NULL){
			/////////////////////
			//remove log records and their pointers in this dpt_entry
			remove_logs_when_commit(global_dpt, dpt_entry);
			////////////////////////////////////
			
			//remove the dpt entry from the dpt hashed line
			prev_dpt_entry = dpt_entry;
			dpt_entry = dpt_entry->next;
			//free resource
			prev_dpt_entry->next = NULL;
			free(prev_dpt_entry);
			prev_dpt_entry = NULL;
		}

		//Until this point, all dpt_entry in the hashed line is removed. Now, set the hashed line to NULL
		local_dpt->buckets[i] = NULL;
		//next hashed line
	}//end for each hashed line

	//(2.2) Until this point, all hashed lines in the local DPT are removed.
	free(local_dpt->buckets);	
	local_dpt->buckets = NULL;
	local_dpt->n = 0;

	//(3) Free the tt entry
	free(local_dpt);
	entry->local_dpt = local_dpt = NULL;

	//(4) Free the tt log list
	entry->list->head = entry->list->tail = NULL;
	entry->list->n_items = 0;	
	free(entry->list);
	entry->list = NULL;

	//(5) Free the entry
	free(entry);
	entry = NULL;
}
/*
 * Add a log record into the transaction table
 * Call this function when a transaction generate the log record in DRAM
 * (1) Add log record rec to the global DPT, result is rec->lsn is assigned
 * (2) Add log record rec to transaction entry, based on rec->tid, the corresponding transaction entry e is added
 * (3) Add log record to the local DPT of e 
 * */
void 
add_log_to_TT	(MEM_TT* tt,
				MEM_DPT* dpt,
			   	MEM_LOG_REC* rec){

	ulint hashed;
	MEM_TT_ENTRY* bucket;
	MEM_TT_ENTRY* prev_bucket;

	//(1) Add log record to the global DPT
	add_log_to_DPT(dpt, rec, false);

	//(2) Add log record to transaction entry
	//Get the hash by transaction id
	PMEM_LOG_HASH_KEY(hashed, rec->tid, tt->n);
	assert (hashed < tt->n);

	bucket = tt->buckets[hashed];
	prev_bucket = NULL;
	

	//Multiple entries can have the same hashed value, Search for the right entry in the same hashed 
	while (bucket != NULL){
		if (bucket->tid == rec->tid){
			//(2.1) insert the log record into this bucket
			add_log_to_TT_entry(bucket, rec); 

			// (3) Add log record to the local DPT of trx entry
			add_log_to_DPT(bucket->local_dpt, rec, true);
			return;
		}
		prev_bucket = bucket;
		bucket = bucket->next;
	}
	// (2.2) the transaction entry (bucket) is not exist
	if (bucket == NULL){
		// new bucket 
		MEM_TT_ENTRY* new_entry = init_TT_entry(rec->tid);	
		add_log_to_TT_entry(new_entry, rec); 
		// (3) Add log record to the local DPT of trx entry
		add_log_to_DPT(new_entry->local_dpt, rec, true);

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
 *Add a log record to a transacton entry in the transaction table
Caller: add_log_to_TT
 * */
void
add_log_to_TT_entry(
	   	MEM_TT_ENTRY* entry,
	   	MEM_LOG_REC* rec){

	MEM_LOG_LIST*	list;

	assert(entry);
	assert(rec);
	//we don't need to acquire the lock because only this transaction can touch the entry
	//mutex_enter(&entry->lock);
	//(1) Insert the log record at the tail of the list
	list = entry->list;
	//If the list is empty
	if (list->n_items == 0){
		list->head = list->tail = rec;
		rec->tt_next = rec->tt_prev = NULL;
		list->n_items++;
		return;
	}
	
	rec->tt_next = NULL;
	rec->tt_prev = list->tail;
	list->tail->tt_next = rec;
	list->tail = rec;

	list->n_items++;
	return;
}
/*
 * Handle changes in the transaction table when a transaction tid commit 
 * pop (in): The global pop
 * buf (in): The global buf
 * global_dpt (in): The global dpt
 * tt (in): The global tt
 * tid (in): Transaction id
 * */
int
trx_commit_TT(
		PMEMobjpool*	pop,
		PMEM_BUF*		buf,
		MEM_DPT*		global_dpt,
		MEM_TT*			tt,
	   	uint64_t		tid)
{
	ulint			hashed;
	ulint			i;
	MEM_TT_ENTRY*	bucket;
	
	MEM_DPT*		local_dpt;	
	MEM_DPT_ENTRY*	dpt_entry;

	//For remove
	MEM_TT_ENTRY*	prev_bucket; // for remove
	MEM_DPT_ENTRY*	prev_dpt_entry;
	MEM_LOG_LIST*	log_list;
	MEM_LOG_REC*	memrec;
	MEM_LOG_REC*	memrec_next;
	
	//(1) Get transaction entry by the input tid 
	PMEM_LOG_HASH_KEY(hashed, tid, tt->n);
	assert (hashed < tt->n);

	bucket = tt->buckets[hashed];
	prev_bucket = NULL;
	
	while (bucket != NULL){
		if (bucket->tid == tid){
			break;
		}
		prev_bucket = bucket;
		bucket = bucket->next;
	}

	if (bucket == NULL){
		printf("PMEM_LOG Error, tid %zu not found in transaction table\n", tid);
		return PMEM_ERROR;
	}

	//(2) For each DPT entry in the local dpt, write REDO log to corresponding page	
	local_dpt = bucket->local_dpt;
	assert(local_dpt);
	for (i = 0; i < local_dpt->n; i++){
		//Get a hashed line
		dpt_entry = local_dpt->buckets[i];
		if (dpt_entry == NULL) 
			continue;
		assert(dpt_entry);
		//for each entry in the same hashed line
		while (dpt_entry != NULL){
			pm_write_REDO_logs(pop, buf, dpt_entry);

			dpt_entry = dpt_entry->next;
		}
		//next hashed line
	}//end for each hashed line

	//(3) Remove tt entry and its corresponding resources
	remove_TT_entry (tt, global_dpt, bucket, prev_bucket, hashed);

	
}

/*
 * write per-page REDO log records when a transaction commits
 * pop: global pop
 * buf: global PMEM_BUF wrapper
 * dpt_entry (in): list of per-page REDO log record sorted by lsn 
 *
 * caller: trx_commit_TT() 
 * */
int
pm_write_REDO_logs(
		PMEMobjpool*	pop,
		PMEM_BUF*		buf,
		MEM_DPT_ENTRY*	dpt_entry
	   	) 
{
	ulint hashed;
	ulint i;
		
	TOID(PMEM_BUF_BLOCK_LIST) hash_list;
	PMEM_BUF_BLOCK_LIST* phashlist;
	PMEM_BUF_BLOCK* pblock;

	TOID(PMEM_BUF_BLOCK) free_block;
	PMEM_BUF_BLOCK* pfree_block;
	
	page_id_t page_id(dpt_entry->id);

	//(1) Get the bucket that has the page
#if defined (UNIV_PMEMOBJ_BUF_PARTITION)
	PMEM_LESS_BUCKET_HASH_KEY(hashed,page_id.space(), page_id.page_no());
#else //EVEN_BUCKET
	PMEM_HASH_KEY(hashed, page_id.fold(), PMEM_N_BUCKETS);
#endif

	TOID_ASSIGN(hash_list, (D_RW(buf->buckets)[hashed]).oid);

	phashlist = D_RW(hash_list);
	assert(phashlist);
	//(2) Get the page in the bucket
	
	pblock = NULL;

	for (i = 0; i < D_RO(hash_list)->cur_pages; i++) {
		pfree_block = D_RW(D_RW(phashlist->arr)[i]);
		if(pfree_block->state == PMEM_IN_USED_BLOCK &&
				pfree_block->id.equals_to(page_id)) {

			//Case A: Directy apply REDO log to the page and remove duplicate UNDO log records
			pmemobj_rwlock_wrlock(pop, &pfree_block->lock);
			pm_write_REDO_logs_to_pmblock(pop, pfree_block, dpt_entry); 	
			pmemobj_rwlock_unlock(pop, &pfree_block->lock);
			return PMEM_SUCCESS;
		}
		else if(pfree_block->state == PMEM_PLACE_HOLDER_BLOCK &&
				pfree_block->id.equals_to(page_id)) {

			//Case B: add this log record to exist REDO log			   
			pmemobj_rwlock_wrlock(pop, &pfree_block->lock);

			pm_merge_REDO_logs_to_placeholder(pop, pfree_block, dpt_entry);
			pmemobj_rwlock_unlock(pop, &pfree_block->lock);
			return PMEM_SUCCESS;
		}
		else if (pfree_block->state == PMEM_FREE_BLOCK) {
			//Case C: create the place-holder	and add the log
			pmemobj_rwlock_wrlock(pop, &pfree_block->lock);

			pfree_block->state = PMEM_PLACE_HOLDER_BLOCK;

			pm_merge_REDO_logs_to_placeholder(pop, pfree_block, dpt_entry);
			pmemobj_rwlock_unlock(pop, &pfree_block->lock);
			return PMEM_SUCCESS;
		}

	} //end for
	
	if (pblock == NULL){
		//(2.1) The dirty page is not in PB-NVM, create the place-holder
	}



	return PMEM_SUCCESS;
}

/*
 * Add REDO log records into a pmem block 
 * Important: The input log records are sorted by lsn
 * We start from the head and remember the inserted position for the next insert
 * */
void 
pm_merge_REDO_logs_to_placeholder(
		PMEMobjpool*	pop,
		PMEM_BUF_BLOCK*	pblock,
	   	MEM_DPT_ENTRY*	dpt_entry)
{
		
		ulint i;	
		MEM_LOG_REC* memrec;
		TOID(PMEM_LOG_REC) cur_insert;

		assert(pblock);

		PMEM_LOG_LIST* plog_list = D_RW(pblock->log_list);
		
		TOID_ASSIGN(cur_insert, (plog_list->head).oid);
		cur_insert = plog_list->head;

		memrec = dpt_entry->list->head;
		
		while(memrec != NULL){
			TOID(PMEM_LOG_REC) pmemrec;
			//(1) Allocate the pmem log record from in-mem log record
			pmemrec = alloc_pmemrec(pop,
									memrec,
									PMEM_REDO_LOG);

			// (2) Add the pmem log record to the list, start from the cur_insert
			if (plog_list->n_items == 0){
				//The first item
				TOID_ASSIGN(plog_list->head, pmemrec.oid);
				TOID_ASSIGN(plog_list->tail, pmemrec.oid);
				plog_list->n_items++;
				TOID_ASSIGN(cur_insert, (plog_list->head).oid);
			}
			else{
				//Insert from the cur_insert
				while (! TOID_IS_NULL(cur_insert)){
					if (D_RW(cur_insert)->lsn > D_RW(pmemrec)->lsn){
						if (TOID_IS_NULL(D_RW(cur_insert)->prev)){
							//insert in head
							TOID_ASSIGN( D_RW(pmemrec)->next, cur_insert.oid);
							TOID_ASSIGN( D_RW(pmemrec)->prev, ( D_RW(cur_insert)->prev).oid);
							TOID_ASSIGN( D_RW(cur_insert)->prev, pmemrec.oid);
							TOID_ASSIGN( plog_list->head, pmemrec.oid);
							
							//pmemrec->next = cur_insert;
							//pmemrec->prev = cur_insert->prev;
							//cur_insert->prev = pmemrec;
							//plog_list->head = pmemrec;
						}
						else {
							//insert between cur_insert and its prev
							TOID_ASSIGN( D_RW(pmemrec)->next, cur_insert.oid);
							TOID_ASSIGN( D_RW(pmemrec)->prev, (D_RW(cur_insert)->prev).oid);
							TOID_ASSIGN( D_RW(D_RW(cur_insert)->prev)->next, pmemrec.oid);
							TOID_ASSIGN( D_RW(cur_insert)->prev, pmemrec.oid);
							//pmemrec->next = cur_insert;
							//pmemrec->prev = cur_insert->prev;
							//cur_insert->prev->next = pmemrec;
							//cur_insert->prev = pmemrec;
						}
						break;
					}
					//next log record in the list
					TOID_ASSIGN(cur_insert, (D_RW(cur_insert)->next).oid);
					//cur_insert = cur_insert->next;
				} //end while
				if (TOID_IS_NULL(cur_insert)){
					//insert in tail
					TOID_ASSIGN( D_RW(pmemrec)->next, OID_NULL);
					TOID_ASSIGN( D_RW(pmemrec)->prev, (plog_list->tail).oid);
					TOID_ASSIGN( D_RW(plog_list->tail)->next, pmemrec.oid);
					TOID_ASSIGN( plog_list->tail, pmemrec.oid);

					TOID_ASSIGN( cur_insert, (plog_list->tail).oid);

					//pmemrec->next = NULL;
					//pmemrec->prev = plog_list->tail;
					//plog_list->tail->next = pmemrec;
					//plog_list->tail = pmemrec;

					//cur_insert = plog_list->tail;
				}
			}
			//next rec in the local dpt entry
			memrec = memrec->trx_page_next;
		}//end while(memrec != NULL) 


}
/*
 * Write REDO logs from a dpt entry into a pmem block when a transaction commit
 * pop (in): global pop
 * pblock (in): The pmem block
 * dpt_entry (in): The dpt entry that consists of REDO logs
 *
 * */
void
pm_write_REDO_logs_to_pmblock(
		PMEMobjpool*	pop,
		PMEM_BUF_BLOCK*	pblock,
	   	MEM_DPT_ENTRY*	dtp_entry
	   	)
{
	MEM_LOG_REC* memrec;
	PMEM_LOG_REC* pmemrec;
	PMEM_LOG_LIST* pundo_log_list;
	//when the pblock is a pmem page, the log_list is UNDO log
	pundo_log_list = D_RW(pblock->log_list);

	memrec = dtp_entry->list->head;

	while (memrec != NULL){
		if (memrec->is_in_pmem){
			//remove the corresponding log record in UNDO list
			pm_remove_UNDO_log_from_list(pop, pundo_log_list, memrec);
		}
		else{
			//TODO: directly apply REDO log to the pmem page
		}
		//next rec in the local dpt entry
		memrec = memrec->trx_page_next;
	}

}
/*
 * Remove an UNDO log record from the list
 * Used when a transaction commit
 * */
void
pm_remove_UNDO_log_from_list(
		PMEMobjpool*	pop,
		PMEM_LOG_LIST* list,
		MEM_LOG_REC* memrec)
{
	TOID(PMEM_LOG_REC) pmemrec;
	PMEM_LOG_REC* ppmemrec;

	TOID_ASSIGN(pmemrec, list->head.oid) ;
	ppmemrec = D_RW(pmemrec);

	assert(ppmemrec);
	assert(ppmemrec->type == PMEM_UNDO_LOG);

	while (ppmemrec != NULL){
		if (ppmemrec->lsn == memrec->lsn){
			//found, remove pmemrec from the list
			if ( !TOID_IS_NULL(ppmemrec->prev)) {
				TOID_ASSIGN( D_RW(ppmemrec->prev)->next,
						(ppmemrec->next).oid);
				TOID_ASSIGN( D_RW(ppmemrec->next)->prev,
						(ppmemrec->prev).oid);
			}
			else {
				//pmem rec is the first UNDO Log record
				TOID_ASSIGN(list->head, (ppmemrec->next).oid);
				TOID_ASSIGN( D_RW(ppmemrec->next)->prev, list->head.oid);
			}

				TOID_ASSIGN(ppmemrec->next, OID_NULL);
				TOID_ASSIGN(ppmemrec->prev, OID_NULL);

				free_pmemrec(pop, pmemrec);
			break;
		}
		//next pmem rec
		TOID_ASSIGN( pmemrec, (D_RW(pmemrec)->next).oid);
		ppmemrec = D_RW(pmemrec);
	} //end while
}

/*
 * Allocate the pmem log record from in-mem log record
 * */
TOID(PMEM_LOG_REC) alloc_pmemrec(
		PMEMobjpool*	pop,
		MEM_LOG_REC*	mem_rec,
		PMEM_LOG_TYPE	type
		)
{

	TOID(char) byte_array;
	byte* plog_data;
	TOID(PMEM_LOG_REC) pmem_rec;
	PMEM_LOG_REC* ppmem_rec;

	//Allocate the pmem log record
	TX_BEGIN(pop) {
		POBJ_ZNEW(pop, &pmem_rec, PMEM_LOG_REC);
		ppmem_rec = D_RW(pmem_rec);

		//Copy data
		ppmem_rec->type = type;
		ppmem_rec->pid.copy_from(mem_rec->pid);
		ppmem_rec->lsn = mem_rec->lsn;
		ppmem_rec->size = mem_rec->size;

		POBJ_ALLOC(pop,
			   	&byte_array,
			   	char,
			   	sizeof(*D_RW(byte_array)) * ppmem_rec->size,
				NULL,
				NULL);
		ppmem_rec->log_data = byte_array.oid;	

		plog_data = static_cast<byte*> (pmemobj_direct(ppmem_rec->log_data));

		pmemobj_memcpy_persist(pop, plog_data, mem_rec->mem_addr, mem_rec->size);
		//Add to the list
	}TX_ONABORT {
	}TX_END

	return pmem_rec;
}

/*
 * Free the pmem log record 
 * */
void 
free_pmemrec(
		PMEMobjpool*	pop,
		TOID(PMEM_LOG_REC)	pmem_rec
		)
{

	TX_BEGIN(pop) {
		//free the log content
		pmemobj_free (&(D_RW(pmem_rec)->log_data));
		D_RW(pmem_rec)->size = 0;
		
		//free the whole log record	
		POBJ_FREE(&pmem_rec);	

	}TX_ONABORT {
	}TX_END
}

/*
 * Remove log records and all of their pointers related to a local dpt entry of a transaction when it commit
 * global_dpt (in): The global dpt, used for remove the corresponding pointers
 * entry (in): The local dpt entry of the transaction
 * */
void 
remove_logs_when_commit(
		MEM_DPT*	global_dpt,
		MEM_DPT_ENTRY*		entry)
{
	ulint hashed;

	MEM_LOG_REC*	memrec;
	MEM_LOG_REC*	next_memrec;//next log rec of memrec in the entry
	MEM_DPT_ENTRY* global_DPT_entry;
	MEM_DPT_ENTRY* bucket;

	//(1) Get DPT entry in the global DPT
	PMEM_LOG_HASH_KEY(hashed, entry->id.fold(), global_dpt->n);
	global_DPT_entry = global_dpt->buckets[hashed];
	assert(global_DPT_entry);

	while (global_DPT_entry != NULL){
		if (global_DPT_entry->id.equals_to(entry->id)){
			break;
		}
		global_DPT_entry = global_DPT_entry->next;
	}

	if (global_DPT_entry == NULL){
		printf("PMEM ERROR! Cannot find corresponding DPT entry in remove_local_DPT_entry()\n");
		assert(0);
	}
	//(2) For each log record in the entry, remove pointers
	memrec = entry->list->head;

	while (memrec != NULL){
		next_memrec = memrec->trx_page_next;

		//(2.1) Remove from global DPT entry
		if (memrec->dpt_prev == NULL){
			//memrec is the first log record in the global DPT entry
			global_DPT_entry->list->head = memrec->dpt_next;
			memrec->dpt_next->dpt_prev = NULL;

		}
		else{
			memrec->dpt_prev->dpt_next = memrec->dpt_next;
			memrec->dpt_next->dpt_prev = memrec->dpt_prev;
		}
		memrec->dpt_next = NULL;
		memrec->dpt_prev = NULL;

		//(2.2) Remove pointers from TT and local DPT entry
		memrec->tt_next = NULL;
		memrec->tt_prev = NULL;
		memrec->trx_page_next = NULL;
		memrec->trx_page_prev = NULL;

		//(2.3) Free log record
		free (memrec->mem_addr);
		memrec->mem_addr = NULL;
		free (memrec);

		memrec = next_memrec;
	}//end while
}
