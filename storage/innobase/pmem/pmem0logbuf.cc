/* 
 * Author; Trong-Dat Nguyen
 * MySQL REDO log with NVDIMM
 * Using libpmemobj
 * Copyright (c) 2017 VLDB Lab - Sungkyunkwan University
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

int pm_wrapper_logbuf_alloc(PMEM_WRAPPER* pmw, const size_t size) {
	assert(pmw);

	pmw->plogbuf = pm_pop_logbuf_alloc(pmw->pop, size);
	if (!pmw->plogbuf)
		return PMEM_ERROR;
	else
		return PMEM_SUCCESS;
}
/*
 * Allocate new log buffer in persistent memory and assign to the pointer in the wrapper
 * */
PMEM_LOG_BUF* pm_pop_logbuf_alloc(PMEMobjpool* pop, const size_t size) {

	TOID(PMEM_LOG_BUF) logbuf; 

	POBJ_ZNEW(pop, &logbuf, PMEM_LOG_BUF);

	PMEM_LOG_BUF *plogbuf = D_RW(logbuf);
	plogbuf->size = size;
	plogbuf->type = LOG_BUF_TYPE;
	//we will update lsn, buf_free later
	plogbuf->lsn = 0;
	plogbuf->buf_free = 0;
	plogbuf->last_tsec_buf_free = 0;
	/*Read the note about need_recv flag*/
	plogbuf->need_recv = false; 
	plogbuf->data = pm_pop_alloc_bytes(pop, size);
	if (OID_IS_NULL(plogbuf->data)){
		//assert(0);
		return NULL;
	}

	pmemobj_persist(pop, plogbuf, sizeof(*plogbuf));
	return plogbuf;
} 
int pm_wrapper_logbuf_realloc(PMEM_WRAPPER* pmw, const size_t size) {
	assert(pmw);

	pmw->plogbuf = pm_pop_logbuf_realloc(pmw->pop, size);
	if (!pmw->plogbuf)
		return PMEM_ERROR;
	else
		return PMEM_SUCCESS;
}
/*
 * Re-Allocate new log buffer in persistent memory and assign to the pointer in the wrapper
 * We can use POBJ_REALLOC. This version use free + alloc
 * */
PMEM_LOG_BUF* pm_pop_logbuf_realloc(PMEMobjpool* pop, const size_t size) {

	TOID(PMEM_LOG_BUF) logbuf; 
	TOID(char) data;

	/* get the old data and free it */
	logbuf = POBJ_FIRST(pop, PMEM_LOG_BUF);
	PMEM_LOG_BUF *plogbuf = D_RW(logbuf);
	
	TOID_ASSIGN(data, plogbuf->data);
	POBJ_FREE(&data);	
	
	/* allocate a new one */	
	plogbuf->data = pm_pop_alloc_bytes(pop, size);
	if (OID_IS_NULL(plogbuf->data)){
		//assert(0);
		return NULL;
	}

	pmemobj_persist(pop, plogbuf, sizeof(*plogbuf));
	return plogbuf;
} 

PMEM_LOG_BUF* pm_pop_get_logbuf(PMEMobjpool* pop) {
	TOID(PMEM_LOG_BUF) logbuf;
	//get the first object in pmem has type PMEM_LOG_BUF
	logbuf = POBJ_FIRST(pop, PMEM_LOG_BUF);

	if (TOID_IS_NULL(logbuf)) {
		return NULL;
	}			
	else {
		PMEM_LOG_BUF *plogbuf = D_RW(logbuf);
		if(!plogbuf) {
			printf("PMEMOBJ_ERROR: message: %s\n",  pmemobj_errormsg() );
			return NULL;
		}
		return plogbuf;
	}
}
/*Perform both AIO and normal IO
 *Input:
	type: PMEM_READ or PMEM_WRITE
	buf: buffer contains data to read to or write from
	offset: offset in file from 0
	n: number of bytes IO
Condition: The plogbuf in the wrapper must be allocated first
   Return:
   Number of bytes has read/written
 * */
ssize_t  pm_wrapper_logbuf_io(PMEM_WRAPPER* pmw, 
							const int type, 
							void* buf, 
							const uint64_t offset,
							unsigned long int n){
	unsigned long int ret_bytes = 0;
		assert(pmw);
		if(!pmw->plogbuf){
			printf("PMEMOBJ_ERROR pmw->plogbuf has not allocated \n");
			return 0;
		}
		char *p = (char*) pmemobj_direct(pmw->plogbuf->data);
		if (type == PMEM_WRITE){
			pmemobj_memcpy_persist(pmw->pop, p + offset, buf, (size_t) n);	
			ret_bytes = n;
		}
		else if (type == PMEM_READ) {
			memcpy(buf, p + offset, n);
			ret_bytes = n;
		}
		return ret_bytes;
}

void* pm_wrapper_logbuf_get_logdata(PMEM_WRAPPER* pmw){
	assert(pmw);
	assert(pmw->plogbuf);
	return pmemobj_direct(pmw->plogbuf->data);
}
