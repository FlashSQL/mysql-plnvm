
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
//#include "pmem0buf.h"

#include "os0file.h"

int pm_wrapper_dbw_alloc(PMEM_WRAPPER* pmw, const size_t size) {
	assert(pmw);

	pmw->pdbw = pm_pop_dbw_alloc(pmw->pop, size);
	if (!pmw->pdbw)
		return PMEM_ERROR;
	else
		return PMEM_SUCCESS;
}

PMEM_DBW* pm_pop_get_dbw(PMEMobjpool* pop) {
	TOID(PMEM_DBW) dbw;
	//get the first object in pmem has type PMEM_DBW
	dbw = POBJ_FIRST(pop, PMEM_DBW);

	if (TOID_IS_NULL(dbw)) {
		return NULL;
	}			
	else {
		PMEM_DBW *pdbw = D_RW(dbw);
		if(!pdbw) {
			printf("PMEMOBJ_ERROR: message: %s\n",  pmemobj_errormsg() );
			return NULL;
		}
		return pdbw;
	}
}
/*Perform both AIO and normal IO
 *Input:
	type: PMEM_READ or PMEM_WRITE
	buf: buffer contains data to read to or write from
	offset: offset in file from 0
	n: number of bytes IO
Condition: The double write buffer in the wrapper must be allocated first
   Return:
   Number of bytes has read/written
 * */
ssize_t  pm_wrapper_dbw_io(PMEM_WRAPPER* pmw, 
							const int type, 
							void* buf, 
							const uint64_t offset,
							unsigned long int n){
	unsigned long int ret_bytes;
		assert(pmw);
		if(!pmw->pdbw){
			printf("PMEMOBJ_ERROR pmw->pdbw has not allocated \n");
			return 0;
		}
		char *p = (char*) pmemobj_direct(pmw->pdbw->data);
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
void* pm_wrapper_dbw_get_dbwdata(PMEM_WRAPPER* pmw){
	assert(pmw);
	assert(pmw->pdbw);
	return pmemobj_direct(pmw->pdbw->data);
}
/*
 * Allocate new log buffer in persistent memory and assign to the pointer in the wrapper
 * */
PMEM_DBW* pm_pop_dbw_alloc(PMEMobjpool* pop, const size_t size) {

	TOID(PMEM_DBW) dbw; 

	POBJ_ZNEW(pop, &dbw, PMEM_DBW);

	PMEM_DBW *pdbw = D_RW(dbw);

	pdbw->size = size;
	pdbw->type = DBW_TYPE;
	pdbw->is_new = true;

	pdbw->data = pm_pop_alloc_bytes(pop, size);
	if (OID_IS_NULL(pdbw->data)){
		//assert(0);
		return NULL;
	}
	
	pmemobj_persist(pop, pdbw, sizeof(*pdbw));
	return pdbw;
} 
