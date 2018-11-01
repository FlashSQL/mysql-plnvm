/* 
 * Author; Trong-Dat Nguyen
 * MySQL REDO log with NVDIMM
 * Copyright (c) 2017 VLDB Lab - Sungkyunkwan University
 * */

/*
 * Implement Persistent-Memory (PMEM) awareness REDO log system for InnoDB in MySQL
 * Open log files with open() are replace with pfc_append_or_set() that call pmem_map_file() from libpmem
 * pread, pwrite (sync IO and AIO) are replace with pfc_pmem_io() that call pmem_memcpy_persist() from libpmem
 * */

#ifndef __PMEM_H__
#define __PMEM_H__


#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>                                                                      
#include <sys/time.h> //for struct timeval, gettimeofday()
#include <string.h>
#include <stdint.h> //for uint64_t
#include <math.h> //for log()
#include <assert.h>
#include <wchar.h>

#include "os0file.h"
#include <libpmem.h>
#include "my_pmem_common.h"
//compile with cc ... -lpmem




struct __pmem_file;
typedef struct __pmem_file PMEM_FILE;
struct __pmem_file_coll;
typedef struct __pmem_file_coll PMEM_FILE_COLL;


struct __pmem_file {
	char* addr; //mapped address
	char* fn; //file name
	int fd; //corresponding fd
	size_t len;
	int is_pmem; //is the file on pmem
};

/*PMEM file collections includes mapped files*/
struct __pmem_file_coll{
	__pmem_file **pfile;
	int size;
	uint64_t file_size;
};



/* FUNCTIONS*/
static inline PMEM_FILE_COLL* pfc_new(uint64_t file_size);
static inline void pfc_free(PMEM_FILE_COLL* pfc);

static inline int pfc_find_by_fn(PMEM_FILE_COLL* pfc, const char* fn);
static inline int pfc_find_by_fd(PMEM_FILE_COLL* pfc, const int fd);

static inline int pfc_append(PMEM_FILE_COLL* pfc, PMEM_FILE* pf);
static inline int pfc_append_or_set(PMEM_FILE_COLL* pfc, unsigned long int create_mode, const char* fn, const int fd, const size_t len);

static inline ssize_t  pfc_pmem_io(PMEM_FILE_COLL* pfc, 
							const int type, 
							const int fd, 
							void* buf, 
							const uint64_t offset,
							unsigned long int n);




/* PMEM_FILE functions*/
static inline PMEM_FILE* pf_init(const char* fn);
static inline int pf_persist(PMEM_FILE_COLL* pfc, const int fd);
//TODO
static inline int pfc_remove(PMEM_FILE_COLL* pfc, const int fd);






static inline PMEM_FILE_COLL* pfc_new(uint64_t file_size){
	wchar_t* reason; //for check version

	PMEM_FILE_COLL* pfc = (PMEM_FILE_COLL*) malloc(sizeof(PMEM_FILE_COLL));

	if (!pfc)
		goto err;
	pfc->pfile = (PMEM_FILE**) calloc(PMEM_MAX_FILES, sizeof(PMEM_FILE*));
	if(!pfc->pfile)
		goto err;
	//allocation ok, now init	
	pfc->size = 0;
	pfc->file_size = file_size;

	//current ext4 filesystem support dax but is not recognize as pmem even though we use real pmem
	//we force libpmem behaviour by setting the PMEM_IS_PMEM_FORCE env variable
	setenv("PMEM_IS_PMEM_FORCE","1",1);
	reason = (wchar_t*)pmem_check_version(PMEM_MAJOR_VERSION, PMEM_MINOR_VERSION);
	if (reason != NULL) {
		printf("PMEM_ERROR: pmem_check_version error with reason:%ls\n",reason);
	}
	else {
		printf("PMEM_INFO: Check pmem version finish.\n");
	}	

	return pfc;	

err:
	if (pfc)
		pfc_free(pfc);
	return NULL;
}

static inline void pfc_free(PMEM_FILE_COLL* pfc){
	int i;

	assert(pfc);

	if (pfc->size > 0){
		for (i = 0; i < pfc->size; i++){
			//unmap each file from NVM
			pmem_unmap(pfc->pfile[i]->addr, pfc->pfile[i]->len);
			free(pfc->pfile[i]);
		}
	}
	free(pfc->pfile);
	free(pfc);
}
/*
 * Find a PMEM_FILE by file name
 * Input:
 *	pfc: The global PMEM_FILE_COLL
 *	key: the filename
 * Output:
 *  index: index of the found PMEM_FILE, -1 if not found
 * */
static inline int pfc_find_by_fn(PMEM_FILE_COLL* pfc, const char* fn){
	int i;

	assert(pfc);

	for (i = 0; i < pfc->size; i++){
		if(strcmp(pfc->pfile[i]->fn, fn) == 0){
			return i;
		}
	}	
	return -1;
}

/* Find a PMEM_FILE by file description
 *Input: 
	pfc: The global PMEM_FILE_COLL
	fd: file description
  Output:
	index: the index to the found PMEM_FILE or -1 if not found
 * */
static inline int pfc_find_by_fd(PMEM_FILE_COLL* pfc, const int fd){
	int i;

	assert(pfc);
	assert(fd > 0);

	for (i = 0; i < pfc->size; i++){
		if (pfc->pfile[i]->fd == fd){
			return i;
		}
	}
	return -1;
}

static inline int pfc_append(PMEM_FILE_COLL* pfc, PMEM_FILE* pf){
	assert(pfc);
	assert(pf);

	if (pfc->size >= PMEM_MAX_FILES){
		printf("ERROR: The number of pmem files have reach the maximum threshold\n");	
		return PMEM_ERROR;
	}
	pfc->pfile[pfc->size] = pf;
	pfc->size++;

	return PMEM_SUCCESS;
}
/* If the filename is already mapped: update it len
 * Else: 
 * create new PMEM_FILE and append it at the end of the list*/
static inline int pfc_append_or_set(PMEM_FILE_COLL* pfc, unsigned long int create_mode, const char* fn, const int fd, const size_t len){
	int index;
	PMEM_FILE* pf; 
	
	//find the PMEM_FILE
	if( (index = pfc_find_by_fn(pfc, fn)) >= 0) {
		//PMEM_FILE exist, update it
		pfc->pfile[index]->fd = fd;
		pfc->pfile[index]->len = len;
#ifdef UNIV_NVM_LOG_DEBUG
		printf("PMEM_LOG: pfc_append_or_set file %s exist in NVM, only update\n", fn);
#endif
		return PMEM_SUCCESS;
	}
	else {
		//create new PMEM_FILE
		pf = pf_init(fn);
		if (create_mode == OS_FILE_OPEN ||
			create_mode == OS_FILE_OPEN_RAW ||
			create_mode == OS_FILE_OPEN_RETRY) {
			//This file has already existed but has not mapped
			if( (pf->addr = (char*)  pmem_map_file(fn, 0, 0, 0666,
							&pf->len	, &pf->is_pmem)) == NULL) {
				perror("pmem_map_file");
				return PMEM_ERROR;
			}
		}
		else if(create_mode == OS_FILE_CREATE) {
			//This file has not  existed and has not mapped
			if( (pf->addr = (char*)  pmem_map_file(fn, len, PMEM_FILE_CREATE, 0666,
							&pf->len	, &pf->is_pmem)) == NULL) {
				perror("pmem_map_file");
				return PMEM_ERROR;
			}
		}
		pf->fd = fd;

#ifdef UNIV_NVM_LOG_DEBUG
		printf("PMEM_LOG: pfc_append_or_set map new file %s\n", fn);
#endif
		return	pfc_append(pfc, pf);
	}
		
}

/*Perform both AIO and normal IO
 *Input:
	type: PMEM_READ or PMEM_WRITE
	fd: file handler
	buf: buffer contains data to read to or write from
	offset: offset in file from 0
	n: number of bytes IO
   Return:
   Number of bytes has read/written
 * */
static inline ssize_t  pfc_pmem_io(PMEM_FILE_COLL* pfc, 
							const int type, 
							const int fd, 
							void* buf, 
							const uint64_t offset,
							unsigned long int n){
	int index = -1;
	ssize_t ret_bytes = 0;

	assert(pfc);
	if(pfc->size <= 0){
		printf("[PMEM_ERROR] the PMEM_FILE_COLL is empty\n");
		return 0;
	}
	

	if( (index = pfc_find_by_fd(pfc, fd)) >= 0){
#ifdef UNIV_NVM_LOG_DEBUG
			printf("[PMEM_INFO] pfc_pmem_io() type: %d fd: %d n_bytes: %lu \n", type, fd, n);
#endif
		if (type == PMEM_WRITE){
			pmem_memcpy_persist(pfc->pfile[index]->addr + offset, buf, (size_t) n);	
			ret_bytes = n;
		}
		else if (type == PMEM_READ) {
			memcpy(buf, pfc->pfile[index]->addr + offset, n);
			ret_bytes = n;
		}
		return ret_bytes;
	}
	else {
		printf("[PMEM_ERROR] there is no file with fd=%d to do IO\n", fd);
		return 0;
	}


	//find the fd in the container

}

//TODO
/*
 *Remove / unmap a file from NVM
Input:
	pfc: Global PMEM_FILE_COLL
	fd: file description
 * */
static inline int pfc_remove(PMEM_FILE_COLL* pfc, const int fd){
	return 0;	
}

/*intit a empty PMEM_FILE with the input is file name
 *other properties are fill in by pmem_map_file() function from libpmem
 * */
static inline PMEM_FILE* pf_init(const char* fn){
	PMEM_FILE* pf = (PMEM_FILE*) malloc(sizeof(PMEM_FILE));

	if (!pf)
		goto err;
	pf->fn = (char*) malloc(PMEM_MAX_FILE_NAME_LENGTH);
	if (!pf->fn)
		goto err;
	strcpy(pf->fn, fn);

	return pf;

err:
	perror("malloc");
	return NULL;
		
}
/*
 * Persist the file in NVM, this is the wrapper that call pmem_persist() from the library
 * The alternative for fsync()
 * */
static inline int pf_persist(PMEM_FILE_COLL* pfc, const int fd){
	int index;

	if ( (index = pfc_find_by_fd(pfc, fd)) >= 0){
		pmem_persist(pfc->pfile[index]->addr, pfc->pfile[index]->len);	
		return PMEM_SUCCESS;
	}
	else {
		printf("[PMEM_ERROR] call pf_persist() to an unmapped file with fd=%d\n", fd);
		return PMEM_ERROR;
	}
}

#endif //__PMEM_H__
